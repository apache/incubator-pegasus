// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "redis_parser.h"

#include <rocksdb/status.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/utility/string_conv.h>

#include <rrdb/rrdb.client.h>
#include <pegasus/error.h>
#include <pegasus_key_schema.h>
#include <pegasus_utils.h>
#include "base/pegasus_const.h"

#define CR '\015'
#define LF '\012'

namespace pegasus {
namespace proxy {

std::atomic_llong redis_parser::s_next_seqid(0);

std::unordered_map<std::string, redis_parser::redis_call_handler> redis_parser::s_dispatcher = {
    {"SET", redis_parser::g_set},
    {"GET", redis_parser::g_get},
    {"DEL", redis_parser::g_del},
    {"SETEX", redis_parser::g_setex},
    {"TTL", redis_parser::g_ttl},
    {"PTTL", redis_parser::g_ttl},
    {"GEODIST", redis_parser::g_geo_dist},
    {"GEORADIUS", redis_parser::g_geo_radius},
    {"GEORADIUSBYMEMBER", redis_parser::g_geo_radius_by_member},
    {"INCR", redis_parser::g_incr},
    {"INCRBY", redis_parser::g_incr_by},
    {"DECR", redis_parser::g_decr},
    {"DECRBY", redis_parser::g_decr_by},
};

redis_parser::redis_call_handler redis_parser::get_handler(const char *command, unsigned int length)
{
    std::string key(command, length);
    std::transform(key.begin(), key.end(), key.begin(), toupper);
    auto iter = s_dispatcher.find(key);
    if (iter == s_dispatcher.end())
        return redis_parser::g_default_handler;
    return iter->second;
}

redis_parser::redis_parser(proxy_stub *op, dsn::message_ex *first_msg)
    : proxy_session(op, first_msg),
      current_msg(new message_entry()),
      status(start_array),
      current_size(),
      total_length(0),
      current_buffer(nullptr),
      current_buffer_length(0),
      current_cursor(0)
{
    ::dsn::apps::rrdb_client *r;
    if (op) {
        std::vector<dsn::rpc_address> meta_list;
        dsn::replication::replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), op->get_cluster());
        r = new ::dsn::apps::rrdb_client(op->get_cluster(), meta_list, op->get_app());
        if (strlen(op->get_geo_app()) != 0) {
            _geo_client = dsn::make_unique<geo::geo_client>("config.ini",
                                                            op->get_cluster(),
                                                            op->get_app(),
                                                            op->get_geo_app(),
                                                            new geo::latlng_extractor_for_lbs());
        }
    } else {
        r = new ::dsn::apps::rrdb_client();
    }
    client.reset(r);
}

redis_parser::~redis_parser()
{
    clear_reply_queue();
    reset_parser();
    ddebug("%s: redis parser destroyed", remote_address.to_string());
}

void redis_parser::prepare_current_buffer()
{
    void *msg_buffer;
    if (current_buffer == nullptr) {
        dsn::message_ex *first_msg = recv_buffers.front();
        dassert(
            first_msg->read_next(&msg_buffer, &current_buffer_length),
            "read dsn::message_ex* failed, msg from_address = %s, to_address = %s, rpc_name = %s",
            first_msg->header->from_address.to_string(),
            first_msg->to_address.to_string(),
            first_msg->header->rpc_name);
        current_buffer = reinterpret_cast<char *>(msg_buffer);
        current_cursor = 0;
    } else if (current_cursor >= current_buffer_length) {
        dsn::message_ex *first_msg = recv_buffers.front();
        first_msg->read_commit(current_buffer_length);
        if (first_msg->read_next(&msg_buffer, &current_buffer_length)) {
            current_cursor = 0;
            current_buffer = reinterpret_cast<char *>(msg_buffer);
            return;
        } else {
            // we have consume this message all over
            // reference is added in append message
            first_msg->release_ref();
            recv_buffers.pop();
            current_buffer = nullptr;
            prepare_current_buffer();
        }
    } else
        return;
}

void redis_parser::reset_parser()
{
    // clear the parser status
    current_msg->request.length = 0;
    current_msg->request.buffers.clear();
    status = start_array;
    current_size.clear();

    // clear the data stream
    total_length = 0;
    if (current_buffer) {
        recv_buffers.front()->read_commit(current_buffer_length);
    }
    current_buffer = nullptr;
    current_buffer_length = 0;
    current_cursor = 0;
    while (!recv_buffers.empty()) {
        recv_buffers.front()->release_ref();
        recv_buffers.pop();
    }
}

char redis_parser::peek()
{
    prepare_current_buffer();
    return current_buffer[current_cursor];
}

bool redis_parser::eat(char c)
{
    prepare_current_buffer();
    if (current_buffer[current_cursor] == c) {
        ++current_cursor;
        --total_length;
        return true;
    } else {
        derror("%s: expect token: %c, got %c",
               remote_address.to_string(),
               c,
               current_buffer[current_cursor]);
        return false;
    }
}

void redis_parser::eat_all(char *dest, size_t length)
{
    total_length -= length;
    while (length > 0) {
        prepare_current_buffer();

        size_t eat_size = current_buffer_length - current_cursor;
        if (eat_size > length)
            eat_size = length;
        memcpy(dest, current_buffer + current_cursor, eat_size);
        dest += eat_size;
        current_cursor += eat_size;
        length -= eat_size;
    }
}

bool redis_parser::end_array_size()
{
    redis_request &current_array = current_msg->request;

    int32_t l;
    bool result = dsn::buf2int32(dsn::string_view(current_size.c_str(), current_size.length()), l);
    dverify_logged(result,
                   LOG_LEVEL_ERROR,
                   "%s: invalid size string \"%s\"",
                   remote_address.to_string(),
                   current_size.c_str());

    current_array.length = l;
    current_size.clear();
    dverify_logged(l > 0,
                   LOG_LEVEL_ERROR,
                   "%s: array size should be positive in redis request, but got %d",
                   remote_address.to_string(),
                   l);

    current_array.buffers.reserve(current_array.length);
    status = start_bulk_string;
    return true;
}

void redis_parser::append_current_bulk_string()
{
    redis_request &current_array = current_msg->request;
    current_array.buffers.push_back(current_str);
    if (current_array.buffers.size() == current_array.length) {
        // we get a full request command
        handle_command(std::move(current_msg));
        current_msg.reset(new message_entry());
        status = start_array;
    } else {
        status = start_bulk_string;
    }
}

bool redis_parser::end_bulk_string_size()
{
    int32_t l;
    bool result = dsn::buf2int32(dsn::string_view(current_size.c_str(), current_size.length()), l);
    dverify_logged(result, LOG_LEVEL_ERROR, "invalid size string \"%s\"", current_size.c_str());

    current_str.length = l;
    current_str.data.assign(nullptr, 0, 0);
    current_size.clear();
    if (-1 == current_str.length) {
        append_current_bulk_string();
    } else if (current_str.length >= 0) {
        status = start_bulk_string_data;
    } else {
        derror(
            "%s: invalid bulk string length: %d", remote_address.to_string(), current_str.length);
        return false;
    }
    return true;
}

void redis_parser::append_message(dsn::message_ex *msg)
{
    msg->add_ref();
    recv_buffers.push(msg);
    total_length += msg->body_size();
    dinfo("%s: recv message, currently total length:%d", remote_address.to_string(), total_length);
}

// refererence: http://redis.io/topics/protocol
bool redis_parser::parse_stream()
{
    char t;
    while (total_length > 0) {
        switch (status) {
        case start_array:
            dverify(eat('*'));
            status = in_array_size;
            break;
        case in_array_size:
        case in_bulk_string_size:
            t = peek();
            if (t == CR) {
                if (total_length > 1) {
                    dverify(eat(CR));
                    dverify(eat(LF));
                    if (in_array_size == status) {
                        dverify(end_array_size());
                    } else {
                        dverify(end_bulk_string_size());
                    }
                } else {
                    return true;
                }
            } else {
                current_size.push_back(t);
                dverify(eat(t));
            }
            break;
        case start_bulk_string:
            dverify(eat('$'));
            status = in_bulk_string_size;
            break;
        case start_bulk_string_data:
            // string content + CR + LF
            if (total_length >= current_str.length + 2) {
                if (current_str.length > 0) {
                    char *ptr = reinterpret_cast<char *>(dsn::tls_trans_malloc(current_str.length));
                    std::shared_ptr<char> str_data(ptr,
                                                   [](char *ptr) { dsn::tls_trans_free(ptr); });
                    eat_all(str_data.get(), current_str.length);
                    current_str.data.assign(std::move(str_data), 0, current_str.length);
                }
                dverify(eat(CR));
                dverify(eat(LF));
                append_current_bulk_string();
            } else
                return true;
            break;
        default:
            break;
        }
    }
    return true;
}

bool redis_parser::parse(dsn::message_ex *msg)
{
    append_message(msg);
    if (parse_stream())
        return true;
    else {
        // when parse a new message failed, we only reset the parser.
        // for pending responses msg queue, we should keep it as it is.
        reset_parser();
        return false;
    }
}

void redis_parser::enqueue_pending_response(std::unique_ptr<message_entry> &&entry)
{
    dsn::zauto_lock l(response_lock);
    pending_response.emplace_back(std::move(entry));
}

void redis_parser::fetch_and_dequeue_messages(std::vector<dsn::message_ex *> &msgs,
                                              bool only_ready_ones)
{
    dsn::zauto_lock l(response_lock);
    while (!pending_response.empty()) {
        message_entry *entry = pending_response.front().get();
        dsn::message_ex *r = entry->response.load(std::memory_order_acquire);
        if (only_ready_ones && r == nullptr) {
            break;
        } else {
            msgs.push_back(r);
            pending_response.pop_front();
        }
    }
}

void redis_parser::clear_reply_queue()
{
    // clear the response pipeline
    std::vector<dsn::message_ex *> all_responses;
    fetch_and_dequeue_messages(all_responses, false);
    for (dsn::message_ex *m : all_responses) {
        if (m != nullptr) {
            m->release_ref();
        }
    }
}

void redis_parser::reply_all_ready()
{
    std::vector<dsn::message_ex *> ready_responses;
    fetch_and_dequeue_messages(ready_responses, true);
    for (dsn::message_ex *m : ready_responses) {
        dassert(m != nullptr, "");
        dsn_rpc_reply(m, ::dsn::ERR_OK);
        // added when message is created
        m->release_ref();
    }
}

void redis_parser::default_handler(redis_parser::message_entry &entry)
{
    ::dsn::blob &cmd = entry.request.buffers[0].data;
    redis_simple_string result;
    result.is_error = true;
    result.message = "ERR unknown command '" + std::string(cmd.data(), cmd.length()) + "'";
    ddebug("%s: %s with seqid %" PRId64 "",
           remote_address.to_string(),
           result.message.c_str(),
           entry.sequence_id);
    reply_message(entry, result);
}

void redis_parser::set(redis_parser::message_entry &entry)
{
    if (_geo_client == nullptr) {
        set_internal(entry);
    } else {
        set_geo_internal(entry);
    }
}

void redis_parser::set_internal(redis_parser::message_entry &entry)
{
    redis_request &request = entry.request;
    if (request.buffers.size() < 3) {
        ddebug("%s: set command with invalid arguments, seqid(%" PRId64 ")",
               remote_address.to_string(),
               entry.sequence_id);
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'set' command";
        reply_message(entry, result);
    } else {
        int ttl_seconds = 0;
        parse_set_parameters(request.buffers, ttl_seconds);

        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        dinfo("%s: send set command(%" PRId64 ")", remote_address.to_string(), entry.sequence_id);
        auto on_set_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            // when the "is_session_reset" flag is set, the socket may be broken.
            // so continue to reply the message is not necessary
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: set command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            // the message_enry "entry" is stored in the queue "pending_response".
            // please ensure that "entry" hasn't been released right now.
            //
            // currently we only clear an entry when it is replied or
            // in the redis_parser's destructor
            dinfo("%s: set command seqid(%" PRId64 ") got reply",
                  remote_address.to_string(),
                  entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                ddebug("%s: set command seqid(%" PRId64 ") got reply with error = %s",
                       remote_address.to_string(),
                       entry.sequence_id,
                       ec.to_string());
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + ec.to_string();
                reply_message(entry, result);
            } else {
                ::dsn::apps::update_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    redis_simple_string result;
                    result.is_error = true;
                    result.message = "ERR internal error " +
                                     boost::lexical_cast<std::string>(rrdb_response.error);
                    reply_message(entry, result);
                } else {
                    redis_simple_string result;
                    result.is_error = false;
                    result.message = "OK";
                    reply_message(entry, result);
                }
            }
        };

        ::dsn::apps::update_request req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req.key, request.buffers[1].data, null_blob);
        req.value = request.buffers[2].data;
        if (ttl_seconds == 0)
            req.expire_ts_seconds = 0;
        else
            req.expire_ts_seconds = ttl_seconds + utils::epoch_now();
        auto partition_hash = pegasus_key_hash(req.key);
        // TODO: set the timeout
        client->put(req, on_set_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

// origin command format:
// SET key value [EX seconds] [PX milliseconds] [NX|XX]
// NOTE: only 'EX' option is supported
void redis_parser::set_geo_internal(message_entry &entry)
{
    redis_request &redis_request = entry.request;
    if (redis_request.buffers.size() < 3) {
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'SET' command";
        reply_message(entry, result);
    } else {
        int ttl_seconds = 0;
        parse_set_parameters(redis_request.buffers, ttl_seconds);

        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto set_callback = [ref_this, this, &entry](int ec, pegasus_client::internal_info &&) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: setex command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            if (PERR_OK != ec) {
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + _geo_client->get_error_string(ec);
                reply_message(entry, result);
            } else {
                redis_simple_string result;
                result.is_error = false;
                result.message = "OK";
                reply_message(entry, result);
            }
        };
        _geo_client->async_set(redis_request.buffers[1].data.to_string(), // key => hash_key
                               std::string(),                             // ""  => sort_key
                               redis_request.buffers[2].data.to_string(), // value
                               set_callback,
                               2000, // TODO: set the timeout
                               ttl_seconds);
    }
}

void redis_parser::setex(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    // setex key ttl_SECONDS value
    if (redis_req.buffers.size() != 4) {
        ddebug("%s: setex command seqid(%" PRId64 ") with invalid arguments",
               remote_address.to_string(),
               entry.sequence_id);
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'setex' command";
        reply_message(entry, result);
    } else {
        dinfo("%s: send setex command seqid(%" PRId64 ")",
              remote_address.to_string(),
              entry.sequence_id);
        redis_simple_string result;
        ::dsn::blob &ttl_blob = redis_req.buffers[2].data;
        int ttl_seconds;
        if (!dsn::buf2int32(ttl_blob, ttl_seconds)) {
            result.is_error = true;
            result.message = "ERR value is not an integer or out of range";
            reply_message(entry, result);
            return;
        }
        if (ttl_seconds <= 0) {
            result.is_error = true;
            result.message = "ERR invalid expire time in setex";
            reply_message(entry, result);
            return;
        }

        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_setex_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: setex command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            dinfo("%s: setex command seqid(%" PRId64 ") got reply",
                  remote_address.to_string(),
                  entry.sequence_id);
            redis_simple_string result;
            if (::dsn::ERR_OK != ec) {
                ddebug("%s: setex command seqid(%" PRId64 ") got reply with error = %s",
                       remote_address.to_string(),
                       entry.sequence_id,
                       ec.to_string());
                result.is_error = true;
                result.message = std::string("ERR ") + ec.to_string();
                reply_message(entry, result);
                return;
            }

            ::dsn::apps::update_response rrdb_response;
            ::dsn::unmarshall(response, rrdb_response);
            if (rrdb_response.error != 0) {
                result.is_error = true;
                result.message =
                    "ERR internal error " + boost::lexical_cast<std::string>(rrdb_response.error);
                reply_message(entry, result);
                return;
            }

            result.is_error = false;
            result.message = "OK";
            reply_message(entry, result);
        };

        ::dsn::apps::update_request req;
        ::dsn::blob null_blob;

        pegasus_generate_key(req.key, redis_req.buffers[1].data, null_blob);
        req.value = redis_req.buffers[3].data;
        req.expire_ts_seconds = pegasus::utils::epoch_now() + ttl_seconds;

        auto partition_hash = pegasus_key_hash(req.key);

        // TODO: set the timeout
        client->put(req, on_setex_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

void redis_parser::get(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    if (redis_req.buffers.size() != 2) {
        ddebug("%s: get command seqid(%" PRId64 ") with invalid arguments",
               remote_address.to_string(),
               entry.sequence_id);
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'get' command";
        reply_message(entry, result);
    } else {
        dinfo("%s: send get command seqid(%" PRId64 ")",
              remote_address.to_string(),
              entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_get_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: get command(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            dinfo("%s: get command seqid(%" PRId64 ") got reply",
                  remote_address.to_string(),
                  entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                ddebug("%s: get command seqid(%" PRId64 ") got reply with error = %s",
                       remote_address.to_string(),
                       entry.sequence_id,
                       ec.to_string());
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + ec.to_string();
                reply_message(entry, result);
            } else {
                ::dsn::apps::read_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    if (rrdb_response.error == rocksdb::Status::kNotFound) {
                        redis_bulk_string result;
                        result.length = -1;
                        reply_message(entry, result);
                    } else {
                        redis_simple_string result;
                        result.is_error = true;
                        result.message = "ERR internal error " +
                                         boost::lexical_cast<std::string>(rrdb_response.error);
                        reply_message(entry, result);
                    }
                } else {
                    redis_bulk_string result(rrdb_response.value);
                    reply_message(entry, result);
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.buffers[1].data, null_blob);
        auto partition_hash = pegasus_key_hash(req);
        // TODO: set the timeout
        client->get(req, on_get_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

void redis_parser::del(message_entry &entry)
{
    if (_geo_client == nullptr) {
        del_internal(entry);
    } else {
        del_geo_internal(entry);
    }
}

void redis_parser::del_internal(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    if (redis_req.buffers.size() != 2) {
        ddebug("%s: del command seqid(%" PRId64 ") with invalid arguments",
               remote_address.to_string(),
               entry.sequence_id);
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'del' command";
        reply_message(entry, result);
    } else {
        dinfo("%s: send del command seqid(%" PRId64 ")",
              remote_address.to_string(),
              entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_del_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: del command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            dinfo("%s: del command seqid(%" PRId64 ") got reply",
                  remote_address.to_string(),
                  entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                ddebug("%s: del command seqid(%" PRId64 ") got reply with error = %s",
                       remote_address.to_string(),
                       entry.sequence_id,
                       ec.to_string());
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + ec.to_string();
                reply_message(entry, result);
            } else {
                ::dsn::apps::read_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    redis_simple_string result;
                    result.is_error = true;
                    result.message = "ERR internal error " +
                                     boost::lexical_cast<std::string>(rrdb_response.error);
                    reply_message(entry, result);
                } else {
                    redis_integer result;
                    result.value = 1;
                    reply_message(entry, result);
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.buffers[1].data, null_blob);
        auto partition_hash = pegasus_key_hash(req);
        // TODO: set the timeout
        client->remove(req, on_del_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

// origin command format:
// DEL key [key ...]
// NOTE: only one key is supported
void redis_parser::del_geo_internal(message_entry &entry)
{
    redis_request &redis_request = entry.request;
    if (redis_request.buffers.size() != 2) {
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'DEL' command";
        reply_message(entry, result);
    } else {
        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto del_callback = [ref_this, this, &entry](int ec, pegasus_client::internal_info &&) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: setex command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            if (PERR_OK != ec) {
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + _geo_client->get_error_string(ec);
                reply_message(entry, result);
            } else {
                redis_simple_string result;
                result.is_error = false;
                result.message = "OK";
                reply_message(entry, result);
            }
        };
        _geo_client->async_del(redis_request.buffers[1].data.to_string(), // key => hash_key
                               std::string(),                             // ""  => sort_key
                               false,
                               del_callback,
                               2000); // TODO: set the timeout
    }
}

// process 'ttl' and 'pttl'
void redis_parser::ttl(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    bool is_ttl = (toupper(redis_req.buffers[0].data.data()[0]) == 'T');
    if (redis_req.buffers.size() != 2) {
        ddebug("%s: ttl/pttl command seqid(%" PRId64 ") with invalid arguments",
               remote_address.to_string(),
               entry.sequence_id);
        redis_simple_string result;
        result.is_error = true;
        if (is_ttl)
            result.message = "ERR wrong number of arguments for 'ttl' command";
        else
            result.message = "ERR wrong number of arguments for 'pttl' command";
        reply_message(entry, result);
    } else {
        dinfo("%s: send pttl/ttl command seqid(%" PRId64 ")",
              remote_address.to_string(),
              entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_ttl_reply = [ref_this, this, &entry, is_ttl](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: ttl/pttl command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            dinfo("%s: ttl/pttl command seqid(%" PRId64 ") got reply",
                  remote_address.to_string(),
                  entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                ddebug("%s: del command seqid(%" PRId64 ") got reply with error = %s",
                       remote_address.to_string(),
                       entry.sequence_id,
                       ec.to_string());
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + ec.to_string();
                reply_message(entry, result);
            } else {
                ::dsn::apps::ttl_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    if (rrdb_response.error == rocksdb::Status::kNotFound) {
                        redis_integer result;
                        result.value = -2;
                        reply_message(entry, result);
                    } else {
                        redis_simple_string result;
                        result.is_error = true;
                        result.message = "ERR internal error " +
                                         boost::lexical_cast<std::string>(rrdb_response.error);
                        reply_message(entry, result);
                    }
                } else {
                    redis_integer result;
                    if (is_ttl)
                        result.value = rrdb_response.ttl_seconds;
                    else // pttl
                        result.value = rrdb_response.ttl_seconds * 1000;
                    reply_message(entry, result);
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.buffers[1].data, null_blob);
        auto partition_hash = pegasus_key_hash(req);
        // TODO: set the timeout
        client->ttl(req, on_ttl_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

// command format:
// GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT
// count] [ASC|DESC] [STORE key] [STOREDIST key]
// NOTE: [STORE key] [STOREDIST key] are not supported
// NOTE: [WITHHASH] will return origin value of member when enabled
// NOTE: we use SET instead of GEOADD to insert data into pegasus, so there is not a `key` as in
// Redis(`GEOADD key longitude latitude member`), and we consider that all geo data in pegasus is
// under "" key, so when execute 'GEORADIUS' command, the `key` parameter will always be ignored and
// treated as "".
// eg: GEORADIUS "" 146.123 34.567 1000
void redis_parser::geo_radius(message_entry &entry)
{
    redis_request &redis_request = entry.request;
    if (redis_request.buffers.size() < 5) {
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'GEORADIUS' command";
        reply_message(entry, result);
        return;
    }

    // longitude latitude
    double lng_degrees = 0.0;
    const std::string &str_lng_degrees = redis_request.buffers[2].data.to_string();
    if (!dsn::buf2double(str_lng_degrees, lng_degrees)) {
        dwarn_f("longitude parameter '{}' is error, use {}", str_lng_degrees, lng_degrees);
    }
    double lat_degrees = 0.0;
    const std::string &str_lat_degrees = redis_request.buffers[3].data.to_string();
    if (!dsn::buf2double(str_lat_degrees, lat_degrees)) {
        dwarn_f("latitude parameter '{}' is error, use {}", str_lat_degrees, lat_degrees);
    }

    // radius m|km|ft|mi [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
    double radius_m = 100.0;
    std::string unit;
    geo::geo_client::SortType sort_type = geo::geo_client::SortType::random;
    int count = -1;
    bool WITHCOORD = false;
    bool WITHDIST = false;
    bool WITHHASH = false;
    parse_geo_radius_parameters(
        redis_request.buffers, 4, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHHASH);

    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    auto search_callback = [ref_this, this, &entry, unit, WITHCOORD, WITHDIST, WITHHASH](
        int ec, std::list<geo::SearchResult> &&results) {
        process_geo_radius_result(
            entry, unit, WITHCOORD, WITHDIST, WITHHASH, ec, std::move(results));
    };

    _geo_client->async_search_radial(
        lat_degrees, lng_degrees, radius_m, count, sort_type, 2000, search_callback);
}

// command format:
// GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]
// [ASC|DESC] [STORE key] [STOREDIST key]
// NOTE: [STORE key] [STOREDIST key] are not supported
// NOTE: [WITHHASH] will return origin value of member when enabled
// NOTE: we use SET instead of GEOADD to insert data into pegasus, so there is not a `key` as in
// Redis(`GEOADD key longitude latitude member`), and we consider that all geo data in pegasus is
// under "" key, so when execute 'GEORADIUSBYMEMBER' command, the `key` parameter will always be
// ignored and
// treated as "", and the `member` parameter is treated as `key` which is inserted by SET command.
// eg: GEORADIUSBYMEMBER "" some_key 1000
void redis_parser::geo_radius_by_member(message_entry &entry)
{
    redis_request &redis_request = entry.request;
    if (redis_request.buffers.size() < 4) {
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'GEORADIUSBYMEMBER' command";
        reply_message(entry, result);
        return;
    }

    // member
    std::string hash_key = redis_request.buffers[2].data.to_string(); // member => hash_key

    // radius m|km|ft|mi [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC] [WITHHASH]
    double radius_m = 100.0;
    std::string unit;
    geo::geo_client::SortType sort_type = geo::geo_client::SortType::random;
    int count = -1;
    bool WITHCOORD = false;
    bool WITHDIST = false;
    bool WITHHASH = false;
    parse_geo_radius_parameters(
        redis_request.buffers, 3, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHHASH);

    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    auto search_callback = [ref_this, this, &entry, unit, WITHCOORD, WITHDIST, WITHHASH](
        int ec, std::list<geo::SearchResult> &&results) {
        process_geo_radius_result(
            entry, unit, WITHCOORD, WITHDIST, WITHHASH, ec, std::move(results));
    };

    _geo_client->async_search_radial(
        hash_key, "", radius_m, count, sort_type, 2000, search_callback);
}

void redis_parser::incr(message_entry &entry) { counter_internal(entry); }

void redis_parser::incr_by(message_entry &entry) { counter_internal(entry); }

void redis_parser::decr(message_entry &entry) { counter_internal(entry); }

void redis_parser::decr_by(message_entry &entry) { counter_internal(entry); }

void redis_parser::counter_internal(message_entry &entry)
{
    dassert(!entry.request.buffers.empty(), "");
    dassert(entry.request.buffers[0].length > 0, "");
    const char *command = entry.request.buffers[0].data.data();
    int64_t increment = 1;
    if (strcasecmp(command, "INCR") == 0 || strcasecmp(command, "DECR") == 0) {
        if (entry.request.buffers.size() != 2) {
            dwarn_f("{}: command {} seqid({}) with invalid arguments count: {}",
                    remote_address.to_string(),
                    command,
                    entry.sequence_id,
                    entry.request.buffers.size());
            redis_simple_string result;
            result.is_error = true;
            result.message = fmt::format("ERR wrong number of arguments for '{}'", command);
            reply_message(entry, result);
            return;
        }
    } else if (strcasecmp(command, "INCRBY") == 0 || strcasecmp(command, "DECRBY") == 0) {
        if (entry.request.buffers.size() != 3) {
            dwarn_f("{}: command {} seqid({}) with invalid arguments count: {}",
                    remote_address.to_string(),
                    command,
                    entry.sequence_id,
                    entry.request.buffers.size());
            redis_simple_string result;
            result.is_error = true;
            result.message = fmt::format("ERR wrong number of arguments for '{}'", command);
            reply_message(entry, result);
            return;
        }
        if (!dsn::buf2int64(entry.request.buffers[2].data, increment)) {
            dwarn_f("{}: command {} seqid({}) with invalid 'increment': {}",
                    remote_address.to_string(),
                    command,
                    entry.sequence_id,
                    entry.request.buffers[2].data.to_string());
            redis_simple_string result;
            result.is_error = true;
            result.message =
                fmt::format("ERR wrong type of argument 'increment 'for '{}'", command);
            reply_message(entry, result);
            return;
        }
    } else {
        dfatal_f("command not support: {}", command);
    }
    if (strncasecmp(command, "DECR", 4) == 0) {
        increment = -increment;
    }

    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    auto on_incr_reply = [ref_this, this, command, &entry](
        ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
        if (is_session_reset.load(std::memory_order_acquire)) {
            dwarn_f("{}: command {} seqid({}) got reply, but session has reset",
                    remote_address.to_string(),
                    command,
                    entry.sequence_id);
            return;
        }

        if (::dsn::ERR_OK != ec) {
            dwarn_f("{}: command {} seqid({}) got reply with error = {}",
                    remote_address.to_string(),
                    command,
                    entry.sequence_id,
                    ec.to_string());
            redis_simple_string result;
            result.is_error = true;
            result.message = std::string("ERR ") + ec.to_string();
            reply_message(entry, result);
        } else {
            ::dsn::apps::incr_response incr_resp;
            ::dsn::unmarshall(response, incr_resp);
            if (incr_resp.error != 0) {
                redis_simple_string result;
                result.is_error = true;
                result.message = "ERR internal error " + std::to_string(incr_resp.error);
                reply_message(entry, result);
            } else {
                redis_integer result;
                result.value = incr_resp.new_value;
                reply_message(entry, result);
            }
        }
    };
    dsn::apps::incr_request req;
    pegasus_generate_key(req.key, entry.request.buffers[1].data, dsn::blob());
    req.increment = increment;
    client->incr(req, on_incr_reply, std::chrono::milliseconds(2000), 0, pegasus_key_hash(req.key));
}

void redis_parser::parse_set_parameters(const std::vector<redis_bulk_string> &opts,
                                        int &ttl_seconds)
{
    // [EX seconds]
    ttl_seconds = 0;
    for (int i = 3; i < opts.size(); ++i) {
        const std::string &opt = opts[i].data.to_string();
        if (strcasecmp(opt.c_str(), "EX") == 0 && i + 1 < opts.size()) {
            const std::string &str_ttl_seconds = opts[i + 1].data.to_string();
            if (!dsn::buf2int32(str_ttl_seconds, ttl_seconds)) {
                dwarn_f("'EX {}' option is error, use {}", str_ttl_seconds, ttl_seconds);
            }
        } else {
            dwarn_f("only 'EX' option is supported");
        }
    }
}

void redis_parser::parse_geo_radius_parameters(const std::vector<redis_bulk_string> &opts,
                                               int base_index,
                                               double &radius_m,
                                               std::string &unit,
                                               geo::geo_client::SortType &sort_type,
                                               int &count,
                                               bool &WITHCOORD,
                                               bool &WITHDIST,
                                               bool &WITHHASH)
{
    // radius
    if (base_index >= opts.size()) {
        return;
    }
    const std::string &str_radius = opts[base_index++].data.to_string();
    if (!dsn::buf2double(str_radius, radius_m)) {
        dwarn_f("radius parameter '{}' is error, use {}", str_radius, radius_m);
    }

    // m|km|ft|mi
    if (base_index >= opts.size()) {
        return;
    }
    unit = opts[base_index++].data.to_string();
    if (unit == "km") {
        radius_m *= 1000;
    } else if (unit == "mi") {
        radius_m *= 1609.344;
    } else if (unit == "ft") {
        radius_m *= 0.3048;
    } else {
        // keep as meter unit
        unit = "m";
        base_index--;
    }

    // [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    while (base_index < opts.size()) {
        const std::string &opt = opts[base_index].data.to_string();
        if (strcasecmp(opt.c_str(), "WITHCOORD") == 0) {
            WITHCOORD = true;
        } else if (strcasecmp(opt.c_str(), "WITHDIST") == 0) {
            WITHDIST = true;
        } else if (strcasecmp(opt.c_str(), "WITHHASH") == 0) {
            WITHHASH = true;
        } else if (strcasecmp(opt.c_str(), "COUNT") == 0 && base_index + 1 < opts.size()) {
            const std::string &str_count = opts[base_index + 1].data.to_string();
            if (!dsn::buf2int32(str_count, count)) {
                derror_f("'COUNT {}' option is error, use {}", str_count, count);
            }
        } else if (strcasecmp(opt.c_str(), "ASC") == 0) {
            sort_type = geo::geo_client::SortType::asc;
        } else if (strcasecmp(opt.c_str(), "DESC") == 0) {
            sort_type = geo::geo_client::SortType::desc;
        }
        base_index++;
    }
}

void redis_parser::process_geo_radius_result(message_entry &entry,
                                             const std::string &unit,
                                             bool WITHCOORD,
                                             bool WITHDIST,
                                             bool WITHHASH,
                                             int ec,
                                             std::list<geo::SearchResult> &&results)
{
    if (is_session_reset.load(std::memory_order_acquire)) {
        ddebug("%s: setex command seqid(%" PRId64 ") got reply, but session has reset",
               remote_address.to_string(),
               entry.sequence_id);
        return;
    }

    if (PERR_OK != ec) {
        redis_simple_string result;
        result.is_error = true;
        result.message = std::string("ERR ") + _geo_client->get_error_string(ec);
        reply_message(entry, result);
    } else {
        redis_array result;
        result.count = (int)results.size();
        for (const auto &elem : results) {
            std::shared_ptr<redis_base_type> key = std::make_shared<redis_bulk_string>(
                (int)elem.hash_key.size(), elem.hash_key.data()); // hash_key => member
            if (!WITHCOORD && !WITHDIST && !WITHHASH) {
                // only member
                result.array.push_back(key);
            } else {
                // member and some WITH* parameters
                std::shared_ptr<redis_array> sub_array = std::make_shared<redis_array>();

                // member
                sub_array->array.push_back(key);
                sub_array->count++;

                // NOTE: the order of WITH* parameters should not be changed for the redis
                // protocol
                if (WITHDIST) {
                    // with distance
                    double distance = elem.distance;
                    if (unit == "km") {
                        distance /= 1000;
                    } else if (unit == "mi") {
                        distance /= 1609.344;
                    } else if (unit == "ft") {
                        distance /= 0.3048;
                    } else {
                        // keep as meter unit
                    }
                    std::string dist = std::to_string(distance);
                    std::shared_ptr<char> dist_buf =
                        dsn::utils::make_shared_array<char>(dist.size());
                    memcpy(dist_buf.get(), dist.data(), dist.size());
                    sub_array->array.push_back(std::make_shared<redis_bulk_string>(
                        dsn::blob(std::move(dist_buf), (int)dist.size())));
                    sub_array->count++;
                }
                if (WITHCOORD) {
                    // with coordinate
                    std::shared_ptr<redis_array> coordinate = std::make_shared<redis_array>();

                    // longitude
                    std::string lng = std::to_string(elem.lng_degrees);
                    std::shared_ptr<char> lng_buf = dsn::utils::make_shared_array<char>(lng.size());
                    memcpy(lng_buf.get(), lng.data(), lng.size());
                    coordinate->array.push_back(std::make_shared<redis_bulk_string>(
                        dsn::blob(std::move(lng_buf), (int)lng.size())));
                    coordinate->count++;

                    // latitude
                    std::string lat = std::to_string(elem.lat_degrees);
                    std::shared_ptr<char> lat_buf = dsn::utils::make_shared_array<char>(lat.size());
                    memcpy(lat_buf.get(), lat.data(), lat.size());
                    coordinate->array.push_back(std::make_shared<redis_bulk_string>(
                        dsn::blob(std::move(lat_buf), (int)lat.size())));
                    coordinate->count++;

                    sub_array->array.push_back(coordinate);
                    sub_array->count++;
                }
                if (WITHHASH) {
                    // with origin value
                    sub_array->array.push_back(std::make_shared<redis_bulk_string>(
                        (int)elem.value.size(), elem.value.data()));
                    sub_array->count++;
                }
                result.array.push_back(sub_array);
            }
        }
        reply_message(entry, result);
    }
}

// command format:
// GEODIST key member1 member2 [unit]
void redis_parser::geo_dist(message_entry &entry)
{
    redis_request &redis_request = entry.request;
    if (redis_request.buffers.size() < 4) {
        redis_simple_string result;
        result.is_error = true;
        result.message = "ERR wrong number of arguments for 'GEODIST' command";
        reply_message(entry, result);
    } else {
        // TODO: set the timeout
        std::string hash_key1 = redis_request.buffers[2].data.to_string(); // member1 => hash_key1
        std::string hash_key2 = redis_request.buffers[3].data.to_string(); // member2 => hash_key2
        std::string unit = redis_request.buffers[4].data.to_string();

        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto get_callback = [ref_this, this, &entry, unit](int error_code, double &&distance) {
            if (is_session_reset.load(std::memory_order_acquire)) {
                ddebug("%s: setex command seqid(%" PRId64 ") got reply, but session has reset",
                       remote_address.to_string(),
                       entry.sequence_id);
                return;
            }

            if (PERR_OK != error_code) {
                redis_simple_string result;
                result.is_error = true;
                result.message = std::string("ERR ") + _geo_client->get_error_string(error_code);
                reply_message(entry, result);
            } else {
                if (unit == "km") {
                    distance /= 1000;
                } else if (unit == "mi") {
                    distance /= 1609.344;
                } else if (unit == "ft") {
                    distance /= 0.3048;
                } else {
                    // keep as meter unit
                }

                std::string str_distance = std::to_string(distance);
                redis_bulk_string result((int)str_distance.size(), str_distance.data());
                reply_message(entry, result);
            }
        };
        _geo_client->async_distance(hash_key1, "", hash_key2, "", 2000, get_callback);
    }
}

void redis_parser::handle_command(std::unique_ptr<message_entry> &&entry)
{
    message_entry &e = *entry.get();
    redis_request &request = e.request;
    e.sequence_id = ++s_next_seqid;
    e.response.store(nullptr, std::memory_order_relaxed);

    dinfo("%s: new command parsed with new seqid %" PRId64 "",
          remote_address.to_string(),
          e.sequence_id);
    enqueue_pending_response(std::move(entry));

    dassert(request.length > 0, "invalid request, request.length = %d", request.length);
    ::dsn::blob &command = request.buffers[0].data;
    redis_call_handler handler = redis_parser::get_handler(command.data(), command.length());
    handler(this, e);
}

void redis_parser::redis_integer::marshalling(::dsn::binary_writer &write_stream) const
{
    write_stream.write_pod(':');
    std::string result = std::to_string(value);
    write_stream.write(result.c_str(), (int)result.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
}

void redis_parser::redis_simple_string::marshalling(::dsn::binary_writer &write_stream) const
{
    write_stream.write_pod(is_error ? '-' : '+');
    write_stream.write(message.c_str(), (int)message.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
}

void redis_parser::redis_bulk_string::marshalling(::dsn::binary_writer &write_stream) const
{
    write_stream.write_pod('$');
    std::string result = std::to_string(length);
    write_stream.write(result.c_str(), (int)result.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
    if (length < 0) {
        return;
    }
    if (length > 0) {
        dassert(data.length() == length, "%u VS %d", data.length(), length);
        write_stream.write(data.data(), length);
    }
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
}

void redis_parser::redis_array::marshalling(::dsn::binary_writer &write_stream) const
{
    write_stream.write_pod('*');
    std::string result = std::to_string(count);
    write_stream.write(result.c_str(), (int)result.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
    if (count > 0) {
        dassert_f(array.size() == count, "{} VS {}", array.size(), count);
        for (const auto &elem : array) {
            elem->marshalling(write_stream);
        }
    }
}
} // namespace proxy
} // namespace pegasus
