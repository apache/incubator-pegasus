/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "redis_parser.h"

#include <ctype.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <pegasus/error.h>
#include <pegasus_key_schema.h>
#include <pegasus_utils.h>
#include <rocksdb/status.h>
#include <rrdb/rrdb.client.h>
#include <string.h>
#include <algorithm>
#include <chrono>
#include <cstdint>

#include "base/pegasus_const.h"
#include "common/replication_other_types.h"
#include "pegasus/client.h"
#include "rrdb/rrdb_types.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/serialization.h"
#include "utils/api_utilities.h"
#include "utils/binary_writer.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"
#include "utils/utils.h"

namespace pegasus {
namespace proxy {

std::atomic_llong redis_parser::s_next_seqid(0);
const char redis_parser::CR = '\015';
const char redis_parser::LF = '\012';

std::unordered_map<std::string, redis_parser::redis_call_handler> redis_parser::s_dispatcher = {
    {"SET", redis_parser::g_set},
    {"GET", redis_parser::g_get},
    {"DEL", redis_parser::g_del},
    {"SETEX", redis_parser::g_setex},
    {"TTL", redis_parser::g_ttl},
    {"PTTL", redis_parser::g_ttl},
    {"GEOADD", redis_parser::g_geo_add},
    {"GEODIST", redis_parser::g_geo_dist},
    {"GEOPOS", redis_parser::g_geo_pos},
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
      _current_msg(new message_entry()),
      _status(kStartArray),
      _current_size(),
      _total_length(0),
      _current_buffer(nullptr),
      _current_buffer_length(0),
      _current_cursor(0)
{
    ::dsn::apps::rrdb_client *r;
    if (op) {
        std::vector<dsn::rpc_address> meta_list;
        dsn::replication::replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), op->get_cluster());
        r = new ::dsn::apps::rrdb_client(op->get_cluster(), meta_list, op->get_app());
        if (!dsn::utils::is_empty(op->get_geo_app())) {
            _geo_client = std::make_unique<geo::geo_client>(
                "config.ini", op->get_cluster(), op->get_app(), op->get_geo_app());
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
    LOG_INFO_PREFIX("redis parser destroyed");
}

void redis_parser::prepare_current_buffer()
{
    void *msg_buffer;
    if (_current_buffer == nullptr) {
        dsn::message_ex *first_msg = _recv_buffers.front();
        CHECK(first_msg->read_next(&msg_buffer, &_current_buffer_length),
              "read dsn::message_ex* failed, msg from_address = {}, to_address = {}, rpc_name = {}",
              first_msg->header->from_address.to_string(),
              first_msg->to_address.to_string(),
              first_msg->header->rpc_name);
        _current_buffer = static_cast<char *>(msg_buffer);
        _current_cursor = 0;
    } else if (_current_cursor >= _current_buffer_length) {
        dsn::message_ex *first_msg = _recv_buffers.front();
        first_msg->read_commit(_current_buffer_length);
        if (first_msg->read_next(&msg_buffer, &_current_buffer_length)) {
            _current_buffer = static_cast<char *>(msg_buffer);
            _current_cursor = 0;
        } else {
            // we have consume this message all over
            // reference is added in append message
            first_msg->release_ref();
            _recv_buffers.pop();
            _current_buffer = nullptr;
            prepare_current_buffer();
        }
    }
}

void redis_parser::reset_parser()
{
    // clear the parser status
    _current_msg->request.sub_request_count = 0;
    _current_msg->request.sub_requests.clear();
    _status = kStartArray;
    _current_size.clear();

    // clear the data stream
    _total_length = 0;
    if (_current_buffer) {
        _recv_buffers.front()->read_commit(_current_buffer_length);
    }
    _current_buffer = nullptr;
    _current_buffer_length = 0;
    _current_cursor = 0;
    while (!_recv_buffers.empty()) {
        _recv_buffers.front()->release_ref();
        _recv_buffers.pop();
    }
}

char redis_parser::peek()
{
    prepare_current_buffer();
    return _current_buffer[_current_cursor];
}

bool redis_parser::eat(char c)
{
    if (dsn_likely(peek() == c)) {
        ++_current_cursor;
        --_total_length;
        return true;
    } else {
        LOG_ERROR("{}: expect token: {}, got {}", _remote_address.to_string(), c, peek());
        return false;
    }
}

void redis_parser::eat_all(char *dest, size_t length)
{
    _total_length -= length;
    while (length > 0) {
        prepare_current_buffer();

        size_t eat_size = _current_buffer_length - _current_cursor;
        if (eat_size > length) {
            eat_size = length;
        }
        memcpy(dest, _current_buffer + _current_cursor, eat_size);
        dest += eat_size;
        _current_cursor += eat_size;
        length -= eat_size;
    }
}

bool redis_parser::end_array_size()
{
    int32_t count = 0;
    if (dsn_unlikely(!dsn::buf2int32(absl::string_view(_current_size), count))) {
        LOG_ERROR(
            "{}: invalid size string \"{}\"", _remote_address.to_string(), _current_size.c_str());
        return false;
    }
    if (dsn_unlikely(count <= 0)) {
        LOG_ERROR("{}: array size should be positive in redis request, but got {}",
                  _remote_address.to_string(),
                  count);
        return false;
    }

    redis_request &current_request = _current_msg->request;
    current_request.sub_request_count = count;
    current_request.sub_requests.reserve(count);

    _current_size.clear();
    _status = kStartBulkString;
    return true;
}

void redis_parser::append_current_bulk_string()
{
    redis_request &current_array = _current_msg->request;
    current_array.sub_requests.push_back(_current_str);
    if (current_array.sub_requests.size() == current_array.sub_request_count) {
        // we get a full request command
        handle_command(std::move(_current_msg));
        _current_msg.reset(new message_entry());
        _status = kStartArray;
    } else {
        _status = kStartBulkString;
    }
}

bool redis_parser::end_bulk_string_size()
{
    int32_t length = 0;
    if (dsn_unlikely(!dsn::buf2int32(absl::string_view(_current_size), length))) {
        LOG_ERROR(
            "{}: invalid size string \"{}\"", _remote_address.to_string(), _current_size.c_str());
        return false;
    }

    _current_str.length = length;
    _current_str.data.assign(nullptr, 0, 0);
    _current_size.clear();

    if (-1 == _current_str.length) {
        append_current_bulk_string();
        return true;
    }

    if (_current_str.length >= 0) {
        _status = kStartBulkStringData;
        return true;
    }

    LOG_ERROR(
        "{}: invalid bulk string length: {}", _remote_address.to_string(), _current_str.length);
    return false;
}

void redis_parser::append_message(dsn::message_ex *msg)
{
    msg->add_ref();
    _recv_buffers.push(msg);
    _total_length += msg->body_size();
    LOG_DEBUG_PREFIX("recv message, currently total length: {}", _total_length);
}

// refererence: http://redis.io/topics/protocol
bool redis_parser::parse_stream()
{
    char t;
    while (_total_length > 0) {
        switch (_status) {
        case kStartArray:
            dverify(eat('*'));
            _status = kInArraySize;
            break;
        case kStartBulkString:
            dverify(eat('$'));
            _status = kInBulkStringSize;
            break;
        case kInArraySize:
        case kInBulkStringSize:
            t = peek();
            if (t == CR) {
                if (_total_length > 1) {
                    dverify(eat(CR));
                    dverify(eat(LF));
                    if (kInArraySize == _status) {
                        dverify(end_array_size());
                    } else {
                        dverify(end_bulk_string_size());
                    }
                } else {
                    return true;
                }
            } else {
                _current_size.push_back(t);
                dverify(eat(t));
            }
            break;
        case kStartBulkStringData:
            // string content + CR + LF
            if (_total_length >= _current_str.length + 2) {
                if (_current_str.length > 0) {
                    std::string str_data(_current_str.length, '\0');
                    eat_all(const_cast<char *>(str_data.data()), _current_str.length);
                    _current_str.data = dsn::blob::create_from_bytes(std::move(str_data));
                }
                dverify(eat(CR));
                dverify(eat(LF));
                append_current_bulk_string();
            } else {
                return true;
            }
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
    if (parse_stream()) {
        return true;
    } else {
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
        CHECK(m, "");
        dsn_rpc_reply(m, ::dsn::ERR_OK);
        // added when message is created
        m->release_ref();
    }
}

std::shared_ptr<redis_parser::redis_bulk_string> redis_parser::construct_bulk_string(double data)
{
    std::string data_str(std::to_string(data));
    std::shared_ptr<char> buf = dsn::utils::make_shared_array<char>(data_str.size());
    memcpy(buf.get(), data_str.data(), data_str.size());
    return std::make_shared<redis_bulk_string>(dsn::blob(std::move(buf), (int)data_str.size()));
}

void redis_parser::simple_ok_reply(message_entry &entry)
{
    simple_string_reply(entry, false, std::move("OK"));
}

void redis_parser::simple_error_reply(message_entry &entry, const std::string &message)
{
    simple_string_reply(entry, true, std::move(std::string("ERR ").append(message)));
}

void redis_parser::simple_string_reply(message_entry &entry, bool is_error, std::string message)
{
    reply_message(entry, redis_simple_string(is_error, std::move(message)));
}

void redis_parser::simple_integer_reply(message_entry &entry, int64_t value)
{
    reply_message(entry, redis_integer(value));
}

void redis_parser::default_handler(redis_parser::message_entry &entry)
{
    ::dsn::blob &cmd = entry.request.sub_requests[0].data;
    std::string message = "unknown command '" + std::string(cmd.data(), cmd.length()) + "'";
    LOG_INFO_PREFIX("{} with seqid {}", message, entry.sequence_id);
    simple_error_reply(entry, message);
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
    if (request.sub_requests.size() < 3) {
        LOG_INFO_PREFIX("SET command with invalid arguments, seqid({})", entry.sequence_id);
        simple_error_reply(entry, "wrong number of arguments for 'set' command");
    } else {
        int ttl_seconds = 0;
        parse_set_parameters(request.sub_requests, ttl_seconds);

        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        LOG_DEBUG_PREFIX("send SET command({})", entry.sequence_id);
        auto on_set_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            // when the "is_session_reset" flag is set, the socket may be broken.
            // so continue to reply the message is not necessary
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("SET command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            // the message_enry "entry" is stored in the queue "pending_response".
            // please ensure that "entry" hasn't been released right now.
            //
            // currently we only clear an entry when it is replied or
            // in the redis_parser's destructor
            LOG_DEBUG_PREFIX("SET command seqid({}) got reply", entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                LOG_INFO_PREFIX(
                    "SET command seqid({}) got reply with error = {}", entry.sequence_id, ec);
                simple_error_reply(entry, ec.to_string());
            } else {
                ::dsn::apps::update_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    simple_error_reply(entry,
                                       "internal error " + std::to_string(rrdb_response.error));
                } else {
                    simple_ok_reply(entry);
                }
            }
        };

        ::dsn::apps::update_request req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req.key, request.sub_requests[1].data, null_blob);
        req.value = request.sub_requests[2].data;
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
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 3) {
        simple_error_reply(entry, "wrong number of arguments for 'SET' command");
    } else {
        int ttl_seconds = 0;
        parse_set_parameters(redis_request.sub_requests, ttl_seconds);

        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto set_callback = [ref_this, this, &entry](int ec, pegasus_client::internal_info &&) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("SETEX command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            if (PERR_OK != ec) {
                simple_error_reply(entry, _geo_client->get_error_string(ec));
            } else {
                simple_ok_reply(entry);
            }
        };
        _geo_client->async_set(redis_request.sub_requests[1].data.to_string(), // key => hash_key
                               std::string(),                                  // ""  => sort_key
                               redis_request.sub_requests[2].data.to_string(), // value
                               set_callback,
                               2000, // TODO: set the timeout
                               ttl_seconds);
    }
}

void redis_parser::setex(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    // setex key ttl_SECONDS value
    if (redis_req.sub_requests.size() != 4) {
        LOG_INFO_PREFIX("SETEX command seqid({}) with invalid arguments", entry.sequence_id);
        simple_error_reply(entry, "wrong number of arguments for 'setex' command");
    } else {
        LOG_DEBUG_PREFIX("send SETEX command seqid({})", entry.sequence_id);
        int ttl_seconds;
        if (!dsn::buf2int32(redis_req.sub_requests[2].data.to_string_view(), ttl_seconds)) {
            simple_error_reply(entry, "value is not an integer or out of range");
            return;
        }
        if (ttl_seconds <= 0) {
            simple_error_reply(entry, "invalid expire time in setex");
            return;
        }

        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_setex_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("SETEX command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            if (::dsn::ERR_OK != ec) {
                LOG_INFO_PREFIX(
                    "SETEX command seqid({}) got reply with error = {}", entry.sequence_id, ec);
                simple_error_reply(entry, ec.to_string());
                return;
            }

            ::dsn::apps::update_response rrdb_response;
            ::dsn::unmarshall(response, rrdb_response);
            if (rrdb_response.error != 0) {
                simple_error_reply(entry, "internal error " + std::to_string(rrdb_response.error));
                return;
            }

            simple_ok_reply(entry);
        };

        ::dsn::apps::update_request req;
        ::dsn::blob null_blob;

        pegasus_generate_key(req.key, redis_req.sub_requests[1].data, null_blob);
        req.value = redis_req.sub_requests[3].data;
        req.expire_ts_seconds = pegasus::utils::epoch_now() + ttl_seconds;

        auto partition_hash = pegasus_key_hash(req.key);

        // TODO: set the timeout
        client->put(req, on_setex_reply, std::chrono::milliseconds(2000), 0, partition_hash);
    }
}

void redis_parser::get(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    if (redis_req.sub_requests.size() != 2) {
        LOG_INFO_PREFIX("GET command seqid({}) with invalid arguments", entry.sequence_id);
        simple_error_reply(entry, "wrong number of arguments for 'get' command");
    } else {
        LOG_DEBUG_PREFIX("send GET command seqid({})", entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_get_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("GET command({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            if (::dsn::ERR_OK != ec) {
                LOG_INFO_PREFIX(
                    "GET command seqid({}) got reply with error = {}", entry.sequence_id, ec);
                simple_error_reply(entry, ec.to_string());
            } else {
                ::dsn::apps::read_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    if (rrdb_response.error == rocksdb::Status::kNotFound) {
                        reply_message(entry, redis_bulk_string());
                    } else {
                        simple_error_reply(entry,
                                           "internal error " + std::to_string(rrdb_response.error));
                    }
                } else {
                    reply_message(entry, redis_bulk_string(rrdb_response.value));
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.sub_requests[1].data, null_blob);
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
    if (redis_req.sub_requests.size() != 2) {
        LOG_INFO_PREFIX("DEL command seqid({}) with invalid arguments", entry.sequence_id);
        simple_error_reply(entry, "wrong number of arguments for 'del' command");
    } else {
        LOG_DEBUG_PREFIX("send DEL command seqid({})", entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_del_reply = [ref_this, this, &entry](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("DEL command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            LOG_DEBUG_PREFIX("DEL command seqid({}) got reply", entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                LOG_INFO_PREFIX(
                    "DEL command seqid({}) got reply with error = {}", entry.sequence_id, ec);
                simple_error_reply(entry, ec.to_string());
            } else {
                ::dsn::apps::read_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    simple_error_reply(entry,
                                       "internal error " + std::to_string(rrdb_response.error));
                } else {
                    // NOTE: Deleting a non-existed key returns 1 too. But standard Redis returns 0.
                    // Pegasus behaves
                    // differently in this case intentionally for performance.
                    // Because if we need to check the existence, we should use check_and_mutate
                    // instead.
                    simple_integer_reply(entry, 1);
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.sub_requests[1].data, null_blob);
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
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() != 2) {
        simple_error_reply(entry, "wrong number of arguments for 'DEL' command");
    } else {
        // with a reference to prevent the object from being destroyed
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto del_callback = [ref_this, this, &entry](int ec, pegasus_client::internal_info &&) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("SETEX command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            if (PERR_OK != ec) {
                simple_error_reply(entry, _geo_client->get_error_string(ec));
            } else {
                simple_integer_reply(entry, 1);
            }
        };
        _geo_client->async_del(redis_request.sub_requests[1].data.to_string(), // key => hash_key
                               std::string(),                                  // ""  => sort_key
                               false,
                               del_callback,
                               2000); // TODO: set the timeout
    }
}

// process 'ttl' and 'pttl'
void redis_parser::ttl(message_entry &entry)
{
    redis_request &redis_req = entry.request;
    bool is_ttl = (toupper(redis_req.sub_requests[0].data.data()[0]) == 'T');
    if (redis_req.sub_requests.size() != 2) {
        LOG_INFO_PREFIX("TTL/PTTL command seqid({}) with invalid arguments", entry.sequence_id);
        simple_error_reply(
            entry, fmt::format("wrong number of arguments for '{}'", is_ttl ? "ttl" : "pttl"));
    } else {
        LOG_DEBUG_PREFIX("send PTTL/TTL command seqid({})", entry.sequence_id);
        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto on_ttl_reply = [ref_this, this, &entry, is_ttl](
            ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("TTL/PTTL command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            LOG_DEBUG_PREFIX("TTL/PTTL command seqid({}) got reply", entry.sequence_id);
            if (::dsn::ERR_OK != ec) {
                LOG_INFO_PREFIX(
                    "DEL command seqid({}) got reply with error = {}", entry.sequence_id, ec);
                simple_error_reply(entry, ec.to_string());
            } else {
                ::dsn::apps::ttl_response rrdb_response;
                ::dsn::unmarshall(response, rrdb_response);
                if (rrdb_response.error != 0) {
                    if (rrdb_response.error == rocksdb::Status::kNotFound) {
                        simple_integer_reply(entry, -2);
                    } else {
                        simple_error_reply(entry,
                                           "internal error " + std::to_string(rrdb_response.error));
                    }
                } else {
                    simple_integer_reply(entry,
                                         is_ttl ? rrdb_response.ttl_seconds
                                                : rrdb_response.ttl_seconds * 1000);
                }
            }
        };
        ::dsn::blob req;
        ::dsn::blob null_blob;
        pegasus_generate_key(req, redis_req.sub_requests[1].data, null_blob);
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
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 5) {
        simple_error_reply(entry, "wrong number of arguments for 'GEORADIUS' command");
        return;
    }

    // longitude latitude
    double lng_degrees = 0.0;
    const std::string &str_lng_degrees = redis_request.sub_requests[2].data.to_string();
    LOG_WARNING_IF(!dsn::buf2double(str_lng_degrees, lng_degrees),
                   "longitude parameter '{}' is error, use {}",
                   str_lng_degrees,
                   lng_degrees);
    double lat_degrees = 0.0;
    const std::string &str_lat_degrees = redis_request.sub_requests[3].data.to_string();
    LOG_WARNING_IF(!dsn::buf2double(str_lat_degrees, lat_degrees),
                   "latitude parameter '{}' is error, use {}",
                   str_lat_degrees,
                   lat_degrees);

    // radius m|km|ft|mi [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
    double radius_m = 100.0;
    std::string unit;
    geo::geo_client::SortType sort_type = geo::geo_client::SortType::random;
    int count = -1;
    bool WITHCOORD = false;
    bool WITHDIST = false;
    bool WITHHASH = false;
    parse_geo_radius_parameters(redis_request.sub_requests,
                                4,
                                radius_m,
                                unit,
                                sort_type,
                                count,
                                WITHCOORD,
                                WITHDIST,
                                WITHHASH);

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
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 4) {
        simple_error_reply(entry, "wrong number of arguments for 'GEORADIUSBYMEMBER' command");
        return;
    }

    // member
    std::string hash_key = redis_request.sub_requests[2].data.to_string(); // member => hash_key

    // radius m|km|ft|mi [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC] [WITHHASH]
    double radius_m = 100.0;
    std::string unit;
    geo::geo_client::SortType sort_type = geo::geo_client::SortType::random;
    int count = -1;
    bool WITHCOORD = false;
    bool WITHDIST = false;
    bool WITHHASH = false;
    parse_geo_radius_parameters(redis_request.sub_requests,
                                3,
                                radius_m,
                                unit,
                                sort_type,
                                count,
                                WITHCOORD,
                                WITHDIST,
                                WITHHASH);

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
    CHECK(!entry.request.sub_requests.empty(), "");
    CHECK_GT(entry.request.sub_requests[0].length, 0);
    const char *command = entry.request.sub_requests[0].data.data();
    int64_t increment = 1;
    if (dsn::utils::iequals(command, "INCR") || dsn::utils::iequals(command, "DECR")) {
        if (entry.request.sub_requests.size() != 2) {
            LOG_WARNING("{}: command {} seqid({}) with invalid arguments count: {}",
                        _remote_address,
                        command,
                        entry.sequence_id,
                        entry.request.sub_requests.size());
            simple_error_reply(entry, fmt::format("wrong number of arguments for '{}'", command));
            return;
        }
    } else if (dsn::utils::iequals(command, "INCRBY") || dsn::utils::iequals(command, "DECRBY")) {
        if (entry.request.sub_requests.size() != 3) {
            LOG_WARNING("{}: command {} seqid({}) with invalid arguments count: {}",
                        _remote_address,
                        command,
                        entry.sequence_id,
                        entry.request.sub_requests.size());
            simple_error_reply(entry, fmt::format("wrong number of arguments for '{}'", command));
            return;
        }
        if (!dsn::buf2int64(entry.request.sub_requests[2].data.to_string_view(), increment)) {
            LOG_WARNING("{}: command {} seqid({}) with invalid 'increment': {}",
                        _remote_address,
                        command,
                        entry.sequence_id,
                        entry.request.sub_requests[2].data.to_string());
            simple_error_reply(entry,
                               fmt::format("wrong type of argument 'increment 'for '{}'", command));
            return;
        }
    } else {
        LOG_FATAL("command not support: {}", command);
    }
    if (dsn::utils::iequals(command, "DECR", 4)) {
        increment = -increment;
    }

    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    auto on_incr_reply = [ref_this, this, command, &entry](
        ::dsn::error_code ec, dsn::message_ex *, dsn::message_ex *response) {
        if (_is_session_reset.load(std::memory_order_acquire)) {
            LOG_WARNING("{}: command {} seqid({}) got reply, but session has reset",
                        _remote_address,
                        command,
                        entry.sequence_id);
            return;
        }

        if (::dsn::ERR_OK != ec) {
            LOG_WARNING("{}: command {} seqid({}) got reply with error = {}",
                        _remote_address,
                        command,
                        entry.sequence_id,
                        ec);
            simple_error_reply(entry, ec.to_string());
        } else {
            ::dsn::apps::incr_response incr_resp;
            ::dsn::unmarshall(response, incr_resp);
            if (incr_resp.error != 0) {
                simple_error_reply(entry, "internal error " + std::to_string(incr_resp.error));
            } else {
                simple_integer_reply(entry, incr_resp.new_value);
            }
        }
    };
    dsn::apps::incr_request req;
    pegasus_generate_key(req.key, entry.request.sub_requests[1].data, dsn::blob());
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
        if (dsn::utils::iequals(opt, "EX") && i + 1 < opts.size()) {
            const std::string &str_ttl_seconds = opts[i + 1].data.to_string();
            if (!dsn::buf2int32(str_ttl_seconds, ttl_seconds)) {
                LOG_WARNING("'EX {}' option is error, use {}", str_ttl_seconds, ttl_seconds);
            }
        } else {
            LOG_WARNING("only 'EX' option is supported");
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
        LOG_WARNING("radius parameter '{}' is error, use {}", str_radius, radius_m);
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
        if (dsn::utils::iequals(opt, "WITHCOORD")) {
            WITHCOORD = true;
        } else if (dsn::utils::iequals(opt, "WITHDIST")) {
            WITHDIST = true;
        } else if (dsn::utils::iequals(opt, "WITHHASH")) {
            WITHHASH = true;
        } else if (dsn::utils::iequals(opt, "COUNT") && base_index + 1 < opts.size()) {
            const std::string &str_count = opts[base_index + 1].data.to_string();
            LOG_ERROR_IF(!dsn::buf2int32(str_count, count),
                         "'COUNT {}' option is error, use {}",
                         str_count,
                         count);
        } else if (dsn::utils::iequals(opt, "ASC")) {
            sort_type = geo::geo_client::SortType::asc;
        } else if (dsn::utils::iequals(opt, "DESC")) {
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
    if (_is_session_reset.load(std::memory_order_acquire)) {
        LOG_INFO_PREFIX("SETEX command seqid({}) got reply, but session has reset",
                        entry.sequence_id);
        return;
    }

    if (PERR_OK != ec) {
        simple_error_reply(entry, _geo_client->get_error_string(ec));
    } else {
        redis_array result;
        result.resize(results.size());
        size_t i = 0;
        for (const auto &elem : results) {
            std::shared_ptr<redis_base_type> key =
                std::make_shared<redis_bulk_string>(elem.hash_key); // hash_key => member
            if (!WITHCOORD && !WITHDIST && !WITHHASH) {
                // only member
                result.array[i++] = key;
            } else {
                // member and some WITH* parameters
                std::shared_ptr<redis_array> sub_array = std::make_shared<redis_array>();

                // member
                sub_array->resize(1 + (WITHDIST ? 1 : 0) + (WITHCOORD ? 1 : 0) +
                                  (WITHHASH ? 1 : 0));
                size_t index = 0;
                sub_array->array[index++] = key;

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
                    sub_array->array[index++] = construct_bulk_string(distance);
                }
                if (WITHCOORD) {
                    // with coordinate
                    std::shared_ptr<redis_array> coordinate = std::make_shared<redis_array>();
                    coordinate->resize(2);
                    coordinate->array[0] = construct_bulk_string(elem.lng_degrees);
                    coordinate->array[1] = construct_bulk_string(elem.lat_degrees);

                    sub_array->array[index++] = coordinate;
                }
                if (WITHHASH) {
                    // with origin value
                    sub_array->array[index++] = std::make_shared<redis_bulk_string>(elem.value);
                }
                result.array[i++] = sub_array;
            }
        }
        reply_message(entry, result);
    }
}

// command format:
// GEOADD key longitude latitude member [longitude latitude member ...]
void redis_parser::geo_add(message_entry &entry)
{
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }

    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 5 || (redis_request.sub_requests.size() - 2) % 3 != 0) {
        simple_error_reply(entry, "wrong number of arguments for 'geoadd' command");
        return;
    }

    int member_count = (redis_request.sub_requests.size() - 2) / 3;
    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    std::shared_ptr<std::atomic<int32_t>> set_count =
        std::make_shared<std::atomic<int32_t>>(member_count);
    std::shared_ptr<redis_integer> result(new redis_integer());
    auto set_latlng_callback = [ref_this, this, &entry, result, set_count](
        int error_code, pegasus_client::internal_info &&info) {
        if (_is_session_reset.load(std::memory_order_acquire)) {
            LOG_INFO_PREFIX("GEOADD command seqid({}) got reply, but session has reset",
                            entry.sequence_id);
            return;
        }

        if (PERR_OK != error_code) {
            if (set_count->fetch_sub(1) == 1) {
                reply_message(entry, *result);
            }
            return;
        }

        ++(result->value);
        if (set_count->fetch_sub(1) == 1) {
            reply_message(entry, *result);
        }
    };

    for (int i = 0; i < member_count; ++i) {
        double lng_degree;
        double lat_degree;
        if (dsn::buf2double(redis_request.sub_requests[2 + i * 3].data.to_string_view(),
                            lng_degree) &&
            dsn::buf2double(redis_request.sub_requests[2 + i * 3 + 1].data.to_string_view(),
                            lat_degree)) {
            const std::string &hashkey = redis_request.sub_requests[2 + i * 3 + 2].data.to_string();
            _geo_client->async_set(hashkey, "", lat_degree, lng_degree, set_latlng_callback, 2000);
        } else if (set_count->fetch_sub(1) == 1) {
            reply_message(entry, *result);
        }
    }
}

// command format:
// GEODIST key member1 member2 [unit]
void redis_parser::geo_dist(message_entry &entry)
{
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 4) {
        simple_error_reply(entry, "wrong number of arguments for 'geodist' command");
    } else {
        // TODO: set the timeout
        std::string hash_key1 =
            redis_request.sub_requests[2].data.to_string(); // member1 => hash_key1
        std::string hash_key2 =
            redis_request.sub_requests[3].data.to_string(); // member2 => hash_key2
        std::string unit = redis_request.sub_requests[4].data.to_string();

        std::shared_ptr<proxy_session> ref_this = shared_from_this();
        auto get_callback = [ref_this, this, &entry, unit](int error_code, double &&distance) {
            if (_is_session_reset.load(std::memory_order_acquire)) {
                LOG_INFO_PREFIX("GEODIST command seqid({}) got reply, but session has reset",
                                entry.sequence_id);
                return;
            }

            if (PERR_OK != error_code) {
                simple_error_reply(entry, _geo_client->get_error_string(error_code));
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

                reply_message(entry, redis_bulk_string(std::to_string(distance)));
            }
        };
        _geo_client->async_distance(hash_key1, "", hash_key2, "", 2000, get_callback);
    }
}

// command format:
// GEOPOS key member [member ...]
void redis_parser::geo_pos(message_entry &entry)
{
    if (!_geo_client) {
        return simple_error_reply(entry, "redis proxy is not on GEO mode");
    }
    redis_request &redis_request = entry.request;
    if (redis_request.sub_requests.size() < 3) {
        simple_error_reply(entry, "wrong number of arguments for 'geopos' command");
        return;
    }

    int member_count = redis_request.sub_requests.size() - 2;
    std::shared_ptr<proxy_session> ref_this = shared_from_this();
    std::shared_ptr<std::atomic<int32_t>> get_count =
        std::make_shared<std::atomic<int32_t>>(member_count);
    std::shared_ptr<redis_array> result(new redis_array());
    result->resize(member_count);
    auto get_latlng_callback = [ref_this, this, &entry, result, get_count](
        int error_code, int index, double lat_degrees, double lng_degrees) {
        if (_is_session_reset.load(std::memory_order_acquire)) {
            LOG_INFO_PREFIX("GEOPOS command seqid({}) got reply, but session has reset",
                            entry.sequence_id);
            return;
        }

        if (PERR_OK != error_code) {
            // null bulk string for this member
            result->array[index] = std::make_shared<redis_bulk_string>();
            if (get_count->fetch_sub(1) == 1) {
                reply_message(entry, *result);
            }
            return;
        }

        std::shared_ptr<redis_array> coordinate(new redis_array());
        coordinate->resize(2);
        coordinate->array[0] = construct_bulk_string(lng_degrees);
        coordinate->array[1] = construct_bulk_string(lat_degrees);

        result->array[index] = coordinate;
        if (get_count->fetch_sub(1) == 1) {
            reply_message(entry, *result);
        }
    };

    for (int i = 0; i < member_count; ++i) {
        _geo_client->async_get(
            redis_request.sub_requests[i + 2].data.to_string(), "", i, get_latlng_callback, 2000);
    }
}

void redis_parser::handle_command(std::unique_ptr<message_entry> &&entry)
{
    message_entry &e = *entry.get();
    redis_request &request = e.request;
    e.sequence_id = ++s_next_seqid;
    e.response.store(nullptr, std::memory_order_relaxed);

    LOG_DEBUG_PREFIX("new command parsed with new seqid {}", e.sequence_id);
    enqueue_pending_response(std::move(entry));

    CHECK_GT_MSG(request.sub_request_count, 0, "invalid request");
    ::dsn::blob &command = request.sub_requests[0].data;
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
    CHECK((-1 == length && data.length() == 0) || data.length() == length,
          "{} VS {}",
          data.length(),
          length);
    write_stream.write_pod('$');
    std::string length_str = std::to_string(length);
    write_stream.write(length_str.c_str(), (int)length_str.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
    if (length >= 0) {
        write_stream.write(data.data(), length);
        write_stream.write_pod(CR);
        write_stream.write_pod(LF);
    }
}

void redis_parser::redis_array::marshalling(::dsn::binary_writer &write_stream) const
{
    CHECK((-1 == count && array.size() == 0) || array.size() == count,
          "{} VS {}",
          array.size(),
          count);
    write_stream.write_pod('*');
    std::string count_str = std::to_string(count);
    write_stream.write(count_str.c_str(), (int)count_str.length());
    write_stream.write_pod(CR);
    write_stream.write_pod(LF);
    for (const auto &elem : array) {
        elem->marshalling(write_stream);
    }
}
} // namespace proxy
} // namespace pegasus
