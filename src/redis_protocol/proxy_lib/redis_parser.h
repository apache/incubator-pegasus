// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <queue>
#include <deque>
#include <list>
#include "proxy_layer.h"
#include "geo/lib/geo_client.h"

#define CR '\015'
#define LF '\012'

namespace dsn {
namespace apps {
class rrdb_client;
}
}

namespace pegasus {
namespace proxy {

// http://redis.io/topics/protocol
class redis_parser : public proxy_session
{
protected:
    struct redis_base_type
    {
        virtual ~redis_base_type() = default;
        virtual void marshalling(::dsn::binary_writer &write_stream) const = 0;
    };
    struct redis_integer : public redis_base_type
    {
        int64_t value = 0;

        explicit redis_integer(int64_t v = 0) : value(v) {}

        void marshalling(::dsn::binary_writer &write_stream) const final;
    };
    // represent both redis simple string and error
    struct redis_simple_string : public redis_base_type
    {
        bool is_error = false;
        std::string message;

        redis_simple_string(bool err, std::string &&msg) : is_error(err), message(std::move(msg)) {}

        void marshalling(::dsn::binary_writer &write_stream) const final;
    };
    struct redis_bulk_string : public redis_base_type
    {
        int length = -1; // max length is 512 MB
        ::dsn::blob data;

        redis_bulk_string() = default;
        redis_bulk_string(std::string str)
        {
            data = ::dsn::blob::create_from_bytes(std::move(str));
            length = data.length();
        }
        explicit redis_bulk_string(const ::dsn::blob &bb) : length(bb.length()), data(bb) {}

        void marshalling(::dsn::binary_writer &write_stream) const final;
    };
    struct redis_array : public redis_base_type
    {
        int count = -1;
        std::vector<std::shared_ptr<redis_base_type>> array;

        void resize(size_t size)
        {
            count = size;
            array.resize(size);
        }

        void marshalling(::dsn::binary_writer &write_stream) const final;
    };

    struct redis_request
    {
        int sub_request_count = 0;
        std::vector<redis_bulk_string> sub_requests;

        redis_request(int count = 0, std::vector<redis_bulk_string> requests = {})
            : sub_request_count(count), sub_requests(std::move(requests))
        {
        }
    };
    struct message_entry
    {
        redis_request request;
        std::atomic<dsn::message_ex *> response;
        int64_t sequence_id = 0;
    };

    bool parse(dsn::message_ex *msg) override;

    // this is virtual only because we can override and test other modules
    virtual void handle_command(std::unique_ptr<message_entry> &&entry);

private:
    // queue for pipeline the response
    dsn::zlock response_lock;
    std::deque<std::unique_ptr<message_entry>> pending_response;

    enum parser_status
    {
        kStartArray,
        kInArraySize,
        kStartBulkString,
        kInBulkStringSize,
        kStartBulkStringData,
    };
    // recieving message and parsing status
    // [
    // content for current parser
    redis_bulk_string _current_str;
    std::unique_ptr<message_entry> _current_msg;
    parser_status _status;
    std::string _current_size;

    // data stream content
    std::queue<dsn::message_ex *> _recv_buffers;
    size_t _total_length;
    char *_current_buffer;
    size_t _current_buffer_length;
    size_t _current_cursor;
    // ]

    // for rrdb
    std::unique_ptr<::dsn::apps::rrdb_client> client;
    std::unique_ptr<geo::geo_client> _geo_client;

protected:
    // function for data stream
    void append_message(dsn::message_ex *msg);
    void prepare_current_buffer();
    char peek();
    bool eat(char c);
    void eat_all(char *dest, size_t length);
    void reset_parser();

    // function for parser
    bool end_array_size();
    bool end_bulk_string_size();
    void append_current_bulk_string();
    bool parse_stream();

// function for rrdb operation
#define DECLARE_REDIS_HANDLER(function_name)                                                       \
    void function_name(message_entry &req);                                                        \
    static void g_##function_name(redis_parser *_this, message_entry &req)                         \
    {                                                                                              \
        _this->function_name(req);                                                                 \
    }

    DECLARE_REDIS_HANDLER(set)
    DECLARE_REDIS_HANDLER(get)
    DECLARE_REDIS_HANDLER(del)
    DECLARE_REDIS_HANDLER(setex)
    DECLARE_REDIS_HANDLER(ttl)
    DECLARE_REDIS_HANDLER(geo_dist)
    DECLARE_REDIS_HANDLER(geo_pos)
    DECLARE_REDIS_HANDLER(geo_radius)
    DECLARE_REDIS_HANDLER(geo_radius_by_member)
    DECLARE_REDIS_HANDLER(incr)
    DECLARE_REDIS_HANDLER(incr_by)
    DECLARE_REDIS_HANDLER(decr)
    DECLARE_REDIS_HANDLER(decr_by)
    DECLARE_REDIS_HANDLER(default_handler)

    void set_internal(message_entry &entry);
    void set_geo_internal(message_entry &entry);
    void del_internal(message_entry &entry);
    void del_geo_internal(message_entry &entry);
    void counter_internal(message_entry &entry);
    static void parse_set_parameters(const std::vector<redis_bulk_string> &opts, int &ttl_seconds);
    static void parse_geo_radius_parameters(const std::vector<redis_bulk_string> &opts,
                                            int base_index,
                                            double &radius_m,
                                            std::string &unit,
                                            geo::geo_client::SortType &sort_type,
                                            int &count,
                                            bool &WITHCOORD,
                                            bool &WITHDIST,
                                            bool &WITHHASH);
    void process_geo_radius_result(message_entry &entry,
                                   const std::string &unit,
                                   bool WITHCOORD,
                                   bool WITHDIST,
                                   bool WITHHASH,
                                   int ec,
                                   std::list<geo::SearchResult> &&results);

    // function for pipeline reply
    void enqueue_pending_response(std::unique_ptr<message_entry> &&entry);
    void fetch_and_dequeue_messages(std::vector<dsn::message_ex *> &msgs, bool only_ready_ones);
    void clear_reply_queue();
    void reply_all_ready();

    template <typename T>
    void reply_message(message_entry &entry, const T &value)
    {
        dsn::message_ex *resp = create_response();
        // release in reply_all_ready or reset
        resp->add_ref();

        dsn::rpc_write_stream s(resp);
        value.marshalling(s);
        s.commit_buffer();

        entry.response.store(resp, std::memory_order_release);
        reply_all_ready();
    }

    std::shared_ptr<redis_bulk_string> construct_bulk_string(double data);
    void simple_ok_reply(message_entry &entry);
    void simple_error_reply(message_entry &entry, const std::string &message);
    void simple_string_reply(message_entry &entry, bool is_error, std::string message);
    void simple_integer_reply(message_entry &entry, int64_t value);

    typedef void (*redis_call_handler)(redis_parser *, message_entry &);
    static std::unordered_map<std::string, redis_call_handler> s_dispatcher;
    static redis_call_handler get_handler(const char *command, unsigned int length);
    static std::atomic_llong s_next_seqid;

public:
    redis_parser(proxy_stub *op, dsn::message_ex *first_msg);
    ~redis_parser() override;
};
}
} // namespace
