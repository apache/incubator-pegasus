// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <queue>
#include <deque>
#include "proxy_layer.h"

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
    enum parser_status
    {
        start_array,
        in_array_size,
        start_bulk_string,
        in_bulk_string_size,
        start_bulk_string_data,
    };
    struct redis_bulk_string
    {
        int length;
        ::dsn::blob data;
        redis_bulk_string() : length(0) {}
        redis_bulk_string(int len, const char *str) : length(len), data(str, 0, len) {}
        redis_bulk_string(const ::dsn::blob &bb) : length(bb.length()), data(bb) {}
    };
    struct redis_simple_string
    {
        bool is_error;
        std::string message;
    };
    struct redis_integer
    {
        int64_t value;
    };
    struct redis_request
    {
        int length;
        std::vector<redis_bulk_string> buffers;
        redis_request() : length(0), buffers() {}
    };
    struct message_entry
    {
        redis_request request;
        std::atomic<dsn_message_t> response;
        int64_t sequence_id;
    };

    static void marshalling(::dsn::binary_writer &write_stream, const redis_simple_string &data);
    static void marshalling(::dsn::binary_writer &write_stream, const redis_bulk_string &data);
    static void marshalling(::dsn::binary_writer &write_stream, const redis_integer &data);

    virtual bool parse(dsn_message_t msg) override;

    // this is virtual only because we can override and test other modules
    virtual void handle_command(std::unique_ptr<message_entry> &&entry);

private:
    // queue for pipeline the response
    dsn::service::zlock response_lock;
    std::deque<std::unique_ptr<message_entry>> pending_response;

    // recieving message and parsing status
    // [
    // content for current parser
    redis_bulk_string current_str;
    std::unique_ptr<message_entry> current_msg;
    parser_status status;
    std::string current_size;

    // data stream content
    std::queue<dsn_message_t> recv_buffers;
    size_t total_length;
    char *current_buffer;
    size_t current_buffer_length;
    size_t current_cursor;
    // ]

    // for rrdb
    std::unique_ptr<::dsn::apps::rrdb_client> client;

private:
    // function for data stream
    void append_message(dsn_message_t msg);
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
    DECLARE_REDIS_HANDLER(default_handler)

    // function for pipeline reply
    void enqueue_pending_response(std::unique_ptr<message_entry> &&entry);
    void fetch_and_dequeue_messages(std::vector<dsn_message_t> &msgs, bool only_ready_ones);
    void clear_reply_queue();
    void reply_all_ready();

    template <typename T>
    void reply_message(message_entry &entry, const T &value)
    {
        dsn_message_t resp = create_response();
        // release in reply_all_ready or reset
        dsn_msg_add_ref(resp);

        dsn::rpc_write_stream s(resp);
        marshalling(s, value);
        s.commit_buffer();

        entry.response.store(resp, std::memory_order_release);
        reply_all_ready();
    }

    typedef void (*redis_call_handler)(redis_parser *, message_entry &);
    static std::unordered_map<std::string, redis_call_handler> s_dispatcher;
    static redis_call_handler get_handler(const char *command, unsigned int length);
    static std::atomic_llong s_next_seqid;

public:
    redis_parser(proxy_stub *op, dsn_message_t first_msg);
    virtual ~redis_parser();
};
}
} // namespace
