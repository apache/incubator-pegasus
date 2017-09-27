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
        removed
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
        dsn_message_t response;
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
    std::deque<std::unique_ptr<message_entry>> pending_response;
    int64_t next_seqid;

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

    // for rrdb
    std::unique_ptr<::dsn::apps::rrdb_client> client;

private:
    // function for data stream
    void append_message(dsn_message_t msg);
    void prepare_current_buffer();
    char peek();
    void eat(char c);
    void eat_all(char *dest, size_t length);
    void reset();

    // function for parser
    void end_array_size();
    void end_bulk_string_size();
    void append_current_bulk_string();
    void parse_stream();

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
    template <typename T>
    void reply_message(message_entry &entry, const T &value)
    {
        dsn_message_t resp = create_response();
        ::dsn::rpc_write_stream s(resp);

        marshalling(s, value);
        s.commit_buffer();
        entry.response = resp;
        // released when dequeue fro the pending_response queue
        dsn_msg_add_ref(entry.response);

        reply_all_ready();
    }
    void reply_all_ready();

    typedef void (*redis_call_handler)(redis_parser *, message_entry &);
    static std::unordered_map<std::string, redis_call_handler> s_dispatcher;
    static redis_call_handler get_handler(const char *command, unsigned int length);

public:
    redis_parser(proxy_stub *op, ::dsn::rpc_address remote);
    virtual ~redis_parser();
    virtual void on_remove_session(std::shared_ptr<proxy_session> _this) override;
};
}
} // namespace
