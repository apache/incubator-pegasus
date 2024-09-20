/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <nodejs/http_parser.h>
#include <stddef.h>
#include <memory>
#include <queue>
#include <string>

#include "rpc/message_parser.h"
#include "rpc/rpc_message.h"
#include "task/task_spec.h"
#include "utils/customizable_id.h"

namespace dsn {

DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_HTTP)

// Number of blobs that a message_ex contains.
#define HTTP_MSG_BUFFERS_NUM 4

// Incoming HTTP requests will be parsed into:
//
//    msg->header->rpc_name = "RPC_HTTP_SERVICE"
//    msg->header->body_length = http body length
//    msg->header->hdr_type = "POST" / "GET ";
//    msg->hdr_format = NET_HDR_HTTP
//    msg->buffers[0] = header
//    msg->buffers[1] = body
//    msg->buffers[2] = url
//    msg->buffers[3] = content-type
//

enum http_parser_stage
{
    HTTP_INVALID,
    HTTP_ON_MESSAGE_BEGIN,
    HTTP_ON_URL,
    HTTP_ON_STATUS,
    HTTP_ON_HEADER_FIELD,
    HTTP_ON_HEADER_VALUE,
    HTTP_ON_HEADERS_COMPLETE,
    HTTP_ON_BODY,
    HTTP_ON_MESSAGE_COMPLETE
};

extern const char *http_parser_stage_to_string(http_parser_stage s);

class http_message_parser : public message_parser
{
public:
    http_message_parser();

    ~http_message_parser() override = default;

    message_ex *get_message_on_receive(message_reader *reader,
                                       /*out*/ int &read_next) override;

    void prepare_on_send(message_ex *msg) override;

    int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) override;

private:
    friend class http_message_parser_test;

    bool is_complete() const { return _stage == HTTP_ON_MESSAGE_COMPLETE; }

    void reset() override;

private:
    // see https://github.com/joyent/http-parser
    http_parser_settings _parser_setting;
    http_parser _parser;

    bool _is_field_content_type{false};
    std::unique_ptr<message_ex> _current_message;
    http_parser_stage _stage{HTTP_INVALID};
    std::string _url;
    size_t _parsed_length{0};
    std::queue<std::unique_ptr<message_ex>> _received_messages;
};

} // namespace dsn
