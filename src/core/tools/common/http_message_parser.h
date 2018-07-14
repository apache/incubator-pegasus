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

/*
* Description:
*     message parser for browser-generated http request
*
* Revision history:
*     Feb. 2016, Tianyi Wang, first version
*     xxxx-xx-xx, author, fix bug about xxx
*/

#pragma once

#include <dsn/utility/ports.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/utility/singleton.h>
#include <dsn/tool-api/message_parser.h>
#include <vector>
#include <queue>
#include "http_parser.h"

namespace dsn {
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_HTTP)

class http_message_parser : public message_parser
{
public:
    http_message_parser();
    virtual ~http_message_parser() {}

    virtual void reset() override;

    virtual message_ex *get_message_on_receive(message_reader *reader,
                                               /*out*/ int &read_next) override;

    virtual void prepare_on_send(message_ex *msg) override;

    virtual int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) override;

private:
    http_parser_settings _parser_setting;
    http_parser _parser;
    dsn::blob _current_buffer;
    std::unique_ptr<message_ex> _current_message;
    std::queue<std::unique_ptr<message_ex>> _received_messages;

    enum
    {
        parsing_nothing,
        parsing_id,
        parsing_trace_id,
        parsing_rpc_name,
        parsing_app_id,
        parsing_partition_index,
        parsing_serialize_format,
        parsing_from_address,
        parsing_client_timeout,
        parsing_client_thread_hash,
        parsing_client_partition_hash,
        parsing_server_error,
    } _response_parse_state;
};
}
