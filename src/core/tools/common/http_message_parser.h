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

# include <dsn/internal/ports.h>
# include <dsn/internal/rpc_message.h>
# include <dsn/internal/singleton.h>
# include <dsn/internal/message_parser.h>
# include <vector>
# include <queue>
# include "http_parser.h"

namespace dsn
{
class http_message_parser : public message_parser
{
public:
    http_message_parser(int buffer_block_size, bool is_write_only);
    message_ex* get_message_on_receive(int read_length, /*out*/ int& read_next) override;

    int prepare_buffers_on_send(message_ex* msg, int offset, /*out*/ send_buf* buffers) override;

    int get_send_buffers_count_and_total_length(message_ex* msg, /*out*/ int* total_length) override;
private:
    http_parser _parser;
    http_parser_settings _parser_setting;
    std::queue<std::unique_ptr<message_ex>> _received_messages;
    struct
    {
        std::string rpc_name;
        int hash;
    } _last_url_info;

# pragma warning(push)
# pragma warning(disable: 4579)
    constexpr static const char header_prefix[] =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html; charset=UTF-8\r\n"
        "Access-Control-Allow-Origin: *\r\n";
    constexpr static const char header_contentlen_prefix[] = "Content-Length: ";
    constexpr static size_t contentlen_placeholder_length = 9;
    constexpr static const char header_contentlen_suffix[] = "\r\n\r\n";
    constexpr static size_t header_size = sizeof(header_prefix) + sizeof(header_contentlen_prefix) + contentlen_placeholder_length + sizeof(header_contentlen_suffix);
    constexpr static const char suffix_padding[] = "\r\n";
# pragma warning(pop)
    char http_header_send_buffer[header_size];
};
}