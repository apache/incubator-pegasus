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

# include <dsn/internal/ports.h>
# include <dsn/internal/rpc_message.h>
# include <dsn/internal/singleton.h>
# include <vector>
# include <iomanip>
#include "http_message_parser.h"
#include <dsn/cpp/serialization.h>

namespace dsn{
http_message_parser::http_message_parser(int buffer_block_size, bool is_write_only)
    : message_parser(buffer_block_size, is_write_only)
{
    memset(&_parser_setting, 0, sizeof(_parser_setting));
    _parser.data = this;
    _parser_setting.on_url = [](http_parser* parser, const char *at, size_t length)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        http_parser_url url;
        http_parser_parse_url(at, length, 1, &url);
        if (((url.field_set >> UF_PATH) & 1) == 0)
        {
            derror("url has no path field");
            //error, reset parser state
            return 1;
        }
        std::vector<std::string> args;
        utils::split_args(std::string(at, length).c_str(), args, '/');
        if (args.size() != 2)
        {
            derror("invalid url");
            //error, reset parser state
            return 1;
        }
        owner->_last_url_info.rpc_name = std::move(args[0]);
        owner->_last_url_info.hash = std::stoi(args[1]);
        return 0;
    };
    _parser_setting.on_body = [](http_parser* parser, const char *at, size_t length)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        dassert(owner->_read_buffer.buffer() != nullptr, "the read buffer is not owning");
        owner->_received_messages.emplace(
            message_ex::create_receive_message_with_standalone_header(
                blob(owner->_read_buffer.buffer(), at - owner->_read_buffer.buffer_ptr(), length)));
        strcpy(owner->_received_messages.back()->header->rpc_name, owner->_last_url_info.rpc_name.c_str());
        owner->_received_messages.back()->header->client.hash = owner->_last_url_info.hash;
        owner->_received_messages.back()->header->context.u.is_request = 1;
        return 0;
    };
    http_parser_init(&_parser, HTTP_REQUEST);
    
}

message_ex* http_message_parser::get_message_on_receive(int read_length, /*out*/ int& read_next)
{
    read_next = 4096;
    auto nparsed = http_parser_execute(&_parser, &_parser_setting, _read_buffer.data() + _read_buffer_occupied, read_length);
    if (_parser.upgrade)
    {
        derror("unsupported protocol");
        return nullptr;
    }
    if (nparsed != read_length)
    {
        derror("malformed http packet, we cannot handle it now");
        return nullptr;
    }
    mark_read(read_length);
    if (!_received_messages.empty())
    {
        auto message = std::move(_received_messages.front());
        _received_messages.pop();
        return message.release();
    }
    else
    {
        return nullptr;
    }
}

int http_message_parser::prepare_buffers_on_send(message_ex* msg, int offset, send_buf* buffers)
{
    //skip message header for browser
    if (offset == 0)
    {
        sprintf(http_header_send_buffer, "%s%s%09lld%s", header_prefix, header_contentlen_prefix, msg->body_size(), header_contentlen_suffix);
    }
    int buffer_iter = 0;
    if (offset < header_size)
    {
        buffers[0].buf = http_header_send_buffer + offset;
        buffers[0].sz = header_size - offset;
        buffer_iter = 1;
        offset = 0;
    }
    else
    {
        offset -= header_size;
    }
    offset += sizeof(message_header);
    for (auto& buf : msg->buffers)
    {
        if (offset >= buf.length())
        {
            offset -= buf.length();
            continue;
        }
        buffers[buffer_iter].buf = const_cast<char*>(buf.data() + offset);
        buffers[buffer_iter].sz = buf.length() - offset;
        offset = 0;
        buffer_iter += 1;
    }

    buffers[buffer_iter].buf = const_cast<char*>(suffix_padding);
    buffers[buffer_iter].sz = sizeof(suffix_padding);
    buffer_iter += 1;
    return buffer_iter;
}

int http_message_parser::get_send_buffers_count_and_total_length(message_ex* msg, int* total_length)
{
    //message + http header + suffix_padding
    *total_length = msg->body_size() + header_size + sizeof(suffix_padding);
    return msg->buffers.size() + 2;
}

constexpr const char http_message_parser::header_prefix[];
constexpr const char http_message_parser::header_contentlen_prefix[];
constexpr const char http_message_parser::header_contentlen_suffix[];
constexpr const char http_message_parser::suffix_padding[];
}
