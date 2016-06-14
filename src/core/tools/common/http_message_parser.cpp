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
# include "http_message_parser.h"
# include <dsn/cpp/serialization.h>
# include <boost/xpressive/xpressive_static.hpp>
# include <boost/lexical_cast.hpp>



namespace dsn{
template <typename T, size_t N>
char(&ArraySizeHelper(T(&array)[N]))[N];
http_message_parser::http_message_parser(unsigned int buffer_block_size, bool is_write_only)
    : message_parser(buffer_block_size, is_write_only)
{
    memset(&_parser_setting, 0, sizeof(_parser_setting));
    _parser.data = this;
    _parser_setting.on_message_begin = [](http_parser* parser)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        owner->_current_message.reset(message_ex::create_receive_message_with_standalone_header(blob()));
        return 0;
    };
    _parser_setting.on_header_field = [](http_parser* parser, const char *at, size_t length)->int
    {
#define StrLiteralLen(str) (sizeof(ArraySizeHelper(str)) - 1)
#define MATCH(pat) (length >= StrLiteralLen(pat) && strncmp(at, pat, StrLiteralLen(pat)) == 0)
        auto owner = static_cast<http_message_parser*>(parser->data);
        if (MATCH("rpc_id"))
        {
            owner->response_parse_state = parsing_rpc_id;
        }
        else if (MATCH("id"))
        {
            owner->response_parse_state = parsing_id;
        }
        else if (MATCH("name"))
        {
            owner->response_parse_state = parsing_rpc_name;
        }
        else if (MATCH("payload_format"))
        {
            owner->response_parse_state = parsing_payload_format;
        }
        return 0;
#undef StrLiteralLen
#undef MATCH
    };
    _parser_setting.on_header_value = [](http_parser* parser, const char *at, size_t length)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        switch(owner->response_parse_state)
        {
        case parsing_rpc_id:
            owner->_current_message->header->rpc_id = std::atoi(std::string(at, length).c_str());
            break;
        case parsing_id:
            owner->_current_message->header->id = std::atoi(std::string(at, length).c_str());
            break;
        case parsing_rpc_name:
            dassert(length < DSN_MAX_TASK_CODE_NAME_LENGTH, "task code too long");
            strncpy(owner->_current_message->header->rpc_name, at, length);
            owner->_current_message->header->rpc_name[length] = 0;
            break;
        case parsing_payload_format:
            owner->_current_message->header->context.u.serialize_format = std::atoi(std::string(at, length).c_str());
            break;
        case parsing_nothing:
            ;
            //no default
        }
        owner->response_parse_state = parsing_nothing;
        return 0;
    };
    _parser_setting.on_url = [](http_parser* parser, const char *at, size_t length)->int
    {

        using namespace boost::xpressive;
        auto owner = static_cast<http_message_parser*>(parser->data);
        
        http_parser_url url;
        http_parser_parse_url(at, length, 1, &url);
        if (((url.field_set >> UF_PATH) & 1) == 0)
        {
            derror("url has no path field");
            //error, reset parser state
            return 1;
        }

        owner->_current_message->header->context.u.is_request = 1;
        cregex url_regex = '/' >> (s1 = +_w) >> '/' >> (s2 = +_d) >> (s3 = '?' >> *_);
        constexpr int const url_regex_subs[] = { 1, 2, 3 };
        cregex_token_iterator cur(at, at + length, url_regex, url_regex_subs), end;
        dassert(cur != end, "");
        dassert(cur->length() <= DSN_MAX_TASK_CODE_NAME_LENGTH, "");
        std::copy(range_begin(*cur), range_end(*cur), owner->_current_message->header->rpc_name);
        ++cur;
        dassert(cur != end, "");
        owner->_current_message->header->client.hash = boost::lexical_cast<uint64_t>(*cur);
        ++cur;
        dassert(cur != end, "");
        {
            cregex query_regex = (as_xpr('?') | '&') >> (s1 = +_w) >> '=' >> (s2 = +_d);
            constexpr int const query_regex_subs[] = { 1, 2 };
            cur = cregex_token_iterator(range_begin(*cur), range_end(*cur), query_regex, query_regex_subs);
            for (; cur != end; ++cur)
            {
                if (cur->compare("payload_format") == 0)
                {
                    ++cur;
                    dassert(cur != end, "");
                    owner->_current_message->header->context.u.serialize_format = boost::lexical_cast<uint64_t>(*cur);
                }
                else
                {
                    dwarn("unused parameter in url");
                }
            }
        }
        return 0;
    };
    _parser_setting.on_body = [](http_parser* parser, const char *at, size_t length)->int
    {
        derror("%s\n", std::string(at, length).c_str());
        auto owner = static_cast<http_message_parser*>(parser->data);
        dassert(owner->_read_buffer.buffer() != nullptr, "the read buffer is not owning");
        owner->_current_message->buffers[0].assign(owner->_read_buffer.buffer(), at - owner->_read_buffer.buffer_ptr(), length);
        owner->_received_messages.emplace(std::move(owner->_current_message));
        return 0;
    };
    http_parser_init(&_parser, HTTP_BOTH);
}

message_ex* http_message_parser::get_message_on_receive(unsigned int read_length, /*out*/ int& read_next)
{
    read_next = 4096;
    auto nparsed = http_parser_execute(&_parser, &_parser_setting, _read_buffer.data(), read_length);
    _read_buffer = _read_buffer.range(nparsed);
    if (_parser.upgrade)
    {
        derror("unsupported protocol");
        return nullptr;
    }
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

int http_message_parser::prepare_buffers_on_send(message_ex* msg, unsigned int offset, send_buf* buffers)
{
    int buffer_iter = 0;
    if (msg->header->context.u.is_request)
    {
        if (offset == 0)
        {
            std::stringstream ss;
            ss << "POST /" << msg->header->rpc_name << "/" << msg->header->client.hash << " HTTP/1.1\r\n";
            ss << "Content-Type: text/plain\r\n";
            ss << "rpc_id: " << msg->header->rpc_id << "\r\n";
            ss << "id: " << msg->header->id << "\r\n";
            ss << "rpc_name: " << msg->header->rpc_name << "\r\n";
            ss << "payload_format: " << msg->header->context.u.serialize_format << "\r\n";
            ss << "Content-Length: " << msg->body_size() << "\r\n";
            ss << "\r\n";
            request_header_send_buffer = ss.str();
        }
        if (offset < request_header_send_buffer.size())
        {
            buffers[0].buf = const_cast<char*>(request_header_send_buffer.c_str()) + offset;
            buffers[0].sz = request_header_send_buffer.size() - offset;
            buffer_iter = 1;
            offset = 0;
        }
        else
        {
            offset -= request_header_send_buffer.size();
        }
    }
    else
    {
        if (offset == 0)
        {
            std::stringstream ss;
            ss << "HTTP/1.1 200 OK\r\n";
            ss << "Access-Control-Allow-Headers: Content-Type, Access-Control-Allow-Headers, Access-Control-Allow-Origin\r\n";
            ss << "Content-Type: text/plain\r\n";
            ss << "Access-Control-Allow-Origin: *\r\n";
            ss << "rpc_id: " << msg->header->rpc_id << "\r\n";
            ss << "id: " << msg->header->id << "\r\n";
            ss << "rpc_name: " << msg->header->rpc_name << "\r\n";
            ss << "payload_format: " << msg->header->context.u.serialize_format << "\r\n";
            ss << "Content-Length: " << msg->body_size() << "\r\n";
            ss << "\r\n";
            response_header_send_buffer = ss.str();
        }
        if (offset < response_header_send_buffer.size())
        {
            buffers[0].buf = const_cast<char*>(response_header_send_buffer.c_str()) + offset;
            buffers[0].sz = response_header_send_buffer.size() - offset;
            buffer_iter = 1;
            offset = 0;
        }
        else
        {
            offset -= response_header_send_buffer.size();
        }
    }
    //skip message header
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
    return buffer_iter;
}

int http_message_parser::get_send_buffers_count(message_ex* msg)
{
    return msg->buffers.size() + 2;
}

}
