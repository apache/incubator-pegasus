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
*     Jun. 2016, Zuoyan Qin, second version
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

http_message_parser::http_message_parser()
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
        if (MATCH("trace_id"))
        {
            owner->response_parse_state = parsing_trace_id;
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
        case parsing_trace_id:
            owner->_current_message->header->trace_id = std::atoi(std::string(at, length).c_str());
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
        dassert(owner->_current_buffer.buffer() != nullptr, "the read buffer is not owning");
        owner->_current_message->buffers[0].assign(owner->_current_buffer.buffer(), at - owner->_current_buffer.buffer_ptr(), length);
        owner->_received_messages.emplace(std::move(owner->_current_message));
        return 0;
    };
    http_parser_init(&_parser, HTTP_BOTH);
}

void http_message_parser::reset()
{
    _current_buffer = blob();
    _current_message.reset();
    _received_messages = std::queue<std::unique_ptr<message_ex>>();
}

message_ex* http_message_parser::get_message_on_receive(message_reader* reader, /*out*/ int& read_next)
{
    read_next = 4096;
    _current_buffer = reader->_buffer;
    auto nparsed = http_parser_execute(&_parser, &_parser_setting, reader->_buffer.data(), reader->_buffer_occupied);
    reader->_buffer = reader->_buffer.range(nparsed);
    reader->_buffer_occupied -= nparsed;
    if (_parser.upgrade)
    {
        derror("unsupported protocol");
        return nullptr;
    }
    if (!_received_messages.empty())
    {
        auto message = std::move(_received_messages.front());
        _received_messages.pop();
        message->parser = this;
        return message.release();
    }
    else
    {
        return nullptr;
    }
}

int http_message_parser::prepare_on_send(message_ex* msg)
{
    return (int)msg->buffers.size() + 1;
}

int http_message_parser::get_buffers_on_send(message_ex* msg, send_buf* buffers)
{
    // construct http header blob
    std::string header_str;
    if (msg->header->context.u.is_request)
    {
        std::stringstream ss;
        ss << "POST /" << msg->header->rpc_name << "/" << msg->header->client.hash << " HTTP/1.1\r\n";
        ss << "Content-Type: text/plain\r\n";
        ss << "id: " << msg->header->id << "\r\n";
        ss << "trace_id: " << msg->header->trace_id << "\r\n";
        ss << "rpc_name: " << msg->header->rpc_name << "\r\n";
        ss << "payload_format: " << msg->header->context.u.serialize_format << "\r\n";
        ss << "Content-Length: " << msg->body_size() << "\r\n";
        ss << "\r\n";
        header_str = ss.str();
    }
    else
    {
        std::stringstream ss;
        ss << "HTTP/1.1 200 OK\r\n";
        ss << "Access-Control-Allow-Headers: Content-Type, Access-Control-Allow-Headers, Access-Control-Allow-Origin\r\n";
        ss << "Content-Type: text/plain\r\n";
        ss << "Access-Control-Allow-Origin: *\r\n";
        ss << "id: " << msg->header->id << "\r\n";
        ss << "trace_id: " << msg->header->trace_id << "\r\n";
        ss << "rpc_name: " << msg->header->rpc_name << "\r\n";
        ss << "payload_format: " << msg->header->context.u.serialize_format << "\r\n";
        ss << "Content-Length: " << msg->body_size() << "\r\n";
        ss << "\r\n";
        header_str = ss.str();
    }
    unsigned int header_len = header_str.size();
    std::shared_ptr<char> header_holder(static_cast<char*>(dsn_transient_malloc(header_len)), [](char* c) {dsn_transient_free(c);});
    memcpy(header_holder.get(), header_str.data(), header_len);

    // first fill the header blob
    buffers[0].buf = header_holder.get();
    buffers[0].sz = header_len;

    // fill buffers
    int i = 1;
    // we must skip the standard message_header
    unsigned int offset = sizeof(message_header);
    for (blob& buf : msg->buffers)
    {
        if (offset >= buf.length())
        {
            offset -= buf.length();
            continue;
        }

        buffers[i].buf = (void*)(buf.data() + offset);
        buffers[i].sz = buf.length() - offset;
        offset = 0;
        ++i;
    }

    // put http header blob at the back of message buffer
    msg->buffers.emplace_back(blob(std::move(header_holder), header_len));

    return i;
}

}
