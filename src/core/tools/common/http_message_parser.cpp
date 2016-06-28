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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "http.message.parser"

namespace dsn{

template <typename T, size_t N>
char(&ArraySizeHelper(T(&array)[N]))[N];

http_message_parser::http_message_parser()
{
    memset(&_parser_setting, 0, sizeof(_parser_setting));
    _parser_setting.on_message_begin = [](http_parser* parser)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        owner->_current_message.reset(message_ex::create_receive_message_with_standalone_header(blob(), false));
        message_header* header = owner->_current_message->header;
        header->hdr_length = sizeof(message_header);
        header->hdr_crc32 = header->body_crc32 = CRC_INVALID;
        return 0;
    };
    _parser_setting.on_url = [](http_parser* parser, const char *at, size_t length)->int
    {
        // url is not used now
        /*
        using namespace boost::xpressive;
        auto owner = static_cast<http_message_parser*>(parser->data);

        http_parser_url url;
        http_parser_parse_url(at, length, 1, &url);
        if (((url.field_set >> UF_PATH) & 1) == 0)
        {
            derror("no path field exist in url '%.*s'", length, at);
            return 1;
        }

        cregex url_regex = '/' >> (s1 = +_w);
        constexpr int const url_regex_subs[] = { 1 };
        cregex_token_iterator cur(at, at + length, url_regex, url_regex_subs), end;
        if (cur == end)
        {
            derror("invalid url '%.*s'", length, at);
            return 1;
        }
        if (cur->length() >= DSN_MAX_TASK_CODE_NAME_LENGTH)
        {
            derror("too long rpc name in url '%.*s'", length, at);
            return 1;
        }
        std::copy(range_begin(*cur), range_end(*cur), owner->_current_message->header->rpc_name);
        owner->_current_message->header->rpc_name[cur->length()] = 0;
        */

        return 0;
    };
    _parser_setting.on_header_field = [](http_parser* parser, const char *at, size_t length)->int
    {
#define StrLiteralLen(str) (sizeof(ArraySizeHelper(str)) - 1)
#define MATCH(pat) (length >= StrLiteralLen(pat) && strncmp(at, pat, StrLiteralLen(pat)) == 0)
        auto owner = static_cast<http_message_parser*>(parser->data);
        if (MATCH("id"))
        {
            owner->_response_parse_state = parsing_id;
        }
        else if (MATCH("trace_id"))
        {
            owner->_response_parse_state = parsing_trace_id;
        }
        else if (MATCH("rpc_name"))
        {
            owner->_response_parse_state = parsing_rpc_name;
        }
        else if (MATCH("app_id"))
        {
            owner->_response_parse_state = parsing_app_id;
        }
        else if (MATCH("partition_index"))
        {
            owner->_response_parse_state = parsing_partition_index;
        }
        else if (MATCH("serialize_format"))
        {
            owner->_response_parse_state = parsing_serialize_format;
        }
        else if (MATCH("from_address"))
        {
            owner->_response_parse_state = parsing_from_address;
        }
        else if (MATCH("client_hash"))
        {
            owner->_response_parse_state = parsing_client_hash;
        }
        else if (MATCH("client_timeout"))
        {
            owner->_response_parse_state = parsing_client_timeout;
        }
        else if (MATCH("server_error"))
        {
            owner->_response_parse_state = parsing_server_error;
        }
        return 0;
#undef StrLiteralLen
#undef MATCH
    };
    _parser_setting.on_header_value = [](http_parser* parser, const char *at, size_t length)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        message_header* header = owner->_current_message->header;
        switch(owner->_response_parse_state)
        {
        case parsing_id:
        {
            char *end;
            header->id = std::strtoull(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_trace_id:
        {
            char *end;
            header->trace_id = std::strtoull(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.trace_id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_rpc_name:
        {
            if (length >= DSN_MAX_TASK_CODE_NAME_LENGTH)
            {
                derror("too long header.rpc_name '%.*s'", length, at);
                return 1;
            }
            strncpy(header->rpc_name, at, length);
            header->rpc_name[length] = 0;
            break;
        }
        case parsing_app_id:
        {
            char *end;
            header->gpid.u.app_id = std::strtol(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.app_id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_partition_index:
        {
            char *end;
            header->gpid.u.partition_index = std::strtol(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.partition_index '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_serialize_format:
        {
            dsn_msg_serialize_format fmt = enum_from_string(std::string(at, length).c_str(), DSF_INVALID);
            if (fmt == DSF_INVALID)
            {
                derror("invalid header.serialize_format '%.*s'", length, at);
                return 1;
            }
            header->context.u.serialize_format = fmt;
            break;
        }
        case parsing_from_address:
        {
            int pos = -1;
            int dot_count = 0;
            for (int i = 0; i < length; ++i)
            {
                if (at[i] == ':')
                {
                    pos = i;
                    break;
                }
                else if (at[i] == '.')
                {
                    dot_count++;
                }
            }
            if (pos == -1 || pos == (length - 1) || dot_count != 3)
            {
                derror("invalid header.from_address '%.*s'", length, at);
                return 1;
            }
            char *end;
            unsigned long port = std::strtol(at + pos + 1, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.from_address '%.*s'", length, at);
                return 1;
            }
            std::string host(at, pos);
            header->from_address.assign_ipv4(host.c_str(), port);
            break;
        }
        case parsing_client_hash:
        {
            char *end;
            header->client.hash = std::strtoull(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.client_hash '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_client_timeout:
        {
            char *end;
            header->client.timeout_ms = std::strtoll(at, &end, 10);
            if (end != at + length)
            {
                derror("invalid header.client_timeout '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_server_error:
        {
            if (length >= DSN_MAX_ERROR_CODE_NAME_LENGTH)
            {
                derror("too long header.server_error '%.*s'", length, at);
                return 1;
            }
            strncpy(header->server.error_name, at, length);
            header->server.error_name[length] = 0;
            break;
        }
        case parsing_nothing:
            ;
            //no default
        }
        owner->_response_parse_state = parsing_nothing;
        return 0;
    };
    _parser_setting.on_headers_complete = [](http_parser* parser)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        message_header* header = owner->_current_message->header;
        if (parser->type == HTTP_REQUEST && parser->method == HTTP_GET)
        {
            header->hdr_type = header_type::hdr_type_http_get;
            header->context.u.is_request = 1;
        }
        else if (parser->type == HTTP_REQUEST && parser->method == HTTP_POST)
        {
            header->hdr_type = header_type::hdr_type_http_post;
            header->context.u.is_request = 1;
        }
        else if (parser->type == HTTP_RESPONSE)
        {
            header->hdr_type = header_type::hdr_type_http_response;
            header->context.u.is_request = 0;
        }
        else
        {
            derror("invalid http type %d and method %d", parser->type, parser->method);
            return 1;
        }
        return 0;
    };
    _parser_setting.on_body = [](http_parser* parser, const char *at, size_t length)->int
    {
        auto owner = static_cast<http_message_parser*>(parser->data);
        dassert(owner->_current_buffer.buffer() != nullptr, "the read buffer is not owning");
        owner->_current_message->buffers[0].assign(owner->_current_buffer.buffer(), at - owner->_current_buffer.buffer_ptr(), length);
        owner->_current_message->header->body_length = length;
        return 0;
    };
    _parser.data = this;
    http_parser_init(&_parser, HTTP_BOTH);
}

void http_message_parser::reset()
{
}

message_ex* http_message_parser::get_message_on_receive(message_reader* reader, /*out*/ int& read_next)
{
    _current_buffer = reader->_buffer;
    _current_message.reset();
    _response_parse_state = parsing_nothing;
    http_parser_init(&_parser, HTTP_BOTH);
    auto nparsed = http_parser_execute(&_parser, &_parser_setting, reader->_buffer.data(), reader->_buffer_occupied);
    _current_buffer = blob();
    reader->_buffer = reader->_buffer.range(nparsed);
    reader->_buffer_occupied -= nparsed;
    if (_parser.upgrade)
    {
        derror("unsupported http protocol");
        read_next = -1;
        return nullptr;
    }
    if (_current_message)
    {
        message_ex* msg = _current_message.release();
        dinfo("rpc_name: %s, from_address: %s, seq_id: %" PRIu64 ", trace_id: %" PRIu64,
              msg->header->rpc_name, msg->header->from_address.to_string(),
              msg->header->id, msg->header->trace_id);
        read_next = 4096;
        msg->hdr_format = NET_HDR_HTTP;
        return msg;
    }
    else
    {
        derror("invalid http message");
        read_next = -1;
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
        ss << "POST /" << msg->header->rpc_name << " HTTP/1.1\r\n";
        ss << "Content-Type: text/plain\r\n";
        ss << "id: " << msg->header->id << "\r\n";
        ss << "trace_id: " << msg->header->trace_id << "\r\n";
        ss << "rpc_name: " << msg->header->rpc_name << "\r\n";
        ss << "app_id: " << msg->header->gpid.u.app_id << "\r\n";
        ss << "partition_index: " << msg->header->gpid.u.partition_index << "\r\n";
        ss << "serialize_format: " << enum_to_string((dsn_msg_serialize_format)msg->header->context.u.serialize_format) << "\r\n";
        ss << "from_address: " << msg->header->from_address.to_string() << "\r\n";
        ss << "client_hash: " << msg->header->client.hash << "\r\n";
        ss << "client_timeout: " << msg->header->client.timeout_ms << "\r\n";
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
        ss << "serialize_format: " << enum_to_string((dsn_msg_serialize_format)msg->header->context.u.serialize_format) << "\r\n";
        ss << "from_address: " << msg->header->from_address.to_string() << "\r\n";
        ss << "server_error: " << msg->header->server.error_name << "\r\n";
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
