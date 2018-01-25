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

#include <dsn/utility/ports.h>
#include <dsn/utility/singleton.h>
#include <dsn/utility/crc.h>
#include <dsn/tool-api/rpc_message.h>
#include <vector>
#include <iomanip>
#include "http_message_parser.h"
#include <dsn/cpp/serialization.h>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "http.message.parser"

namespace dsn {

template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];

http_message_parser::http_message_parser()
{
    memset(&_parser_setting, 0, sizeof(_parser_setting));
    _parser.data = this;
    _parser_setting.on_message_begin = [](http_parser *parser) -> int {
        auto owner = static_cast<http_message_parser *>(parser->data);

        owner->_current_message.reset(
            message_ex::create_receive_message_with_standalone_header(blob()));
        owner->_response_parse_state = parsing_nothing;

        message_header *header = owner->_current_message->header;
        header->hdr_length = sizeof(message_header);
        header->hdr_crc32 = header->body_crc32 = CRC_INVALID;
        return 0;
    };
    _parser_setting.on_url = [](http_parser *parser, const char *at, size_t length) -> int {
        // see https://github.com/imzhenyu/rDSN/issues/420
        // url = "/" + payload_format + "/" + thread_hash + "/" + rpc_code;
        // e.g., /DSF_THRIFT_JSON/0/RPC_CLI_CLI_CALL

        std::string url(at, length);
        std::vector<std::string> args;
        utils::split_args(url.c_str(), args, '/');

        dinfo("http call %s", url.c_str());

        if (args.size() != 3) {
            dinfo("skip url parse for %s, could be done in headers if not cross-domain",
                  url.c_str());
            return 0;
        }

        auto owner = static_cast<http_message_parser *>(parser->data);
        auto &hdr = owner->_current_message->header;

        // serialize-type
        dsn_msg_serialize_format fmt = enum_from_string(args[0].c_str(), DSF_INVALID);
        if (fmt == DSF_INVALID) {
            derror("invalid serialize_format in url %s", url.c_str());
            return 1;
        }
        hdr->context.u.serialize_format = fmt;

        // thread-hash
        char *end;
        hdr->client.thread_hash = std::strtol(args[1].c_str(), &end, 10);
        if (end != args[1].c_str() + args[1].length()) {
            derror("invalid thread hash in url %s", url.c_str());
            return 1;
        }

        // rpc-code
        if (args[2].length() > DSN_MAX_TASK_CODE_NAME_LENGTH) {
            derror("too long rpc code in url %s", url.c_str());
            return 1;
        }
        strcpy(hdr->rpc_name, args[2].c_str());

        return 0;
    };
    _parser_setting.on_header_field =
        [](http_parser *parser, const char *at, size_t length) -> int {
#define StrLiteralLen(str) (sizeof(ArraySizeHelper(str)) - 1)
#define MATCH(pat) (length >= StrLiteralLen(pat) && strncmp(at, pat, StrLiteralLen(pat)) == 0)
        auto owner = static_cast<http_message_parser *>(parser->data);
        if (MATCH("id")) {
            owner->_response_parse_state = parsing_id;
        } else if (MATCH("trace_id")) {
            owner->_response_parse_state = parsing_trace_id;
        } else if (MATCH("rpc_name")) {
            owner->_response_parse_state = parsing_rpc_name;
        } else if (MATCH("app_id")) {
            owner->_response_parse_state = parsing_app_id;
        } else if (MATCH("partition_index")) {
            owner->_response_parse_state = parsing_partition_index;
        } else if (MATCH("serialize_format")) {
            owner->_response_parse_state = parsing_serialize_format;
        } else if (MATCH("from_address")) {
            owner->_response_parse_state = parsing_from_address;
        } else if (MATCH("client_timeout")) {
            owner->_response_parse_state = parsing_client_timeout;
        } else if (MATCH("client_thread_hash")) {
            owner->_response_parse_state = parsing_client_thread_hash;
        } else if (MATCH("client_partition_hash")) {
            owner->_response_parse_state = parsing_client_partition_hash;
        } else if (MATCH("server_error")) {
            owner->_response_parse_state = parsing_server_error;
        }
        return 0;
#undef StrLiteralLen
#undef MATCH
    };
    _parser_setting.on_header_value =
        [](http_parser *parser, const char *at, size_t length) -> int {
        auto owner = static_cast<http_message_parser *>(parser->data);
        message_header *header = owner->_current_message->header;
        switch (owner->_response_parse_state) {
        case parsing_id: {
            char *end;
            header->id = std::strtoull(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_trace_id: {
            char *end;
            header->trace_id = std::strtoull(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.trace_id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_rpc_name: {
            if (length >= DSN_MAX_TASK_CODE_NAME_LENGTH) {
                derror("too long header.rpc_name '%.*s'", length, at);
                return 1;
            }
            strncpy(header->rpc_name, at, length);
            header->rpc_name[length] = 0;
            break;
        }
        case parsing_app_id: {
            char *end;
            header->gpid.u.app_id = std::strtol(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.app_id '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_partition_index: {
            char *end;
            header->gpid.u.partition_index = std::strtol(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.partition_index '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_serialize_format: {
            dsn_msg_serialize_format fmt =
                enum_from_string(std::string(at, length).c_str(), DSF_INVALID);
            if (fmt == DSF_INVALID) {
                derror("invalid header.serialize_format '%.*s'", length, at);
                return 1;
            }
            header->context.u.serialize_format = fmt;
            break;
        }
        case parsing_from_address: {
            int pos = -1;
            int dot_count = 0;
            for (int i = 0; i < length; ++i) {
                if (at[i] == ':') {
                    pos = i;
                    break;
                } else if (at[i] == '.') {
                    dot_count++;
                }
            }
            if (pos == -1 || pos == (length - 1) || dot_count != 3) {
                derror("invalid header.from_address '%.*s'", length, at);
                return 1;
            }
            char *end;
            unsigned long port = std::strtol(at + pos + 1, &end, 10);
            if (end != at + length) {
                derror("invalid header.from_address '%.*s'", length, at);
                return 1;
            }
            std::string host(at, pos);
            header->from_address.assign_ipv4(host.c_str(), port);
            break;
        }
        case parsing_client_timeout: {
            char *end;
            header->client.timeout_ms = std::strtol(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.client_timeout '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_client_thread_hash: {
            char *end;
            header->client.thread_hash = std::strtol(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.client_thread_hash '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_client_partition_hash: {
            char *end;
            header->client.partition_hash = std::strtoull(at, &end, 10);
            if (end != at + length) {
                derror("invalid header.client_partition_hash '%.*s'", length, at);
                return 1;
            }
            break;
        }
        case parsing_server_error: {
            if (length >= DSN_MAX_ERROR_CODE_NAME_LENGTH) {
                derror("too long header.server_error '%.*s'", length, at);
                return 1;
            }
            strncpy(header->server.error_name, at, length);
            header->server.error_name[length] = 0;
            break;
        }
        case parsing_nothing:;
            // no default
        }
        owner->_response_parse_state = parsing_nothing;
        return 0;
    };
    _parser_setting.on_headers_complete = [](http_parser *parser) -> int {
        auto owner = static_cast<http_message_parser *>(parser->data);
        message_header *header = owner->_current_message->header;
        if (parser->type == HTTP_REQUEST && parser->method == HTTP_GET) {
            header->hdr_type = *(uint32_t *)"GET ";
            header->context.u.is_request = 1;
        } else if (parser->type == HTTP_REQUEST && parser->method == HTTP_POST) {
            header->hdr_type = *(uint32_t *)"POST";
            header->context.u.is_request = 1;
        } else if (parser->type == HTTP_REQUEST && parser->method == HTTP_OPTIONS) {
            header->hdr_type = *(uint32_t *)"OPTI";
            header->context.u.is_request = 1;
        } else if (parser->type == HTTP_RESPONSE) {
            header->hdr_type = *(uint32_t *)"HTTP";
            header->context.u.is_request = 0;
        } else {
            derror("invalid http type %d and method %d", parser->type, parser->method);
            return 1;
        }
        return 0;
    };
    _parser_setting.on_body = [](http_parser *parser, const char *at, size_t length) -> int {
        auto owner = static_cast<http_message_parser *>(parser->data);
        dassert(owner->_current_buffer.buffer() != nullptr, "the read buffer is not owning");
        owner->_current_message->buffers.rbegin()->assign(
            owner->_current_buffer.buffer(), at - owner->_current_buffer.buffer_ptr(), length);
        owner->_current_message->header->body_length = length;
        owner->_received_messages.emplace(std::move(owner->_current_message));
        return 0;
    };
    http_parser_init(&_parser, HTTP_BOTH);
}

void http_message_parser::reset() { http_parser_init(&_parser, HTTP_BOTH); }

message_ex *http_message_parser::get_message_on_receive(message_reader *reader,
                                                        /*out*/ int &read_next)
{
    read_next = 4096;

    if (reader->_buffer_occupied > 0) {
        _current_buffer = reader->_buffer;
        auto nparsed = http_parser_execute(
            &_parser, &_parser_setting, reader->_buffer.data(), reader->_buffer_occupied);
        _current_buffer = blob();
        reader->_buffer = reader->_buffer.range(nparsed);
        reader->_buffer_occupied -= nparsed;
        if (_parser.upgrade) {
            derror("unsupported http protocol");
            read_next = -1;
            return nullptr;
        }
    }

    if (!_received_messages.empty()) {
        auto msg = std::move(_received_messages.front());
        _received_messages.pop();

        dinfo("rpc_name = %s, from_address = %s, seq_id = %" PRIu64 ", trace_id = %016" PRIx64,
              msg->header->rpc_name,
              msg->header->from_address.to_string(),
              msg->header->id,
              msg->header->trace_id);

        msg->hdr_format = NET_HDR_HTTP;
        return msg.release();
    } else {
        return nullptr;
    }
}

void http_message_parser::prepare_on_send(message_ex *msg)
{
    auto &header = msg->header;
    auto &buffers = msg->buffers;

    // construct http header blob
    std::string header_str;
    if (header->context.u.is_request) {
        std::stringstream ss;
        ss << "POST /" << header->rpc_name << " HTTP/1.1\r\n";
        ss << "Content-Type: text/plain\r\n";
        ss << "id: " << header->id << "\r\n";
        ss << "trace_id: " << header->trace_id << "\r\n";
        ss << "rpc_name: " << header->rpc_name << "\r\n";
        ss << "app_id: " << header->gpid.u.app_id << "\r\n";
        ss << "partition_index: " << header->gpid.u.partition_index << "\r\n";
        ss << "serialize_format: "
           << enum_to_string((dsn_msg_serialize_format)header->context.u.serialize_format)
           << "\r\n";
        ss << "from_address: " << header->from_address.to_string() << "\r\n";
        ss << "client_timeout: " << header->client.timeout_ms << "\r\n";
        ss << "client_thread_hash: " << header->client.thread_hash << "\r\n";
        ss << "client_partition_hash: " << header->client.partition_hash << "\r\n";
        ss << "Content-Length: " << msg->body_size() << "\r\n";
        ss << "\r\n";
        header_str = ss.str();
    } else {
        std::stringstream ss;
        ss << "HTTP/1.1 200 OK\r\n";
        ss << "Access-Control-Allow-Headers: Content-Type, Access-Control-Allow-Headers, "
              "Access-Control-Allow-Origin\r\n";
        ss << "Content-Type: text/plain\r\n";
        ss << "Access-Control-Allow-Origin: *\r\n";
        ss << "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n";
        ss << "id: " << header->id << "\r\n";
        ss << "trace_id: " << header->trace_id << "\r\n";
        ss << "rpc_name: " << header->rpc_name << "\r\n";
        ss << "serialize_format: "
           << enum_to_string((dsn_msg_serialize_format)header->context.u.serialize_format)
           << "\r\n";
        ss << "from_address: " << header->from_address.to_string() << "\r\n";
        ss << "server_error: " << header->server.error_name << "\r\n";
        ss << "Content-Length: " << msg->body_size() << "\r\n";
        ss << "\r\n";
        header_str = ss.str();
    }
    unsigned int header_len = header_str.size();
    std::shared_ptr<char> header_holder(static_cast<char *>(dsn_transient_malloc(header_len)),
                                        [](char *c) { dsn_transient_free(c); });
    memcpy(header_holder.get(), header_str.data(), header_len);

    unsigned int dsn_size = sizeof(message_header) + header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < buffers.size()) {
        blob &buf = buffers[dsn_buf_count];
        dassert(dsn_size >= buf.length(), "%u VS %u", dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;
    }
    dassert(dsn_size == 0, "dsn_size = %u", dsn_size);

    // put header_bb at the end
    buffers.resize(dsn_buf_count);
    buffers.emplace_back(blob(std::move(header_holder), header_len));
}

int http_message_parser::get_buffer_count_on_send(message_ex *msg)
{
    return (int)msg->buffers.size();
}

int http_message_parser::get_buffers_on_send(message_ex *msg, send_buf *buffers)
{
    auto &msg_header = msg->header;
    auto &msg_buffers = msg->buffers;

    // leave buffers[0] to header
    int i = 1;
    // we must skip the dsn message header
    unsigned int offset = sizeof(message_header);
    unsigned int dsn_size = sizeof(message_header) + msg_header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < msg_buffers.size()) {
        blob &buf = msg_buffers[dsn_buf_count];
        dassert(dsn_size >= buf.length(), "%u VS %u", dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;

        if (offset >= buf.length()) {
            offset -= buf.length();
            continue;
        }
        buffers[i].buf = (void *)(buf.data() + offset);
        buffers[i].sz = buf.length() - offset;
        offset = 0;
        ++i;
    }
    dassert(dsn_size == 0, "dsn_size = %u", dsn_size);
    dassert(dsn_buf_count + 1 == msg_buffers.size(), "must have 1 more blob at the end");

    // set header
    blob &header_bb = msg_buffers[dsn_buf_count];
    buffers[0].buf = (void *)header_bb.data();
    buffers[0].sz = header_bb.length();

    return i;
}
}
