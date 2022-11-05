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

#include "http_message_parser.h"

#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/crc.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "http_server.h"
#include <iomanip>

namespace dsn {

struct parser_context
{
    http_message_parser *parser;
    message_reader *reader;
};

/*extern*/ const char *http_parser_stage_to_string(http_parser_stage s)
{
    switch (s) {
    case HTTP_ON_MESSAGE_BEGIN:
        return "HTTP_ON_MESSAGE_BEGIN";
    case HTTP_ON_URL:
        return "HTTP_ON_URL";
    case HTTP_ON_STATUS:
        return "HTTP_ON_STATUS";
    case HTTP_ON_HEADER_FIELD:
        return "HTTP_ON_HEADER_FIELD";
    case HTTP_ON_HEADER_VALUE:
        return "HTTP_ON_HEADER_VALUE";
    case HTTP_ON_HEADERS_COMPLETE:
        return "HTTP_ON_HEADERS_COMPLETE";
    case HTTP_ON_BODY:
        return "HTTP_ON_BODY";
    case HTTP_ON_MESSAGE_COMPLETE:
        return "HTTP_ON_MESSAGE_COMPLETE";
    default:
        return "invalid";
    }
}

http_message_parser::http_message_parser()
{
    memset(&_parser_setting, 0, sizeof(_parser_setting));

    _parser_setting.on_message_begin = [](http_parser *parser) -> int {
        auto &msg = static_cast<parser_context *>(parser->data)->parser->_current_message;

        // initialize http message
        // msg->buffers[0] = header
        // msg->buffers[1] = body (blob())
        msg.reset(message_ex::create_receive_message_with_standalone_header(blob()));
        msg->buffers.resize(HTTP_MSG_BUFFERS_NUM);

        message_header *header = msg->header;
        header->hdr_length = sizeof(message_header);
        header->hdr_crc32 = header->body_crc32 = CRC_INVALID;
        strcpy(header->rpc_name, "RPC_HTTP_SERVICE");
        return 0;
    };

    _parser_setting.on_url = [](http_parser *parser, const char *at, size_t length) -> int {
        http_message_parser *msg_parser = static_cast<parser_context *>(parser->data)->parser;
        msg_parser->_stage = HTTP_ON_URL;
        msg_parser->_url.append(at, length);
        return 0;
    };

    _parser_setting.on_header_field =
        [](http_parser *parser, const char *at, size_t length) -> int {
        http_message_parser *msg_parser = static_cast<parser_context *>(parser->data)->parser;
        msg_parser->_stage = HTTP_ON_HEADER_FIELD;
        if (strncmp(at, "Content-Type", length) == 0) {
            msg_parser->_is_field_content_type = true;
        }
        return 0;
    };

    _parser_setting.on_header_value =
        [](http_parser *parser, const char *at, size_t length) -> int {
        http_message_parser *msg_parser = static_cast<parser_context *>(parser->data)->parser;
        msg_parser->_stage = HTTP_ON_HEADER_VALUE;
        if (msg_parser->_is_field_content_type) {
            auto &msg = msg_parser->_current_message;
            // msg->buffers[3] = content-type
            msg->buffers[3] = blob::create_from_bytes(at, length);
            msg_parser->_is_field_content_type = false;
        }
        return 0;
    };

    _parser_setting.on_headers_complete = [](http_parser *parser) -> int {
        http_message_parser *msg_parser = static_cast<parser_context *>(parser->data)->parser;
        msg_parser->_stage = HTTP_ON_HEADERS_COMPLETE;

        auto &msg = msg_parser->_current_message;

        // msg->buffers[2] = url
        msg->buffers[2] = blob::create_from_bytes(std::move(msg_parser->_url));

        message_header *header = msg->header;
        if (parser->type == HTTP_REQUEST && parser->method == HTTP_GET) {
            header->hdr_type = http_method::HTTP_METHOD_GET;
            header->context.u.is_request = 1;
        } else if (parser->type == HTTP_REQUEST && parser->method == HTTP_POST) {
            header->hdr_type = http_method::HTTP_METHOD_POST;
            header->context.u.is_request = 1;
        } else {
            LOG_ERROR("invalid http type %d and method %d", parser->type, parser->method);
            return 1;
        }
        return 0;
    };

    _parser_setting.on_message_complete = [](http_parser *parser) -> int {
        auto message_parser = static_cast<parser_context *>(parser->data)->parser;
        message_parser->_received_messages.emplace(std::move(message_parser->_current_message));
        message_parser->_stage = HTTP_ON_MESSAGE_COMPLETE;
        return 0;
    };

    // rDSN application can only serve as http server, support for http client is not in our plan.
    http_parser_init(&_parser, HTTP_REQUEST);
}

message_ex *http_message_parser::get_message_on_receive(message_reader *reader,
                                                        /*out*/ int &read_next)
{
    read_next = 4096;

    if (reader->_buffer_occupied > 0) {
        parser_context ctx{this, reader};
        _parser.data = &ctx;

        _parser_setting.on_body = [](http_parser *parser, const char *at, size_t length) -> int {
            auto data = static_cast<parser_context *>(parser->data);
            auto &msg = data->parser->_current_message;
            blob read_buf = data->reader->_buffer;

            // set http body
            msg->buffers[1].assign(read_buf.buffer(), at - read_buf.buffer_ptr(), length);
            msg->header->body_length = length;
            return 0;
        };

        auto nparsed = http_parser_execute(
            &_parser, &_parser_setting, reader->_buffer.data(), reader->_buffer_occupied);

        // error handling
        if (_parser.http_errno != HPE_OK) {
            auto err = HTTP_PARSER_ERRNO(&_parser);
            LOG_ERROR("failed on stage %s [%s]",
                      http_parser_stage_to_string(_stage),
                      http_errno_description(err));

            read_next = -1;
            return nullptr;
        }

        _parsed_length += nparsed;
        if (is_complete()) {
            // parsing complete
            reader->_buffer = reader->_buffer.range(_parsed_length);
            reader->_buffer_occupied -= _parsed_length;
            reset();
        }
    }

    if (!_received_messages.empty()) {
        std::unique_ptr<message_ex> msg = std::move(_received_messages.front());
        _received_messages.pop();
        msg->hdr_format = NET_HDR_HTTP;
        return msg.release();
    } else {
        return nullptr;
    }
}

void http_message_parser::prepare_on_send(message_ex *msg)
{
    const message_header *header = msg->header;
    std::vector<blob> &buffers = msg->buffers;

    CHECK(!header->context.u.is_request, "send response only");

    unsigned int dsn_size = sizeof(message_header) + header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < buffers.size()) {
        blob &buf = buffers[dsn_buf_count];
        CHECK_GE(dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;
    }
    CHECK_EQ(dsn_size, 0);

    buffers.resize(dsn_buf_count);
}

int http_message_parser::get_buffers_on_send(message_ex *msg, send_buf *buffers)
{
    // we must skip the message header
    unsigned int offset = sizeof(message_header);
    int i = 0;
    for (blob &buf : msg->buffers) {
        if (offset >= buf.length()) {
            offset -= buf.length();
            continue;
        }
        buffers[i].buf = (void *)(buf.data() + offset);
        buffers[i].sz = buf.length() - offset;
        offset = 0;
        ++i;
    }
    return i;
}

void http_message_parser::reset()
{
    _current_message.reset();
    _url.clear();
    _stage = HTTP_INVALID;
    _parsed_length = 0;
}

} // namespace dsn
