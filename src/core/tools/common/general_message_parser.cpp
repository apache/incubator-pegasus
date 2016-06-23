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
 *     What is this file about?
 *
 * Revision history:
 *     Jun. 2016, Zuoyan Qin, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "general_message_parser.h"
# include <dsn/service_api_c.h>
# include <dsn/cpp/serialization_helper/thrift_helper.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "general.message.parser"

namespace dsn
{
    general_message_parser::general_message_parser(int buffer_block_size, bool is_write_only)
        : message_parser(buffer_block_size, is_write_only),
          _header_parsed(false), _header_checked(false)
    {
    }

    message_ex* general_message_parser::get_message_on_receive(unsigned int read_length, /*out*/ int& read_next)
    {
        mark_read(read_length);

        if (_read_buffer_occupied < sizeof(header_type))
        {
            read_next = sizeof(header_type) - _read_buffer_occupied;
            return nullptr;
        }

        header_type hdr_type(_read_buffer.data());

        if (hdr_type == header_type::hdr_dsn_default)
        {
            if (_read_buffer_occupied >= sizeof(message_header))
            {
                if (!_header_checked)
                {
                    if (!message_ex::is_right_header((char*)_read_buffer.data()))
                    {
                        derror("receive message header check failed for message");

                        truncate_read();
                        read_next = -1;
                        return nullptr;
                    }
                    else
                    {
                        _header_checked = true;
                    }
                }

                unsigned int msg_sz = sizeof(message_header) + message_ex::get_body_length((char*)_read_buffer.data());

                // msg done
                if (_read_buffer_occupied >= msg_sz)
                {
                    auto msg_bb = _read_buffer.range(0, msg_sz);
                    message_ex* msg = message_ex::create_receive_message(msg_bb);
                    if (!msg->is_right_body(false))
                    {
                        message_header* header = (message_header*)_read_buffer.data();
                        derror("body check failed for message, id: %d, rpc_name: %s, from: %s",
                              header->id, header->rpc_name, header->from_address.to_string());

                        truncate_read();
                        read_next = -1;
                        return nullptr;
                    }
                    else
                    {
                        _read_buffer = _read_buffer.range(msg_sz);
                        _read_buffer_occupied -= msg_sz;
                        _header_checked = false;

                        read_next = sizeof(message_header);
                        return msg;
                    }
                }
                else
                {
                    read_next = msg_sz - _read_buffer_occupied;
                    return nullptr;
                }
            }

            else
            {
                read_next = sizeof(message_header) - _read_buffer_occupied;
                return nullptr;
            }
        }
        else if (hdr_type == header_type::hdr_dsn_thrift)
        {
            if (_read_buffer_occupied >= sizeof(thrift_message_header))
            {
                if (!_header_parsed)
                {
                    thrift_message_parser::read_thrift_header(_read_buffer.data(), _thrift_header);

                    if (!thrift_message_parser::check_thrift_header(_thrift_header))
                    {
                        derror("check thrift header of version 0 failed");

                        truncate_read();
                        read_next = -1;
                        return nullptr;
                    }
                    else
                    {
                        _header_parsed = true;
                    }
                }

                unsigned int msg_sz = _thrift_header.hdr_length + _thrift_header.body_length;

                // msg done
                if (_read_buffer_occupied >= msg_sz)
                {
                    dsn::blob msg_bb = _read_buffer.range(0, msg_sz);
                    message_ex* msg = thrift_message_parser::parse_message(_thrift_header, msg_bb);

                    _read_buffer = _read_buffer.range(msg_sz);
                    _read_buffer_occupied -= msg_sz;
                    _header_parsed = false;

                    read_next = sizeof(header_type);
                    return msg;
                }
                else
                {
                    read_next = msg_sz - _read_buffer_occupied;
                    return nullptr;
                }
            }
            else
            {
                read_next = sizeof(thrift_message_header) - _read_buffer_occupied;
                return nullptr;
            }
        }
        else
        {
            derror("invalid message header type: %d", hdr_type.type.itype);

            truncate_read();
            read_next = -1;
            return nullptr;
        }
    }

    void general_message_parser::truncate_read()
    {
        message_parser::truncate_read();
        _header_parsed = false;
        _header_checked = false;
    }

    void general_message_parser::on_create_response(message_ex* request_msg, message_ex* response_msg)
    {
        if (request_msg->header->hdr_type == header_type::hdr_dsn_default)
        {
            // do nothing
        }
        else if (request_msg->header->hdr_type == header_type::hdr_dsn_thrift)
        {
            dsn::rpc_write_stream write_stream(response_msg);
            ::dsn::binary_writer_transport trans(write_stream);
            boost::shared_ptr< ::dsn::binary_writer_transport > trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
            ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

            //add message begin for each thrift response, corresponding with thrift_parser::add_post_fix's writeMessageEnd
            msg_proto.writeMessageBegin(request_msg->header->rpc_name, ::apache::thrift::protocol::T_REPLY, request_msg->header->id);
        }
        else
        {
            dassert(false, "not supported message header type: %d", request_msg->header->hdr_type.type.itype);
        }
    }

    int general_message_parser::prepare_on_send(message_ex* msg)
    {
        if (msg->header->hdr_type == header_type::hdr_dsn_default)
        {
            return (int)msg->buffers.size();
        }
        else if (msg->header->hdr_type == header_type::hdr_dsn_thrift)
        {
            dassert(!msg->header->context.u.is_request, "only support send response");

            dsn::rpc_write_stream write_stream(msg);
            ::dsn::binary_writer_transport trans(write_stream);
            boost::shared_ptr< ::dsn::binary_writer_transport > trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
            ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

            msg_proto.writeMessageEnd();
            // as we must add a thrift header for the response, so we'd better reserve one more buffer
            // refer to the get_buffers_on_send
            return (int)msg->buffers.size() + 1;
        }
        else
        {
            dassert(false, "not supported message header type: %d", msg->header->hdr_type.type.itype);
            return 0;
        }
    }

    int general_message_parser::get_buffers_on_send(message_ex* msg, /*out*/ send_buf* buffers)
    {
        if (msg->header->hdr_type == header_type::hdr_dsn_default)
        {
            int i = 0;
            for (auto& buf : msg->buffers)
            {
                buffers[i].buf = (void*)buf.data();
                buffers[i].sz = (size_t)buf.length();
                ++i;
            }
            return i;
        }
        else if (msg->header->hdr_type == header_type::hdr_dsn_thrift)
        {
            dassert(!msg->header->context.u.is_request, "only support send response");
            dassert(msg->header->server.error_name[0], "error name should be set");
            dassert(!msg->buffers.empty(), "buffers can not be empty");

            // response format:
            //     <total_len(int32)> <error_len(int32)> <error_str(bytes)> <body_data(bytes)>
            //    |-----------response header------------------------------|

            int32_t err_len = strlen(msg->header->server.error_name);
            int32_t header_len = sizeof(int32_t) * 2 + err_len;
            int32_t total_len = header_len + msg->header->body_length;

            // construct thrift header blob
            std::shared_ptr<char> header_holder(static_cast<char*>(dsn_transient_malloc(header_len)), [](char* c) {dsn_transient_free(c);});
            char* ptr = header_holder.get();
            *((int32_t*)ptr) = htobe32(total_len);
            ptr += sizeof(int32_t);
            *((int32_t*)ptr) = htobe32(err_len);
            ptr += sizeof(int32_t);
            memcpy(ptr, msg->header->server.error_name, err_len);

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
                buffers[i].sz = (uint32_t)(buf.length() - offset);
                offset = 0;
                ++i;
            }

            // put thrift header blob at the back of message buffer
            msg->buffers.emplace_back(blob(std::move(header_holder), header_len));

            return i;
        }
        else
        {
            dassert(false, "not supported message header type: %d", msg->header->hdr_type.type.itype);
            return 0;
        }
    }
}
