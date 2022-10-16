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

#include "runtime/rpc/message_parser.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/ports.h"
#include "utils/endians.h"
#include <gtest/gtest_prod.h>
#include "common/serialization_helper/dsn.layer2_types.h"

#include "request_meta_types.h"

namespace dsn {

struct request_meta_v0
{
    void clear()
    {
        hdr_crc32 = 0;
        body_length = 0;
        body_crc32 = 0;
        app_id = 0;
        partition_index = 0;
        client_timeout = 0;
        client_thread_hash = 0;
        client_partition_hash = 0;
    }

    uint32_t hdr_crc32 = 0;
    uint32_t body_length = 0;
    uint32_t body_crc32 = 0;
    int32_t app_id = 0;
    int32_t partition_index = 0;
    int32_t client_timeout = 0;
    int32_t client_thread_hash = 0;
    uint64_t client_partition_hash = 0;
};

struct v1_specific_vars
{
    v1_specific_vars() : _meta_v1(new thrift_request_meta_v1) {}

    void clear()
    {
        _meta_v1.reset(new thrift_request_meta_v1);
        _meta_parsed = false;
        _meta_length = 0;
        _body_length = 0;
    }

    bool _meta_parsed{false};
    uint32_t _meta_length{0};
    uint32_t _body_length{0};
    std::unique_ptr<thrift_request_meta_v1> _meta_v1;
};

static const uint32_t THRIFT_HDR_SIG = 0x54464854; //"THFT"

DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_THRIFT)

// Parses request sent in rDSN thrift protocol, which is
// mainly used by our Java/GoLang/NodeJs/Python clients,
// and encodes response to them.
class thrift_message_parser final : public message_parser
{
public:
    thrift_message_parser();

    ~thrift_message_parser() override;

    void reset() override;

    message_ex *get_message_on_receive(message_reader *reader,
                                       /*out*/ int &read_next) override;

    // thrift response format:
    //     <total_len(int32)> <thrift_string> <thrift_message_begin> <body_data(bytes)>
    //     <thrift_message_end>
    void prepare_on_send(message_ex *msg) override;

    int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) override;

private:
    message_ex *parse_request_body_v0(message_reader *reader,
                                      /*out*/ int &read_next);

    message_ex *parse_request_body_v1(message_reader *reader,
                                      /*out*/ int &read_next);

    bool parse_request_header(message_reader *reader, int &read_next);

private:
    friend class thrift_message_parser_test;
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_incomplete_second_field);
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_incomplete_v0_hdr_len);
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_invalid_v0_hdr_length);
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_valid_v0_hdr);
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_incomplete_v1_hdr);
    FRIEND_TEST(thrift_message_parser_test, get_message_on_receive_valid_v1_hdr);

    int _header_version{-1};

    // meta version 1 specific variables
    std::unique_ptr<v1_specific_vars> _v1_specific_vars;

    // meta version 0 specific variables
    std::unique_ptr<request_meta_v0> _meta_v0;
};

} // namespace dsn
