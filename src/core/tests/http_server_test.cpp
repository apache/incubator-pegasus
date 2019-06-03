// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/http_server.h>
#include <gtest/gtest.h>

#include "core/tools/http/http_message_parser.h"

namespace dsn {

TEST(http_server, parse_url)
{
    struct test_case
    {
        std::string url;

        error_code err;
        std::pair<std::string, std::string> result;
    } tests[] = {
        {"http://127.0.0.1:34601", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601/", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601///", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601/threads", ERR_OK, {"threads", ""}},
        {"http://127.0.0.1:34601/threads/", ERR_OK, {"threads", ""}},
        {"http://127.0.0.1:34601//pprof/heap/", ERR_OK, {"pprof", "heap"}},
        {"http://127.0.0.1:34601//pprof///heap", ERR_OK, {"pprof", "heap"}},
        {"http://127.0.0.1:34601/pprof/heap/arg/", ERR_INVALID_PARAMETERS, {}},
    };

    for (auto tt : tests) {
        ref_ptr<message_ex> m = message_ex::create_receive_message_with_standalone_header(
            blob::create_from_bytes(std::string("POST")));
        m->buffers.emplace_back(blob::create_from_bytes(std::string(tt.url)));

        auto res = http_request::parse(m.get());
        if (res.is_ok()) {
            ASSERT_EQ(res.get_value().service_method, tt.result) << tt.url;
        } else {
            ASSERT_EQ(res.get_error().code(), tt.err);
        }
    }
}

class http_message_parser_test : public testing::Test
{
public:
    void parse_bad_request()
    {
        // not complete in normal way
        std::string http_request = "GET "
                                   "/ HTTP/1.1\r\n";

        message_reader reader(64);
        char *buf = reader.read_buffer_ptr(http_request.size());
        memcpy(buf, http_request.data(), http_request.size());
        reader.mark_read(http_request.size());

        http_message_parser parser;
        int read_next = 0;
        message_ex *msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(parser._stage, HTTP_ON_URL); // url parsed
        ASSERT_EQ(parser._parsed_length, http_request.size());
        ASSERT_EQ(parser._url, "/");
        ASSERT_NE(parser._current_message, nullptr);
        ASSERT_NE(read_next, -1);

        // normal request
        const char http_request2[] = "GET / HTTP/1.1\r\n"
                                     "Host: baidu.com\r\n"
                                     "Accept: */*\r\n"
                                     "\r\n";
        buf = reader.read_buffer_ptr(sizeof(http_request2));
        memcpy(buf, http_request2, sizeof(http_request2));
        reader.mark_read(sizeof(http_request2));

        msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(read_next, -1);
    }

    void parse_multiple_requests()
    {
        std::string http_request = std::string("GET /") + " HTTP/1.1\r\n\r\n";
        std::string requests;
        for (int i = 0; i < 100; i++) {
            requests += http_request;
        }

        message_reader reader(64);
        char *buf = reader.read_buffer_ptr(requests.size());
        memcpy(buf, requests.data(), requests.size());
        reader.mark_read(requests.size());

        http_message_parser parser;
        int read_next = 0;

        for (int i = 0; i < 100; i++) {
            message_ptr msg = parser.get_message_on_receive(&reader, read_next);
            ASSERT_NE(msg, nullptr);
            ASSERT_EQ(msg->hdr_format, NET_HDR_HTTP);
            ASSERT_EQ(msg->header->hdr_type, http_method::HTTP_METHOD_GET);
            ASSERT_EQ(msg->header->context.u.is_request, 1);
            ASSERT_EQ(msg->buffers.size(), 3);
            ASSERT_EQ(msg->buffers[2].size(), 1); // url

            // ensure states are reset
            ASSERT_EQ(parser._current_message, nullptr);
            ASSERT_EQ(parser._stage, HTTP_INVALID);
            ASSERT_EQ(parser._parsed_length, 0);
            ASSERT_EQ(parser._received_messages.size(), 100 - i - 1);
        }

        ASSERT_EQ(parser._received_messages.size(), 0);
    }
};

TEST_F(http_message_parser_test, parse_request)
{
    std::string http_request = "POST /path/file.html?sdfsdf=sdfs&sldf1=sdf HTTP/12.34\r\n"
                               "From: someuser@jmarshall.com\r\n"
                               "User-Agent: HTTPTool/1.0  \r\n" // intended ending spaces
                               "Content-Type: json\r\n"
                               "Content-Length: 19\r\n"
                               "Log-ID: 456\r\n"
                               "Host: myhost\r\n"
                               "Correlation-ID: 123\r\n"
                               "Authorization: test\r\n"
                               "Accept: */*\r\n"
                               "\r\n"
                               "Message Body sdfsdf\r\n";

    message_reader reader(64);
    char *buf = reader.read_buffer_ptr(http_request.size());
    memcpy(buf, http_request.data(), http_request.size());
    reader.mark_read(http_request.size());

    http_message_parser parser;
    int read_next = 0;
    message_ptr msg = parser.get_message_on_receive(&reader, read_next);
    ASSERT_NE(msg, nullptr);

    ASSERT_EQ(msg->hdr_format, NET_HDR_HTTP);
    ASSERT_EQ(msg->header->hdr_type, http_method::HTTP_METHOD_POST);
    ASSERT_EQ(msg->header->context.u.is_request, 1);
    ASSERT_EQ(msg->buffers.size(), 3);
    ASSERT_EQ(msg->buffers[1].to_string(), "Message Body sdfsdf"); // body
    ASSERT_EQ(                                                     // url
        msg->buffers[2].to_string(),
        std::string("/path/file.html?sdfsdf=sdfs&sldf1=sdf"));
}

TEST_F(http_message_parser_test, eof)
{
    std::string http_request =
        "GET "
        "/CloudApiControl/HttpServer/telematics/v3/"
        "weather?location=%E6%B5%B7%E5%8D%97%E7%9C%81%E7%9B%B4%E8%BE%96%E5%8E%BF%E7%BA%A7%E8%A1%8C%"
        "E6%94%BF%E5%8D%95%E4%BD%8D&output=json&ak=0l3FSP6qA0WbOzGRaafbmczS HTTP/1.1\r\n"
        "X-Host: api.map.baidu.com\r\n"
        "X-Forwarded-Proto: http\r\n"
        "Host: api.map.baidu.com\r\n"
        "User-Agent: IME/Android/4.4.2/N80.QHD.LT.X10.V3/N80.QHD.LT.X10.V3.20150812.031915\r\n"
        "Accept: application/json\r\n"
        "Accept-Charset: UTF-8,*;q=0.5\r\n"
        "Accept-Encoding: deflate,sdch\r\n"
        "Accept-Language: zh-CN,en-US;q=0.8,zh;q=0.6\r\n"
        "Bfe-Atk: NORMAL_BROWSER\r\n"
        "Bfe_logid: 8767802212038413243\r\n"
        "Bfeip: 10.26.124.40\r\n"
        "CLIENTIP: 119.29.102.26\r\n"
        "CLIENTPORT: 59863\r\n"
        "Cache-Control: max-age=0\r\n"
        "Content-Type: application/json;charset=utf8\r\n"
        "X-Forwarded-For: 119.29.102.26\r\n"
        "X-Forwarded-Port: 59863\r\n"
        "X-Ime-Imei: 35629601890905\r\n"
        "X_BD_LOGID: 3959476981\r\n"
        "X_BD_LOGID64: 16815814797661447369\r\n"
        "X_BD_PRODUCT: map\r\n"
        "X_BD_SUBSYS: apimap\r\n\r\n";

    message_reader reader(64);
    char *buf = reader.read_buffer_ptr(http_request.size());
    memcpy(buf, http_request.data(), http_request.size());
    reader.mark_read(http_request.size());

    http_message_parser parser;
    int read_next = 0;
    message_ptr msg = parser.get_message_on_receive(&reader, read_next);
    ASSERT_NE(msg, nullptr);

    ASSERT_EQ(msg->hdr_format, NET_HDR_HTTP);
    ASSERT_EQ(msg->header->hdr_type, http_method::HTTP_METHOD_GET);
    ASSERT_EQ(msg->header->context.u.is_request, 1);
    ASSERT_EQ(msg->buffers.size(), 3);
    ASSERT_EQ(msg->buffers[1].to_string(), ""); // body
    ASSERT_EQ(                                  // url
        msg->buffers[2].to_string(),
        std::string("/CloudApiControl/HttpServer/telematics/v3/"
                    "weather?location=%E6%B5%B7%E5%8D%97%E7%9C%81%E7%9B%B4%E8%BE%96%E5%8E%BF%E7%BA%"
                    "A7%E8%A1%8C%"
                    "E6%94%BF%E5%8D%95%E4%BD%8D&output=json&ak=0l3FSP6qA0WbOzGRaafbmczS"));
}

TEST_F(http_message_parser_test, parse_bad_request) { parse_bad_request(); }

TEST_F(http_message_parser_test, parse_multiple_requests) { parse_multiple_requests(); }

TEST_F(http_message_parser_test, parse_long_url)
{
    std::string http_request = "GET /" + std::string(4096, 'a') + " HTTP/1.1\r\n\r\n";

    message_reader reader(64);
    char *buf = reader.read_buffer_ptr(http_request.size());
    memcpy(buf, http_request.data(), http_request.size());
    reader.mark_read(http_request.size());

    http_message_parser parser;
    int read_next = 0;
    message_ptr msg = parser.get_message_on_receive(&reader, read_next);
    ASSERT_NE(msg, nullptr);
    ASSERT_EQ(msg->hdr_format, NET_HDR_HTTP);
    ASSERT_EQ(msg->header->hdr_type, http_method::HTTP_METHOD_GET);
    ASSERT_EQ(msg->header->context.u.is_request, 1);
    ASSERT_EQ(msg->buffers.size(), 3);
    ASSERT_EQ(msg->buffers[2].size(), 4097); // url
}

} // namespace dsn
