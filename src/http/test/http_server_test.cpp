// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "http/http_server.h"
#include "http/http_message_parser.h"
#include "http/builtin_http_calls.h"
#include "http/http_call_registry.h"

namespace dsn {

TEST(http_server, parse_url)
{
    struct test_case
    {
        std::string url;

        error_code err;
        std::string path;
    } tests[] = {
        {"http://127.0.0.1:34601", ERR_OK, ""},
        {"http://127.0.0.1:34601/", ERR_OK, ""},
        {"http://127.0.0.1:34601///", ERR_OK, ""},
        {"http://127.0.0.1:34601/threads", ERR_OK, "threads"},
        {"http://127.0.0.1:34601/threads/?detail", ERR_OK, "threads"},
        {"http://127.0.0.1:34601//pprof/heap/", ERR_OK, "pprof/heap"},
        {"http://127.0.0.1:34601//pprof///heap?detailed=true", ERR_OK, "pprof/heap"},
        {"http://127.0.0.1:34601/pprof/heap/arg/", ERR_OK, "pprof/heap/arg"},
        {"http://127.0.0.1:34601/pprof///heap///arg/", ERR_OK, "pprof/heap/arg"},
    };

    for (auto tt : tests) {
        ref_ptr<message_ex> m = message_ex::create_receive_message_with_standalone_header(
            blob::create_from_bytes("POST"));
        m->buffers.emplace_back(blob::create_from_bytes(std::string(tt.url)));
        m->buffers.resize(HTTP_MSG_BUFFERS_NUM);

        auto res = http_request::parse(m.get());
        if (res.is_ok()) {
            ASSERT_EQ(res.get_value().path, tt.path) << tt.url;
        } else {
            ASSERT_EQ(res.get_error().code(), tt.err);
        }
    }
}

TEST(bultin_http_calls_test, meta_query)
{
    http_request req;
    http_response resp;
    get_recent_start_time_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::ok);

    get_version_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::ok);
}

TEST(bultin_http_calls_test, get_help)
{
    for (const auto &call : http_call_registry::instance().list_all_calls()) {
        http_call_registry::instance().remove(call->path);
    }

    register_http_call("")
        .with_callback(
            [](const http_request &req, http_response &resp) { get_help_handler(req, resp); })
        .with_help("ip:port/");

    http_request req;
    http_response resp;
    get_help_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::ok);
    ASSERT_EQ(resp.body, "{\"/\":\"ip:port/\"}\n");

    register_http_call("recentStartTime")
        .with_callback([](const http_request &req, http_response &resp) {
            get_recent_start_time_handler(req, resp);
        })
        .with_help("ip:port/recentStartTime");

    get_help_handler(req, resp);
    ASSERT_EQ(resp.body, "{\"/\":\"ip:port/\",\"/recentStartTime\":\"ip:port/recentStartTime\"}\n");

    for (const auto &call : http_call_registry::instance().list_all_calls()) {
        http_call_registry::instance().remove(call->path);
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
            ASSERT_EQ(msg->buffers.size(), HTTP_MSG_BUFFERS_NUM);
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
    std::string http_request = "POST /path/file.html?sdfsdf=sdfs&sldf1=sdf HTTP/1.1\r\n"
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
    ASSERT_EQ(msg->buffers.size(), HTTP_MSG_BUFFERS_NUM);
    ASSERT_EQ(msg->buffers[1].to_string(), "Message Body sdfsdf"); // body
    ASSERT_EQ(                                                     // url
        msg->buffers[2].to_string(),
        std::string("/path/file.html?sdfsdf=sdfs&sldf1=sdf"));
    ASSERT_EQ(msg->buffers[3].to_string(), std::string("json"));
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
    ASSERT_EQ(msg->buffers.size(), HTTP_MSG_BUFFERS_NUM);
    ASSERT_EQ(msg->buffers[1].to_string(), ""); // body
    ASSERT_EQ(                                  // url
        msg->buffers[2].to_string(),
        std::string("/CloudApiControl/HttpServer/telematics/v3/"
                    "weather?location=%E6%B5%B7%E5%8D%97%E7%9C%81%E7%9B%B4%E8%BE%96%E5%8E%BF%E7%BA%"
                    "A7%E8%A1%8C%"
                    "E6%94%BF%E5%8D%95%E4%BD%8D&output=json&ak=0l3FSP6qA0WbOzGRaafbmczS"));
    ASSERT_EQ(msg->buffers[3].to_string(), std::string("application/json;charset=utf8"));
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
    ASSERT_EQ(msg->buffers.size(), HTTP_MSG_BUFFERS_NUM);
    ASSERT_EQ(msg->buffers[2].size(), 4097); // url
}

TEST_F(http_message_parser_test, parse_query_params)
{
    struct test_case
    {
        std::string url;

        error_code err;
        std::unordered_map<std::string, std::string> result;
    } tests[] = {
        {"http://127.0.0.1:34601?query1", ERR_OK, {{"query1", ""}}},
        {"http://127.0.0.1:34601?query1=", ERR_OK, {{"query1", ""}}},
        {"http://127.0.0.1:34601?query1=value1", ERR_OK, {{"query1", "value1"}}},
        {"http://127.0.0.1:34601?=", ERR_OK, {{"", ""}}},
        {"http://127.0.0.1:34601?", ERR_OK, {}},

        {"http://127.0.0.1:34601?query1=value1&query2=value2",
         ERR_OK,
         {{"query1", "value1"}, {"query2", "value2"}}},

        {"http://127.0.0.1:34601?query1=value1&query2",
         ERR_OK,
         {{"query1", "value1"}, {"query2", ""}}},
    };

    for (auto tt : tests) {
        ref_ptr<message_ex> m = message_ex::create_receive_message_with_standalone_header(
            blob::create_from_bytes("POST"));
        m->buffers.emplace_back(blob::create_from_bytes(std::string(tt.url)));
        m->buffers.resize(HTTP_MSG_BUFFERS_NUM);

        auto res = http_request::parse(m.get());
        if (res.is_ok()) {
            ASSERT_EQ(res.get_value().query_args, tt.result) << tt.url;
        } else {
            ASSERT_EQ(res.get_error().code(), tt.err);
        }
    }
}

} // namespace dsn
