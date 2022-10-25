/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <algorithm>
#include <memory>
#include <string>
#include <boost/asio.hpp>

#include "utils/string_conv.h"
#include "utils/rand.h"

#include <gtest/gtest.h>
#include <rrdb/rrdb.client.h>
#include <pegasus_utils.h>
#include "proxy_layer.h"
#include "redis_parser.h"

using namespace boost::asio;
using namespace ::pegasus::proxy;

class proxy_test_app : public ::dsn::service_app
{
public:
    explicit proxy_test_app(const dsn::service_app_info *info) : service_app(info) {}

    ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        if (args.size() < 3) {
            return ::dsn::ERR_INVALID_PARAMETERS;
        }

        proxy_session::factory f = [](proxy_stub *p, dsn::message_ex *m) {
            return std::make_shared<redis_parser>(p, m);
        };
        _proxy = dsn::make_unique<proxy_stub>(f, args[1].c_str(), args[2].c_str());
        return ::dsn::ERR_OK;
    }
    ::dsn::error_code stop(bool) override { return ::dsn::ERR_OK; }

private:
    std::unique_ptr<pegasus::proxy::proxy_stub> _proxy;
};

class redis_test_parser : public redis_parser
{
public:
    redis_test_parser(proxy_stub *stub, dsn::message_ex *msg) : redis_parser(stub, msg)
    {
        _reserved_entry.reserve(20);
        for (int i = 0; i < 20; ++i) {
            _reserved_entry.emplace_back(new message_entry());
        }
        _entry_index = 0;
        _got_a_message = false;
    }

    void reset()
    {
        _got_a_message = false;
        _entry_index = 0;
    }

    void set_msg(int index, redis_request msg)
    {
        _reserved_entry[index]->request.sub_request_count = msg.sub_request_count;
        _reserved_entry[index]->request.sub_requests = std::move(msg.sub_requests);
    }

    static dsn::message_ex *create_message(const char *data)
    {
        return dsn::message_ex::create_received_request(
            RPC_CALL_RAW_MESSAGE, dsn::DSF_THRIFT_BINARY, (void *)data, strlen(data));
    }

    static dsn::message_ex *create_message(const char *data, int length)
    {
        return dsn::message_ex::create_received_request(
            RPC_CALL_RAW_MESSAGE, dsn::DSF_THRIFT_BINARY, (void *)data, length);
    }

    static dsn::message_ex *marshalling_array(const redis_request &request)
    {
        dsn::message_ex *msg = create_message("dummy");

        dsn::message_ex *resp = msg->create_response();
        ::dsn::rpc_write_stream stream(resp);

        stream.write_pod('*');
        std::string count_str = std::to_string(request.sub_request_count);
        stream.write(count_str.c_str(), count_str.length());
        stream.write_pod(CR);
        stream.write_pod(LF);
        for (const auto &sub_request : request.sub_requests) {
            sub_request.marshalling(stream);
        }

        msg->release_ref();
        return resp;
    }

protected:
    void handle_command(std::unique_ptr<message_entry> &&entry) override
    {
        redis_request &act_request = entry->request;
        redis_request &exp_request = _reserved_entry[_entry_index]->request;

        ASSERT_TRUE(act_request.sub_request_count > 0);
        ASSERT_EQ(act_request.sub_request_count, exp_request.sub_request_count);
        for (unsigned int i = 0; i < act_request.sub_request_count; ++i) {
            redis_bulk_string &bs1 = act_request.sub_requests[i];
            redis_bulk_string &bs2 = exp_request.sub_requests[i];
            ASSERT_EQ(bs1.length, bs2.length);
            if (bs1.length > 0) {
                ASSERT_EQ(bs1.data.length(), bs2.data.length());
                ASSERT_EQ(0, memcmp(bs1.data.data(), bs2.data.data(), bs2.data.length()));
            }
        }

        _got_a_message = true;
        ++_entry_index;
    }

private:
    friend class proxy_test;
    FRIEND_TEST(proxy_test, test_simple_cases);
    FRIEND_TEST(proxy_test, test_segmented_cases);
    FRIEND_TEST(proxy_test, test_georadius);
    FRIEND_TEST(proxy_test, test_georadiusbymember);
    FRIEND_TEST(proxy_test, test_geopos);
    FRIEND_TEST(proxy_test, test_error_message);
    FRIEND_TEST(proxy_test, test_nil_bulk_string);
    FRIEND_TEST(proxy_test, test_random_cases);
    FRIEND_TEST(proxy_test, test_parse_parameters);

    std::vector<std::unique_ptr<message_entry>> _reserved_entry;
    int _entry_index;
    bool _got_a_message;
};

class proxy_test : public ::testing::Test
{
public:
    proxy_test()
    {
        dsn::message_ex *msg = dsn::message_ex::create_received_request(
            RPC_CALL_RAW_MESSAGE, dsn::DSF_THRIFT_BINARY, nullptr, 0);
        msg->header->from_address = dsn::rpc_address("127.0.0.1", 123);
        _parser.reset(new redis_test_parser(nullptr, msg));
    }

    void SetUp() override { _parser->reset(); }

    void reset() { _parser->reset(); }
    void set_msg(int index, redis_parser::redis_request msg)
    {
        _parser->set_msg(index, std::move(msg));
    }
    bool parse(dsn::message_ex *msg) { return _parser->parse(msg); }
    bool got_message() { return _parser->_got_a_message; }
    int parsed_entry_count() { return _parser->_entry_index; }

private:
    std::shared_ptr<redis_test_parser> _parser;
};

TEST_F(proxy_test, test_simple_cases)
{
    set_msg(0, redis_test_parser::redis_request(3, {{"SET"}, {"foo"}, {"bar"}}));
    const char *request_data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto request = redis_test_parser::create_message(request_data);
    ASSERT_TRUE(parse(request));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_segmented_cases)
{
    set_msg(0, redis_test_parser::redis_request(3, {{"SET"}, {"foo"}, {"bar"}}));
    const char *request_data1 = "*3\r\n$3\r\nSET\r\n$3\r";
    const char *request_data2 = "\nfoo\r\n$3\r\nbar\r\n";
    auto request1 = redis_test_parser::create_message(request_data1);
    auto request2 = redis_test_parser::create_message(request_data2);
    ASSERT_TRUE(parse(request1));
    ASSERT_TRUE(parse(request2));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_georadius)
{
    set_msg(0,
            redis_test_parser::redis_request(
                6, {{"GEORADIUS"}, {""}, {"123.4"}, {"56.78"}, {"100"}, {"m"}}));
    const char *request_data = "*6\r\n$9\r\nGEORADIUS\r\n$0\r\n\r\n$5\r\n123.4\r\n$5\r\n56."
                               "78\r\n$3\r\n100\r\n$1\r\nm\r\n";
    auto request = redis_test_parser::create_message(request_data);
    ASSERT_TRUE(parse(request));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_georadiusbymember)
{
    set_msg(0,
            redis_test_parser::redis_request(
                5, {{"GEORADIUSBYMEMBER"}, {""}, {"member1"}, {"1000.5"}, {"km"}}));
    const char *request_data = "*5\r\n$17\r\nGEORADIUSBYMEMBER\r\n$0\r\n\r\n$"
                               "7\r\nmember1\r\n$6\r\n1000.5\r\n$2\r\nkm\r\n";
    auto request = redis_test_parser::create_message(request_data);
    ASSERT_TRUE(parse(request));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_geopos)
{
    set_msg(0,
            redis_test_parser::redis_request(
                5, {{"GEOPOS"}, {""}, {"member1"}, {"member2"}, {"member3"}}));
    const char *request_data = "*5\r\n$6\r\nGEOPOS\r\n$0\r\n\r\n$"
                               "7\r\nmember1\r\n$7\r\nmember2\r\n$7\r\nmember3\r\n";
    auto request = redis_test_parser::create_message(request_data);
    ASSERT_TRUE(parse(request));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_error_message)
{
    const char *bad_data[] = {"$1\r\n$1\r\nt\r\n",
                              "*1\r$5\r\ntest_\r\n",
                              "*hello\r\n$1\r\nt\r\n",
                              "*-23\r\n$1\r\nt\r\n",
                              "*1\r\n12\r\ntest_command\r\n",
                              "*1\r\n$12test_command\r\n",
                              "*1\r\n$12\rtest_command\r\n",
                              "*2\r\n$3\r\nget\r\n*6\r\nkeykey\r\n",
                              "*2\r\n$3\r\nget\r\n$6\rkeykey\r\n",
                              nullptr};

    for (unsigned int i = 0; bad_data[i]; ++i) {
        auto request = redis_test_parser::create_message(bad_data[i]);
        ASSERT_FALSE(parse(request));
        ASSERT_FALSE(got_message());
    }

    // after wrong message, parser should be reset
    reset(); // TODO should remove?
    set_msg(0, redis_test_parser::redis_request(3, {{"set"}, {"hello"}, {""}}));
    const char *good_data = "*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$0\r\n\r\n";
    auto request = redis_test_parser::create_message(good_data);
    ASSERT_TRUE(parse(request));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_nil_bulk_string)
{
    set_msg(0, redis_test_parser::redis_request(1, {{redis_test_parser::redis_bulk_string()}}));
    const char *data = "*1\r\n$-1\r\n";
    ASSERT_TRUE(parse(redis_test_parser::create_message(data)));
    ASSERT_TRUE(got_message());
}

TEST_F(proxy_test, test_random_cases)
{
    int total_requests = 10;
    std::vector<dsn::message_ex *> fake_requests;
    int total_body_size = 0;

    // create several requests
    for (int i = 0; i < total_requests; ++i) {
        int sub_request_count = dsn::rand::next_u32(1, 20);
        std::vector<redis_test_parser::redis_bulk_string> sub_requests(sub_request_count);
        for (auto &sub_request : sub_requests) {
            sub_request.length = dsn::rand::next_u32(0, 8);
            if (sub_request.length == 0) {
                sub_request.length = -1;
            } else if (sub_request.length == 1) {
                sub_request.length = 0;
            } else {
                sub_request.length = dsn::rand::next_u32(1, 256);
                std::shared_ptr<char> raw_buf(new char[sub_request.length],
                                              std::default_delete<char[]>());
                memset(raw_buf.get(), 't', sub_request.length);
                sub_request.data.assign(std::move(raw_buf), 0, sub_request.length);
            }
        }

        redis_test_parser::redis_request request(sub_request_count, std::move(sub_requests));
        set_msg(i, request);
        dsn::message_ex *fake_response = redis_test_parser::marshalling_array(request);
        dsn::message_ex *fake_request = fake_response->copy(true, true);

        fake_response->add_ref();
        fake_response->release_ref();

        fake_requests.push_back(fake_request);
        total_body_size += fake_request->body_size();
    }

    // let's copy the messages
    std::shared_ptr<char> msg_buffer(new char[total_body_size + 10], std::default_delete<char[]>());
    char *msg_buffer_ptr = msg_buffer.get();

    for (dsn::message_ex *fake_request : fake_requests) {
        void *rw_ptr;
        size_t length;
        while (fake_request->read_next(&rw_ptr, &length)) {
            memcpy(msg_buffer_ptr, rw_ptr, length);
            msg_buffer_ptr += length;
            fake_request->read_commit(length);
        }
        fake_request->add_ref();
        fake_request->release_ref();
    }
    *msg_buffer_ptr = 0;

    ASSERT_EQ(msg_buffer_ptr - msg_buffer.get(), total_body_size);

    msg_buffer_ptr = msg_buffer.get();
    // first create a big message, test the pipeline
    {
        reset();
        dsn::message_ex *msg = redis_test_parser::create_message(msg_buffer_ptr, total_body_size);
        ASSERT_TRUE(parse(msg));
        ASSERT_EQ(parsed_entry_count(), total_requests);
    }

    // let's split the messages into different pieces
    {
        reset();
        size_t slice_count = dsn::rand::next_u32(total_requests, total_body_size);
        std::set<int> offsets;
        while (offsets.size() < slice_count - 1) {
            offsets.insert(dsn::rand::next_u32(1, total_body_size - 1));
        }
        offsets.insert(total_body_size);

        int last_offset = 0;
        for (int offset : offsets) {
            dsn::message_ex *msg = redis_test_parser::create_message(msg_buffer_ptr + last_offset,
                                                                     offset - last_offset);
            ASSERT_TRUE(parse(msg));
            last_offset = offset;
        }
        ASSERT_EQ(parsed_entry_count(), total_requests);
    }
}

TEST_F(proxy_test, test_parse_parameters)
{
    double radius_m = 0;
    std::string unit;
    pegasus::geo::geo_client::SortType sort_type = pegasus::geo::geo_client::SortType::random;
    int count = 0;
    bool WITHCOORD = false;
    bool WITHDIST = false;
    bool WITHVALUE = false;

    {
        radius_m = 0;
        sort_type = pegasus::geo::geo_client::SortType::random;
        count = 0;
        WITHCOORD = false;
        WITHDIST = false;
        WITHVALUE = false;
        std::vector<redis_test_parser::redis_bulk_string> opts({{"GEORADIUS"},
                                                                {""},
                                                                {"12.3"},
                                                                {"45.6"},
                                                                {"100"},
                                                                {"m"},
                                                                {"WITHCOORD"},
                                                                {"WITHDIST"},
                                                                {"WITHHASH"},
                                                                {"COUNT"},
                                                                {"-1"},
                                                                {"ASC"},
                                                                {"WITHVALUE"}});

        redis_test_parser::parse_geo_radius_parameters(
            opts, 4, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHVALUE);

        ASSERT_DOUBLE_EQ(radius_m, 100);
        ASSERT_EQ(unit, "m");
        ASSERT_EQ(sort_type, pegasus::geo::geo_client::SortType::asc);
        ASSERT_EQ(count, -1);
        ASSERT_TRUE(WITHCOORD);
        ASSERT_TRUE(WITHDIST);
        ASSERT_TRUE(WITHVALUE);
    }

    {
        radius_m = 0;
        sort_type = pegasus::geo::geo_client::SortType::random;
        count = 0;
        WITHCOORD = false;
        WITHDIST = false;
        WITHVALUE = false;
        std::vector<redis_test_parser::redis_bulk_string> opts({{"GEORADIUS"},
                                                                {""},
                                                                {"12.3"},
                                                                {"45.6"},
                                                                {"100.23"},
                                                                {"km"},
                                                                {"COUNT"},
                                                                {"500"},
                                                                {"DESC"}});

        redis_test_parser::parse_geo_radius_parameters(
            opts, 4, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHVALUE);

        ASSERT_DOUBLE_EQ(radius_m, 100230);
        ASSERT_EQ(unit, "km");
        ASSERT_EQ(sort_type, pegasus::geo::geo_client::SortType::desc);
        ASSERT_EQ(count, 500);
        ASSERT_FALSE(WITHCOORD);
        ASSERT_FALSE(WITHDIST);
        ASSERT_FALSE(WITHVALUE);
    }

    {
        radius_m = 0;
        sort_type = pegasus::geo::geo_client::SortType::random;
        count = 0;
        WITHCOORD = false;
        WITHDIST = false;
        WITHVALUE = false;
        std::vector<redis_test_parser::redis_bulk_string> opts({{"GEORADIUSBYMEMBER"},
                                                                {""},
                                                                {"somekey"},
                                                                {"100"},
                                                                {"m"},
                                                                {"WITHCOORD"},
                                                                {"WITHDIST"},
                                                                {"WITHHASH"},
                                                                {"COUNT"},
                                                                {"-1"},
                                                                {"ASC"},
                                                                {"WITHVALUE"}});

        redis_test_parser::parse_geo_radius_parameters(
            opts, 3, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHVALUE);

        ASSERT_DOUBLE_EQ(radius_m, 100);
        ASSERT_EQ(unit, "m");
        ASSERT_EQ(sort_type, pegasus::geo::geo_client::SortType::asc);
        ASSERT_EQ(count, -1);
        ASSERT_TRUE(WITHCOORD);
        ASSERT_TRUE(WITHDIST);
        ASSERT_TRUE(WITHVALUE);
    }

    {
        radius_m = 0;
        sort_type = pegasus::geo::geo_client::SortType::random;
        count = 0;
        WITHCOORD = false;
        WITHDIST = false;
        WITHVALUE = false;
        std::vector<redis_test_parser::redis_bulk_string> opts({{"GEORADIUSBYMEMBER"},
                                                                {""},
                                                                {"somekey"},
                                                                {"100.23"},
                                                                {"km"},
                                                                {"COUNT"},
                                                                {"500"},
                                                                {"DESC"}});

        redis_test_parser::parse_geo_radius_parameters(
            opts, 3, radius_m, unit, sort_type, count, WITHCOORD, WITHDIST, WITHVALUE);

        ASSERT_DOUBLE_EQ(radius_m, 100230);
        ASSERT_EQ(unit, "km");
        ASSERT_EQ(sort_type, pegasus::geo::geo_client::SortType::desc);
        ASSERT_EQ(count, 500);
        ASSERT_FALSE(WITHCOORD);
        ASSERT_FALSE(WITHDIST);
        ASSERT_FALSE(WITHVALUE);
    }

    {
        int ttl_seconds = 0;
        std::vector<redis_test_parser::redis_bulk_string> opts(
            {{"SET"}, {"KK"}, {"vv"}, {"EX"}, {"123"}});
        redis_test_parser::parse_set_parameters(opts, ttl_seconds);
        ASSERT_EQ(ttl_seconds, 123);
    }
}

TEST(proxy, connection)
{
    ::dsn::rpc_address redis_address("127.0.0.1", 12345);
    ip::tcp::endpoint redis_endpoint(ip::address_v4(redis_address.ip()), redis_address.port());

    io_service ios;
    ip::tcp::socket client_socket(ios);
    client_socket.open(ip::tcp::v4());
    boost::system::error_code ec;
    ASSERT_TRUE(!client_socket.connect(redis_endpoint, ec));

    char got_reply[1024];
    // basic pipeline
    {
        const char *reqs = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbar1\r\n"
                           "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbar2\r\n"
                           "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbar3\r\n";

        boost::asio::write(client_socket, boost::asio::buffer(reqs, strlen(reqs)));

        const char *resps = "+OK\r\n"
                            "+OK\r\n"
                            "+OK\r\n";
        size_t got_length =
            boost::asio::read(client_socket, boost::asio::buffer(got_reply, strlen(resps)));
        got_reply[got_length] = 0;
        ASSERT_STREQ(resps, got_reply);
    }

    // then let's get the value
    {
        const char *req = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        boost::asio::write(client_socket, boost::asio::buffer(req, strlen(req)));

        const char *resp = "$4\r\nbar3\r\n";
        size_t got_length =
            boost::asio::read(client_socket, boost::asio::buffer(got_reply, strlen(resp)));
        got_reply[got_length] = 0;
        ASSERT_STREQ(resp, got_reply);
    }

    // then ttl test of set key
    {
        const char *req = "*4\r\n$5\r\nSETEX\r\n$3\r\nfo1\r\n$1\r\n2\r\n$3\r\nbar\r\n"
                          "*4\r\n$5\r\nSETEX\r\n$3\r\nfo2\r\n$4\r\n9999\r\n$3\r\nbar\r\n"
                          "*3\r\n$3\r\nSET\r\n$3\r\nfo3\r\n$3\r\nbar\r\n";
        boost::asio::write(client_socket, boost::asio::buffer(req, strlen(req)));

        const char *resps = "+OK\r\n"
                            "+OK\r\n"
                            "+OK\r\n";
        size_t got_length =
            boost::asio::read(client_socket, boost::asio::buffer(got_reply, strlen(resps)));
        got_reply[got_length] = 0;
        ASSERT_STREQ(resps, got_reply);
    }

    {
        const char *req = "*2\r\n$3\r\nTTL\r\n$3\r\nfo2\r\n"
                          "*2\r\n$3\r\nTTL\r\n$3\r\nfo3\r\n"
                          "*2\r\n$3\r\nTTL\r\n$3\r\nfo4\r\n";
        boost::asio::write(client_socket, boost::asio::buffer(req, strlen(req)));

        const char *resps1 = ":9999\r\n"
                             ":-1\r\n"
                             ":-2\r\n";
        const char *resps2 = ":9998\r\n"
                             ":-1\r\n"
                             ":-2\r\n";
        size_t got_length =
            boost::asio::read(client_socket, boost::asio::buffer(got_reply, strlen(resps1)));
        got_reply[got_length] = 0;
        ASSERT_EQ(got_length, strlen(resps1));
        ASSERT_TRUE(strncmp(got_reply, resps1, got_length) == 0 ||
                    strncmp(got_reply, resps2, got_length) == 0);
    }

    {
        std::this_thread::sleep_for(std::chrono::milliseconds(2100));
        const char *req = "*2\r\n$3\r\nGET\r\n$3\r\nfo1\r\n"
                          "*2\r\n$3\r\nGET\r\n$3\r\nfo2\r\n"
                          "*2\r\n$3\r\nGET\r\n$3\r\nfo3\r\n";
        boost::asio::write(client_socket, boost::asio::buffer(req, strlen(req)));

        const char *resps = "$-1\r\n"
                            "$3\r\nbar\r\n"
                            "$3\r\nbar\r\n";
        size_t got_length =
            boost::asio::read(client_socket, boost::asio::buffer(got_reply, strlen(resps)));
        got_reply[got_length] = 0;
        ASSERT_STREQ(resps, got_reply);
    }

    // let's send partitial message then close the socket
    {
        const char *req = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbar1\r\n"
                          "*3\r\n$3\r\nSET\r\n$3\r\nfo";
        boost::asio::write(client_socket, boost::asio::buffer(req, strlen(req)));

        try {
            client_socket.shutdown(boost::asio::socket_base::shutdown_both);
            client_socket.close();
        } catch (...) {
            LOG_INFO("exception in shutdown");
        }
    }
}

void dsn_init()
{
    dsn::service_app::register_factory<proxy_test_app>("proxy");
    dsn_run_config("config.ini", false);
}

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    dsn_init();
    int ret = RUN_ALL_TESTS();
    dsn_exit(ret);
}
