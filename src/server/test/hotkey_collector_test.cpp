// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <server/hotkey_collector.h>

#include <gtest/gtest.h>
#include <stdlib.h>
#include <dsn/utility/rand.h>
#include <dsn/utility/defer.h>
#include <rrdb/rrdb_types.h>
#include <dsn/cpp/serverlet.h>
#include <dsn/utility/error_code.h>

#include "message_utils.h"
#include "pegasus_server_test_base.h"
#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

class hotkey_collector_test : public pegasus_server_test_base
{
public:
    std::string hotkey_generator(bool is_hotkey)
    {
        if (is_hotkey && rand() % 2) {
            return "ThisisahotkeyThisisahotkey";
        } else {
            const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
            const int len = strlen(CCH);
            std::string result = "";
            int index;
            for (int i = 0; i < 20; i++) {
                index = rand() % len;
                result += CCH[index];
            }
            return result;
        }
    }

    hotkey_collector_test() : pegasus_server_test_base() { start(); }

    // test on_get random data
    void read_test_readom_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "STOP");
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "COARSE");
        srand(1);
        ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
        dsn::blob key;
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(false), std::string("sortkeysortkeysortkeysortkey"));
            auto get = [&] { _server->on_get(key, reply); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(LPC_WRITE, nullptr, get);
            t->wait();
            if (i % 25 == 0)
                _server->get_read_hotkey_collector()->analyse_data();
        }
        ASSERT_NE(_server->get_read_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_result(result), false);

        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
    }

    // test on_get one hotkey in random data
    void read_test_hotkey_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));

        dsn::blob key;
        ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "COARSE");
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(true), std::string("sortkeysortkeysortkeysortkey"));
            auto get = [&] { _server->on_get(key, reply); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(LPC_WRITE, nullptr, get);
            t->wait();
            if (i % 25 == 0)
                _server->get_read_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
    }

    // test hotkey_collector off
    void read_test_no_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "STOP");
        ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
        dsn::blob key;
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(false), std::string("sortkeysortkeysortkeysortkey"));
            auto get = [&] { _server->on_get(key, reply); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(LPC_WRITE, nullptr, get);
            t->wait();
            if (i % 25 == 0)
                _server->get_read_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "STOP");
        std::string result;
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_result(result), false);
    }

    // test on_multi_get one hotkey data
    void read_test_multi_get()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "COARSE");
        dsn::blob key;
        for (int i = 0; i < 600; i++) {
            ::dsn::apps::multi_get_request request;
            pegasus_generate_key(key, hotkey_generator(true), std::string(""));
            request.__set_hash_key(key);
            ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
            auto multi_get = [&] { _server->on_multi_get(request, reply); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(LPC_WRITE, nullptr, multi_get);
            t->wait();
            if (i % 25 == 0)
                _server->get_read_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
    }

    void write_test_random_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "COARSE");

        dsn::blob key;
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(false), std::string("sortkeysortkeysortkeysortkey"));
            dsn::apps::update_request req;
            req.key = key;
            req.value.assign("value", 0, 5);
            auto writes = new dsn::message_ex *[2];
            writes[0] = pegasus::create_put_request(req);
            writes[1] = pegasus::create_remove_request(key);
            auto cleanup = dsn::defer([=]() { delete[] writes; });
            auto write = [&] { _server->on_batched_write_requests(i, 0, writes, 2); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, write);
            t->wait();
            if (i % 25 == 0)
                _server->get_write_hotkey_collector()->analyse_data();
        }
        ASSERT_NE(_server->get_write_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_result(result), false);

        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
    }

    void write_test_hotkey_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "COARSE");

        dsn::blob key;
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(true), std::string("sortkeysortkeysortkeysortkey"));
            dsn::apps::update_request req;
            req.key = key;
            req.value.assign("value", 0, 5);
            auto writes = new dsn::message_ex *[2];
            writes[0] = pegasus::create_put_request(req);
            writes[1] = pegasus::create_remove_request(key);
            auto cleanup = dsn::defer([=]() { delete[] writes; });
            auto write = [&] { _server->on_batched_write_requests(i, 0, writes, 2); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, write);
            t->wait();
            if (i % 25 == 0)
                _server->get_write_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
    }

    void write_test_no_data()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "STOP");

        dsn::blob key;
        for (int i = 0; i < 300; i++) {
            pegasus_generate_key(
                key, hotkey_generator(true), std::string("sortkeysortkeysortkeysortkey"));
            dsn::apps::update_request req;
            req.key = key;
            req.value.assign("value", 0, 5);
            auto writes = new dsn::message_ex *[2];
            writes[0] = pegasus::create_put_request(req);
            writes[1] = pegasus::create_remove_request(key);
            auto cleanup = dsn::defer([=]() { delete[] writes; });
            auto write = [&] { _server->on_batched_write_requests(i, 0, writes, 2); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, write);
            t->wait();
            if (i % 25 == 0)
                _server->get_write_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "STOP");
        std::string result;
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_result(result), false);
    }

    void write_test_multi_put()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "COARSE");

        dsn::blob key;
        for (int i = 1; i < 300; i++) {
            std::string hash_key(hotkey_generator(true));
            dsn::apps::multi_put_request request;
            request.hash_key.assign(hash_key.data(), 0, hash_key.length());
            for (int j = 0; j < 10; j++) {
                request.kvs.emplace_back();
                std::string temp = std::to_string(j);
                request.kvs.back().key.assign(temp.data(), 0, temp.size());
                request.kvs.back().value.assign(temp.data(), 0, temp.size());
            }
            auto msg = new dsn::message_ex *[1];
            auto write = [&] { _server->on_batched_write_requests(i, 0, msg, 1); };
            msg[0] = dsn::from_thrift_request_to_received_message(
                request, dsn::apps::RPC_RRDB_RRDB_MULTI_PUT);
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, write);
            t->wait();
            if (i % 25 == 0)
                _server->get_write_hotkey_collector()->analyse_data();
        }
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
    }

    void read_collector_start_stop_test()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "COARSE");

        dsn::blob key;
        ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
        for (int i = 0; i < 5; i++) {
            std::string hashkey = hotkey_generator(true);
            pegasus_generate_key(key, hashkey, std::string("sortkeysortkeysortkeysortkey"));
            auto get = [&] { _server->on_get(key, reply); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(LPC_WRITE, nullptr, get);
            t->wait();
            if (i % 2 == 0)
                _server->get_read_hotkey_collector()->analyse_data();
        }

        req.type = dsn::apps::hotkey_type::READ;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_read_hotkey_collector()->get_status(), "STOP");
    }

    void write_collector_start_stop_test()
    {
        ::dsn::apps::hotkey_detect_request req;
        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        ::dsn::rpc_replier<::dsn::apps::hotkey_detect_response> resp(nullptr);
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "COARSE");

        dsn::blob key;
        for (int i = 0; i < 5; i++) {
            pegasus_generate_key(
                key, hotkey_generator(true), std::string("sortkeysortkeysortkeysortkey"));
            dsn::apps::update_request req;
            req.key = key;
            req.value.assign("value", 0, 5);
            auto writes = new dsn::message_ex *[2];
            writes[0] = pegasus::create_put_request(req);
            writes[1] = pegasus::create_remove_request(key);
            auto cleanup = dsn::defer([=]() { delete[] writes; });
            auto write = [&] { _server->on_batched_write_requests(i, 0, writes, 2); };
            dsn::task_ptr t;
            t = dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, write);
            t->wait();
            if (i % 2 == 0)
                _server->get_write_hotkey_collector()->analyse_data();
        }

        req.type = dsn::apps::hotkey_type::WRITE;
        req.operation = dsn::apps::hotkey_collector_operation::STOP;
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(_server->get_write_hotkey_collector()->get_status(), "STOP");
    }
};

TEST_F(hotkey_collector_test, read_test_readom_data) { read_test_readom_data(); }

TEST_F(hotkey_collector_test, read_test_hotkey_data) { read_test_hotkey_data(); }

TEST_F(hotkey_collector_test, read_test_no_data) { read_test_no_data(); }

TEST_F(hotkey_collector_test, read_test_multi_get) { read_test_multi_get(); }

TEST_F(hotkey_collector_test, write_test_random_data) { write_test_random_data(); }

TEST_F(hotkey_collector_test, write_test_hotkey_data) { write_test_hotkey_data(); }

TEST_F(hotkey_collector_test, write_test_no_data) { write_test_no_data(); }

TEST_F(hotkey_collector_test, write_test_multi_put) { write_test_multi_put(); }

TEST_F(hotkey_collector_test, read_collector_start_stop_test) { read_collector_start_stop_test(); }

TEST_F(hotkey_collector_test, write_collector_start_stop_test)
{
    write_collector_start_stop_test();
}

} // namespace server
} // namespace pegasus
