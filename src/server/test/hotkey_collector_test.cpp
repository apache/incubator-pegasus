// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/hotkey_collector.h"

#include <gtest/gtest.h>
#include <rrdb/rrdb_types.h>
#include <dsn/utility/rand.h>
#include <dsn/cpp/serverlet.h>

#include "message_utils.h"
#include "pegasus_server_test_base.h"
#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(hotkey_collector_max_work_time);

class hotkey_collector_test : public pegasus_server_test_base
{
public:
    static std::string generate_hash_key(bool is_hotkey)
    {
        if (is_hotkey) {
            return "ThisisahotkeyThisisahotkey";
        }
        static const std::string chars("abcdefghijklmnopqrstuvwxyz"
                                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                       "1234567890"
                                       "!@#$%^&*()"
                                       "`~-_=+[{]{\\|;:'\",<.>/? ");
        std::string result;
        for (int i = 0; i < 20; i++) {
            result += chars[dsn::rand::next_u32(chars.size())];
        }
        return result;
    }

    static get_rpc generate_get_rpc(bool is_hotkey, std::string &hash_key)
    {
        get_rpc rpc(dsn::make_unique<dsn::blob>(), dsn::apps::RPC_RRDB_RRDB_GET);
        dsn::blob &raw_key = *rpc.mutable_request();
        hash_key = generate_hash_key(is_hotkey);
        pegasus_generate_key(raw_key, hash_key, std::string("sortkey"));
        return rpc;
    }

    static multi_get_rpc generate_multi_get_rpc(bool is_hotkey)
    {
        multi_get_rpc rpc(dsn::make_unique<dsn::apps::multi_get_request>(),
                          dsn::apps::RPC_RRDB_RRDB_MULTI_GET);
        rpc.mutable_request()->hash_key =
            dsn::blob::create_from_bytes(generate_hash_key(is_hotkey));
        return rpc;
    }

    hotkey_collector_test() { start(); }

    hotkey_coarse_data_collector &get_read_coarse_collector()
    {
        return *get_read_collector()._coarse_data_collector;
    }
    hotkey_coarse_data_collector &get_write_coarse_collector()
    {
        return *get_write_collector()._coarse_data_collector;
    }
    hotkey_collector &get_read_collector() { return *_server->_read_hotkey_collector; }
    hotkey_collector &get_write_collector() { return *_server->_write_hotkey_collector; }

    void start_hotkey_detection(dsn::apps::hotkey_type::type hotkey_type)
    {
        hotkey_collector *collector = &get_read_collector();
        if (hotkey_type == dsn::apps::hotkey_type::WRITE) {
            collector = &get_write_collector();
        }

        ASSERT_EQ(collector->get_status(), "STOP");
        ::dsn::apps::hotkey_detect_request req;
        req.type = hotkey_type;
        req.operation = dsn::apps::hotkey_collector_operation::START;
        _server->on_detect_hotkey(detect_hotkey_rpc(
            dsn::make_unique<::dsn::apps::hotkey_detect_request>(req), RPC_DETECT_HOTKEY));
        ASSERT_EQ(collector->get_status(), "COARSE");
    }
    void start_read_hotkey_detection() { start_hotkey_detection(dsn::apps::hotkey_type::READ); }
    void start_write_hotkey_detection() { start_hotkey_detection(dsn::apps::hotkey_type::WRITE); }
};

TEST_F(hotkey_collector_test, detection_disabled)
{
    hotkey_collector &collector = get_write_collector();
    ASSERT_EQ(collector.get_status(), "STOP");

    for (int round = 0; round < 8; round++) {
        for (int i = 0; i < 25; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
            collector.capture_hash_key(hash_key);
        }
        get_write_collector().analyse_data();
        ASSERT_EQ(collector.get_status(), "STOP");
        std::string result;
        ASSERT_EQ(collector.get_result(result), false);
    }
}

TEST_F(hotkey_collector_test, detection_enabled)
{
    start_write_hotkey_detection();
    hotkey_collector &collector = get_write_collector();

    for (int round = 0; round < 8; round++) {
        // ensure detection won't move forward when no hotkey is found.

        for (int i = 0; i < 25; i++) {
            // generates uniformly distributed key.
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(false));
            collector.capture_hash_key(hash_key);
        }

        collector.analyse_data();

        // get no result
        ASSERT_EQ(collector.get_status(), "COARSE");
        std::string result;
        ASSERT_EQ(collector.get_result(result), false);
    }
    collector.stop();
    ASSERT_EQ(collector.get_status(), "STOP");
    start_write_hotkey_detection(); // start a new round
    {
        // with hotkey existence
        for (int i = 0; i < 100; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
            collector.capture_hash_key(hash_key);
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINE");
        for (int i = 0; i < 100; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
            collector.capture_hash_key(hash_key);
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(collector.get_result(result), true);
    }
}

TEST_F(hotkey_collector_test, start)
{
    hotkey_collector &collector = get_write_collector();

    // start(STOP) -> COARSE
    std::string err_hint;
    ASSERT_TRUE(collector.start(err_hint));
    ASSERT_EQ(err_hint, "");
    ASSERT_EQ(collector.get_status(), "COARSE");

    // start(COARSE) failed.
    ASSERT_FALSE(collector.start(err_hint));
    ASSERT_EQ(err_hint, "still detecting WRITE hotkey, state is COARSE");
    ASSERT_EQ(collector.get_status(), "COARSE");

    // start(FINE) failed.
    collector._state.store(collector_state::FINE);
    ASSERT_FALSE(collector.start(err_hint));
    ASSERT_EQ(err_hint, "still detecting WRITE hotkey, state is FINE");
    ASSERT_EQ(collector.get_status(), "FINE");

    // start(FINISH) failed.
    collector._state.store(collector_state::FINISH);
    ASSERT_FALSE(collector.start(err_hint));
    ASSERT_EQ(
        err_hint,
        "WRITE hotkey result has been found, you can send a stop rpc to restart hotkey detection");
    ASSERT_EQ(collector.get_status(), "FINISH");
}

TEST_F(hotkey_collector_test, capture_read)
{
    // ensure a capture_data is triggered for each read when detection enabled.

    start_read_hotkey_detection();
    hotkey_collector *collector = &get_read_collector();
    { // hotkey on Get
        std::string hash_key;
        get_rpc rpc = generate_get_rpc(false, hash_key);
        for (int i = 0; i < 11; i++) {
            dsn::tasking::enqueue(LPC_WRITE, nullptr, [&] { _server->on_get(rpc); })->wait();
        }

        int bucket_id = hotkey_collector::get_bucket_id(hash_key);
        int bucket_size = get_read_coarse_collector()._hash_buckets[bucket_id];
        ASSERT_EQ(bucket_size, 11);
    }
    collector->stop();
    start_read_hotkey_detection();
    { // hotkey on MultiGet
        multi_get_rpc rpc = generate_multi_get_rpc(false);
        for (int i = 0; i < 5; i++) {
            rpc.mutable_request()->sort_keys.push_back(
                dsn::blob::create_from_bytes(std::to_string(i)));
        }
        std::vector<dsn::task_ptr> tasks;
        for (int i = 0; i < 11; i++) {
            // run reads concurrently
            auto t = dsn::tasking::enqueue(LPC_WRITE, nullptr, [&] { _server->on_multi_get(rpc); });
            tasks.push_back(t);
        }
        for (auto &t : tasks) {
            t->wait();
        }
        int bucket_id = hotkey_collector::get_bucket_id(rpc.request().hash_key);
        int bucket_size = get_read_coarse_collector()._hash_buckets[bucket_id];
        ASSERT_EQ(bucket_size, 11 * 5);
    }
}

TEST_F(hotkey_collector_test, capture_write)
{
    // hotkey on Put/Remove/Incr/MultiPut. For simplicity we do not include all writes.

    hotkey_collector *collector = &get_write_collector();
    start_write_hotkey_detection();
    std::string hash_key = generate_hash_key(true);
    dsn::blob raw_key;
    pegasus_generate_key(raw_key, hash_key, std::string("sortkey"));

    dsn::apps::update_request update;
    update.key = raw_key;
    update.value = dsn::blob::create_from_bytes("value");

    dsn::apps::incr_request incr_req;
    incr_req.key = raw_key;
    incr_req.increment = 1;
    auto incr = pegasus::create_incr_request(incr_req);

    dsn::apps::multi_put_request multi_put_req;
    multi_put_req.hash_key = dsn::blob::create_from_bytes(std::string(hash_key));
    multi_put_req.kvs.push_back({});
    auto multi_put = pegasus::create_multi_put_request(multi_put_req);

    for (int i = 0; i < 11; i++) {
        // put & remove
        std::vector<dsn::message_ex *> writes(
            {pegasus::create_put_request(update), pegasus::create_remove_request(raw_key)});
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, [&] {
            _server->on_batched_write_requests(1, 0, writes.data(), writes.size());
        })->wait();
    }
    for (auto msg : {multi_put, incr}) { // incr & multi_put
        std::vector<dsn::message_ex *> writes({msg});
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, nullptr, [&] {
            _server->on_batched_write_requests(1, 0, writes.data(), writes.size());
        })->wait();
    }

    int bucket_id = hotkey_collector::get_bucket_id(hash_key);
    int bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * 2 + 2);
}

TEST_F(hotkey_collector_test, detection_exceeds_work_time)
{
    start_write_hotkey_detection();
    hotkey_collector &collector = get_write_collector();

    auto old_config = FLAGS_hotkey_collector_max_work_time;
    FLAGS_hotkey_collector_max_work_time = 0;

    sleep(2);
    collector.analyse_data();
    ASSERT_EQ(collector.get_status(), "STOP");

    FLAGS_hotkey_collector_max_work_time = old_config;
}

} // namespace server
} // namespace pegasus
