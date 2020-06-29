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
#include "server/capacity_unit_calculator.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(hotkey_collector_max_work_time);

class hotkey_collector_test : public pegasus_server_test_base
{
public:
    static std::string generate_hash_key(bool is_hotkey, int probability = 100)
    {
        if (is_hotkey && (dsn::rand::next_u32(100) < probability)) {
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
    void refresh_read_hotkey()
    {
        hotkey_collector *collector = &get_read_collector();
        collector->stop();
        start_read_hotkey_detection();
    }
    void refresh_write_hotkey()
    {
        hotkey_collector *collector = &get_write_collector();
        collector->stop();
        start_write_hotkey_detection();
    }
};

TEST_F(hotkey_collector_test, detection_disabled)
{
    hotkey_collector &collector = get_write_collector();
    ASSERT_EQ(collector.get_status(), "STOP");

    for (int round = 0; round < 8; round++) {
        for (int i = 0; i < 25; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
            collector.capture_hash_key(hash_key, hash_key.size());
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
            collector.capture_hash_key(hash_key, hash_key.size());
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
            collector.capture_hash_key(hash_key, hash_key.size());
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINE");
        for (int i = 0; i < 100; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
            collector.capture_hash_key(hash_key, hash_key.size());
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(collector.get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
    }
    collector.stop();
    ASSERT_EQ(collector.get_status(), "STOP");
    start_write_hotkey_detection();
    {
        // with random hotkey existence
        for (int i = 0; i < 100; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true, 50));
            collector.capture_hash_key(hash_key, hash_key.size());
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINE");
        for (int i = 0; i < 100; i++) {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true, 50));
            collector.capture_hash_key(hash_key, hash_key.size());
        }
        collector.analyse_data();
        ASSERT_EQ(collector.get_status(), "FINISH");
        std::string result;
        ASSERT_EQ(collector.get_result(result), true);
        ASSERT_EQ(result, "ThisisahotkeyThisisahotkey");
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
    // ensure a capture_hash_key is triggered for each read when detection enabled.

    start_read_hotkey_detection();
    hotkey_collector *collector = &get_read_collector();
    { // hotkey on Get
        dsn::blob raw_key;
        std::string hash_key = generate_hash_key(true);
        pegasus_generate_key(raw_key, generate_hash_key(true), std::string("sortkey"));
        dsn::blob value;
        for (int i = 0; i < 11; i++) {
            dsn::tasking::enqueue(LPC_WRITE, nullptr, [&] {
                _server->_cu_calculator->add_get_cu(
                    0, raw_key, dsn::blob::create_from_bytes("value"));
            })->wait();
        }

        int bucket_id = hotkey_collector::get_bucket_id(hash_key);
        uint64_t bucket_size = get_read_coarse_collector()._hash_buckets[bucket_id];
        ASSERT_EQ(bucket_size, 440);
    }
    collector->stop();
    start_read_hotkey_detection();
    { // hotkey on MultiGet
        dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
        std::vector<::dsn::apps::key_value> kvs;
        ::dsn::apps::key_value kv;
        kv.key = dsn::blob::create_from_bytes(generate_hash_key(true));
        kv.value = dsn::blob::create_from_bytes(generate_hash_key(true));
        for (int i = 0; i < 5; i++) {
            kvs.push_back(kv);
        }
        std::vector<dsn::task_ptr> tasks;
        for (int i = 0; i < 11; i++) {
            // run reads concurrently
            auto t = dsn::tasking::enqueue(LPC_WRITE, nullptr, [&] {
                _server->_cu_calculator->add_multi_get_cu(0, hash_key, kvs);
            });
            tasks.push_back(t);
        }
        for (auto &t : tasks) {
            t->wait();
        }
        int bucket_id = hotkey_collector::get_bucket_id(hash_key);
        uint64_t bucket_size = get_read_coarse_collector()._hash_buckets[bucket_id];
        ASSERT_EQ(bucket_size, 4290);
        // hotkey on Scan
        for (int i = 0; i < 11; i++) {
            auto t = dsn::tasking::enqueue(LPC_WRITE, nullptr, [&] {
                _server->_cu_calculator->add_scan_cu(0, kvs, hash_key);
            });
            tasks.push_back(t);
        }
        for (auto &t : tasks) {
            t->wait();
        }
        bucket_size = get_read_coarse_collector()._hash_buckets[bucket_id];
        ASSERT_EQ(bucket_size, 7150);
    }
}

TEST_F(hotkey_collector_test, capture_write)
{
    // hotkey on Put/MultiPut/Remove/MultiRemove/MultiPut/CheckAndSet/CheckAndMutate

    dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key(true));
    dsn::blob raw_key;
    pegasus_generate_key(raw_key, generate_hash_key(true), std::string("sortkey"));
    dsn::blob value = dsn::blob::create_from_bytes("value");
    int bucket_id = hotkey_collector::get_bucket_id(hash_key);
    std::vector<::dsn::apps::key_value> kvs;
    ::dsn::apps::key_value kv;
    kv.key = dsn::blob::create_from_bytes(generate_hash_key(true));
    kv.value = dsn::blob::create_from_bytes(generate_hash_key(true));
    for (int i = 0; i < 5; i++) {
        kvs.push_back(kv);
    }

    start_write_hotkey_detection();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_put_cu(0, raw_key, value);
    }
    uint64_t bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * (35 + 5));

    refresh_write_hotkey();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_remove_cu(0, raw_key);
    }
    bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * 35);

    refresh_write_hotkey();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_multi_put_cu(0, hash_key, kvs);
    }
    bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * 5 * (26 + 26 + 26));

    refresh_write_hotkey();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_multi_remove_cu(
            0, hash_key, {dsn::blob::create_from_bytes("sort_key")});
    }
    bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * (26 + 8));

    refresh_write_hotkey();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_check_and_set_cu(
            0, hash_key, ::dsn::blob(), ::dsn::blob(), value);
    }
    bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * (26 + 5));

    refresh_write_hotkey();
    for (int i = 0; i < 11; i++) {
        _server->_cu_calculator->add_check_and_mutate_cu(
            0, hash_key, ::dsn::blob(), std::vector<::dsn::apps::mutate>(1));
    }
    bucket_size = get_write_coarse_collector()._hash_buckets[bucket_id];
    ASSERT_EQ(bucket_size, 11 * 26);
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
