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

#include "server/hotkey_collector.h"

#include "utils/rand.h"
#include "utils/flags.h"
#include "utils/defer.h"
#include "runtime/task/task_tracker.h"
#include "server/test/message_utils.h"
#include "base/pegasus_key_schema.h"
#include "pegasus_server_test_base.h"

namespace pegasus {
namespace server {

DSN_DECLARE_uint32(hotkey_buckets_num);

static std::string generate_hash_key_by_random(bool is_hotkey, int probability = 100)
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

TEST(hotkey_collector_public_func_test, get_bucket_id_test)
{
    int bucket_id = -1;
    for (int i = 0; i < 1000000; i++) {
        bucket_id = get_bucket_id(dsn::blob::create_from_bytes(generate_hash_key_by_random(false)),
                                  FLAGS_hotkey_buckets_num);
        ASSERT_GE(bucket_id, 0);
        ASSERT_LT(bucket_id, FLAGS_hotkey_buckets_num);
    }
}

TEST(hotkey_collector_public_func_test, find_outlier_index_test)
{
    int threshold = 3;
    int hot_index;
    bool hot_index_found;

    hot_index_found = find_outlier_index({1, 2, 3}, threshold, hot_index);
    ASSERT_EQ(hot_index_found, false);
    ASSERT_EQ(hot_index, -1);

    hot_index_found = find_outlier_index({1, 2, 100000}, threshold, hot_index);
    ASSERT_EQ(hot_index_found, true);
    ASSERT_EQ(hot_index, 2);

    hot_index_found = find_outlier_index({1, 10000, 2, 3, 4, 10000000, 6}, threshold, hot_index);
    ASSERT_EQ(hot_index_found, true);
    ASSERT_EQ(hot_index, 5);

    hot_index_found = find_outlier_index(
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, threshold, hot_index);
    ASSERT_EQ(hot_index_found, false);
    ASSERT_EQ(hot_index, -1);
}

class coarse_collector_test : public pegasus_server_test_base
{
public:
    coarse_collector_test() : coarse_collector(_server.get(), FLAGS_hotkey_buckets_num){};

    hotkey_coarse_data_collector coarse_collector;

    bool empty()
    {
        int empty = true;
        for (const auto &iter : coarse_collector._hash_buckets) {
            if (iter.load() != 0) {
                return false;
            }
        }
        return true;
    }

    dsn::task_tracker _tracker;
};

TEST_F(coarse_collector_test, coarse_collector)
{
    detect_hotkey_result result;

    for (int i = 0; i < 1000; i++) {
        dsn::tasking::enqueue(LPC_WRITE, &_tracker, [&] {
            dsn::blob hash_key =
                dsn::blob::create_from_bytes(generate_hash_key_by_random(true, 80));
            coarse_collector.capture_data(hash_key, 1);
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    coarse_collector.analyse_data(result);
    ASSERT_NE(result.coarse_bucket_index, -1);
    _tracker.wait_outstanding_tasks();

    coarse_collector.clear();
    ASSERT_TRUE(empty());

    for (int i = 0; i < 1000; i++) {

        dsn::tasking::enqueue(LPC_WRITE, &_tracker, [&] {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key_by_random(false));
            coarse_collector.capture_data(hash_key, 1);
        });
    }
    coarse_collector.analyse_data(result);
    _tracker.wait_outstanding_tasks();
    ASSERT_EQ(result.coarse_bucket_index, -1);
}

class fine_collector_test : public pegasus_server_test_base
{
public:
    int max_queue_size = 1000;
    int target_bucket_index = 0;
    hotkey_fine_data_collector fine_collector;
    fine_collector_test() : fine_collector(_server.get(), 1, max_queue_size)
    {
        fine_collector.change_target_bucket(0);
    };

    int now_queue_size()
    {
        int queue_size = 0;
        std::pair<dsn::blob, uint64_t> key_weight_pair;
        while (fine_collector._capture_key_queue.try_dequeue(key_weight_pair)) {
            queue_size++;
        };
        return queue_size;
    }

    dsn::task_tracker _tracker;
};

TEST_F(fine_collector_test, fine_collector)
{
    detect_hotkey_result result;

    for (int i = 0; i < 1000; i++) {
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, &_tracker, [&] {
            dsn::blob hash_key =
                dsn::blob::create_from_bytes(generate_hash_key_by_random(true, 80));
            fine_collector.capture_data(hash_key, 1);
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    fine_collector.analyse_data(result);
    _tracker.wait_outstanding_tasks();

    ASSERT_EQ(result.hot_hash_key, "ThisisahotkeyThisisahotkey");

    fine_collector.clear();
    ASSERT_EQ(now_queue_size(), 0);

    result.hot_hash_key = "";
    for (int i = 0; i < 1000; i++) {
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, &_tracker, [&] {
            dsn::blob hash_key = dsn::blob::create_from_bytes(generate_hash_key_by_random(false));
            fine_collector.capture_data(hash_key, 1);
        });
    }
    fine_collector.analyse_data(result);
    _tracker.wait_outstanding_tasks();
    ASSERT_TRUE(result.hot_hash_key.empty());

    for (int i = 0; i < 5000; i++) {
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, &_tracker, [&] {
            dsn::blob hash_key =
                dsn::blob::create_from_bytes(generate_hash_key_by_random(true, 80));
            fine_collector.capture_data(hash_key, 1);
        });
    }
    _tracker.wait_outstanding_tasks();
    ASSERT_LT(now_queue_size(), max_queue_size * 2);
}

class hotkey_collector_test : public pegasus_server_test_base
{
public:
    hotkey_collector_test() { start(); }

    std::shared_ptr<pegasus::server::hotkey_collector> get_read_collector()
    {
        return _server->_read_hotkey_collector;
    }
    std::shared_ptr<pegasus::server::hotkey_collector> get_write_collector()
    {
        return _server->_write_hotkey_collector;
    }
    dsn::replication::hotkey_type::type
    get_collector_type(std::shared_ptr<pegasus::server::hotkey_collector> c)
    {
        return c->_hotkey_type;
    }
    hotkey_collector_state get_collector_stat(std::shared_ptr<pegasus::server::hotkey_collector> c)
    {
        return c->_state;
    }

    detect_hotkey_result *get_result(std::shared_ptr<pegasus::server::hotkey_collector> c)
    {
        return &c->_result;
    }

    void on_detect_hotkey(const dsn::replication::detect_hotkey_request &req,
                          dsn::replication::detect_hotkey_response &resp)
    {
        _server->on_detect_hotkey(req, resp);
    }

    get_rpc generate_get_rpc(std::string hash_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, std::string("sortkey"));
        get_rpc rpc(dsn::make_unique<dsn::blob>(raw_key), dsn::apps::RPC_RRDB_RRDB_GET);
        return rpc;
    }

    dsn::apps::update_request generate_set_req(std::string hash_key)
    {
        dsn::apps::update_request req;
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, std::string("sortkey"));
        req.key = raw_key;
        req.value.assign(hash_key.c_str(), 0, hash_key.length());
        return req;
    }

    dsn::replication::detect_hotkey_request
    generate_control_rpc(dsn::replication::hotkey_type::type type,
                         dsn::replication::detect_action::type action)
    {
        dsn::replication::detect_hotkey_request req;
        req.type = type;
        req.action = action;
        req.pid = dsn::gpid(0, 2);
        return req;
    }

    dsn::task_tracker _tracker;
};

TEST_F(hotkey_collector_test, hotkey_type)
{
    ASSERT_EQ(get_collector_type(get_read_collector()), dsn::replication::hotkey_type::READ);
    ASSERT_EQ(get_collector_type(get_write_collector()), dsn::replication::hotkey_type::WRITE);
}

TEST_F(hotkey_collector_test, state_transform)
{
    auto collector = get_read_collector();
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::STOPPED);

    dsn::replication::detect_hotkey_response resp;
    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::START),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::COARSE_DETECTING);

    for (int i = 0; i < 100; i++) {
        dsn::tasking::enqueue(LPC_WRITE, &_tracker, [&] {
            _server->on_get(generate_get_rpc(generate_hash_key_by_random(true, 80)));
        });
    }
    _tracker.wait_outstanding_tasks();
    collector->analyse_data();
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::FINE_DETECTING);

    for (int i = 0; i < 100; i++) {
        dsn::tasking::enqueue(LPC_WRITE, &_tracker, [&] {
            _server->on_get(generate_get_rpc(generate_hash_key_by_random(true, 80)));
        });
    }
    _tracker.wait_outstanding_tasks();
    collector->analyse_data();
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::FINISHED);

    auto result = get_result(collector);
    ASSERT_TRUE(result->if_find_result);
    ASSERT_EQ(result->hot_hash_key, "ThisisahotkeyThisisahotkey");

    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::QUERY),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    ASSERT_EQ(resp.hotkey_result, "ThisisahotkeyThisisahotkey");

    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::STOP),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::STOPPED);

    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::START),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::COARSE_DETECTING);

    for (int i = 0; i < 1000; i++) {
        dsn::tasking::enqueue(LPC_WRITE, &_tracker, [&] {
            _server->on_get(generate_get_rpc(generate_hash_key_by_random(false)));
        });
    }
    collector->analyse_data();
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::COARSE_DETECTING);

    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::STOP),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    ASSERT_EQ(get_collector_stat(collector), hotkey_collector_state::STOPPED);
    _tracker.wait_outstanding_tasks();
}

TEST_F(hotkey_collector_test, data_completeness)
{
    dsn::replication::detect_hotkey_response resp;
    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::START),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::WRITE,
                                          dsn::replication::detect_action::START),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);

    const uint16_t WRITE_REQUEST_COUNT = 1000;
    dsn::message_ex *writes[WRITE_REQUEST_COUNT];
    for (int i = 0; i < WRITE_REQUEST_COUNT; i++) {
        writes[i] = create_put_request(generate_set_req(std::to_string(i)));
    }
    _server->on_batched_write_requests(int64_t(0), uint64_t(0), writes, WRITE_REQUEST_COUNT);

    for (int i = 0; i < WRITE_REQUEST_COUNT; i++) {
        auto rpc = generate_get_rpc(std::to_string(i));
        _server->on_get(rpc);
        auto value = rpc.response().value.to_string();
        ASSERT_EQ(value, std::to_string(i));
    }

    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::READ,
                                          dsn::replication::detect_action::STOP),
                     resp);
    ASSERT_EQ(resp.err, dsn::ERR_OK);
    on_detect_hotkey(generate_control_rpc(dsn::replication::hotkey_type::WRITE,
                                          dsn::replication::detect_action::STOP),
                     resp);
}

} // namespace server
} // namespace pegasus
