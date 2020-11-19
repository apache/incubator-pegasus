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

#include <dsn/utility/rand.h>
#include <dsn/utility/flags.h>
#include <dsn/tool-api/task_tracker.h>
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

TEST(hotkey_collector_test, get_bucket_id_test)
{
    int bucket_id = -1;
    for (int i = 0; i < 1000000; i++) {
        bucket_id = get_bucket_id(dsn::blob::create_from_bytes(generate_hash_key_by_random(false)));
        ASSERT_GE(bucket_id, 0);
        ASSERT_LT(bucket_id, FLAGS_hotkey_buckets_num);
    }
}

TEST(hotkey_collector_test, find_outlier_index_test)
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
    coarse_collector_test() : coarse_collector(_server.get()){};

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
    fine_collector_test() : fine_collector(_server.get(), 0, max_queue_size){};

    hotkey_fine_data_collector fine_collector;

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
    auto hotkey_buckets_num_backup = FLAGS_hotkey_buckets_num;
    FLAGS_hotkey_buckets_num = 1;
    detect_hotkey_result result;

    for (int i = 0; i < 1000; i++) {
        dsn::tasking::enqueue(RPC_REPLICATION_WRITE_EMPTY, &_tracker, [&] {
            dsn::blob hash_key =
                dsn::blob::create_from_bytes(generate_hash_key_by_random(true, 80));
            fine_collector.capture_data(hash_key, 1);
        });
    }
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

    FLAGS_hotkey_buckets_num = hotkey_buckets_num_backup;
}

} // namespace server
} // namespace pegasus
