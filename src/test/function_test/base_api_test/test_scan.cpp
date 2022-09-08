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

#include <cstdlib>
#include <string>
#include <vector>
#include <map>
#include <iostream>

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
#include <unistd.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>
#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"
#include "test/function_test/utils/utils.h"
#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;

static const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static char buffer[256];
static constexpr int ttl_seconds = 24 * 60 * 60;

class scan : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        // TODO(yingchun): can be removed?
        ASSERT_EQ(dsn::ERR_OK, ddl_client->drop_app(app_name_, 0));
        ASSERT_EQ(dsn::ERR_OK, ddl_client->create_app(app_name_, "pegasus", 8, 3, {}, false));
        client = pegasus_client_factory::get_client(cluster_name_.c_str(), app_name_.c_str());
        ASSERT_TRUE(client != nullptr);
        ASSERT_NO_FATAL_FAILURE(fill_database());
    }

    void TearDown() override
    {
        // TODO(yingchun): can be removed too?
        ASSERT_EQ(dsn::ERR_OK, ddl_client->drop_app(app_name_, 0));
    }

    // REQUIRED: 'buffer' has been filled with random chars.
    static const std::string random_string()
    {
        int pos = random() % sizeof(buffer);
        unsigned int length = random() % sizeof(buffer) + 1;
        if (pos + length < sizeof(buffer)) {
            return std::string(buffer + pos, length);
        } else {
            return std::string(buffer + pos, sizeof(buffer) - pos) +
                   std::string(buffer, length + pos - sizeof(buffer));
        }
    }

    // REQUIRED: 'base' is empty
    void fill_database()
    {
        ddebug("FILLING_DATABASE...");

        srandom((unsigned int)time(nullptr));
        for (auto &c : buffer) {
            c = CCH[random() % strlen(CCH)];
        }

        int i = 0;
        // Fill data with <expected_hash_key> : <sort_key> -> <value>, there are 1000 sort keys in
        // the unique <expected_hash_key>.
        expected_hash_key = random_string();
        std::string sort_key;
        std::string value;
        while (base[expected_hash_key].size() < 1000) {
            sort_key = random_string();
            value = random_string();
            int ret = client->set(expected_hash_key, sort_key, value);
            ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << expected_hash_key
                                    << ", sort_key=" << sort_key
                                    << ", error=" << client->get_error_string(ret);
            i++;
            base[expected_hash_key][sort_key] = value;
        }
        // 1000 kvs

        std::string hash_key;
        // Fill data with <hash_key> : <sort_key> -> <value>, except <expected_hash_key>, there are
        // total 499 hash keys and 10 sortkeys in each <hash_key>.
        while (base.size() < 501) {
            hash_key = random_string();
            if (base.count(hash_key) != 0) {
                continue;
            }
            while (base[hash_key].size() < 10) {
                sort_key = random_string();
                if (base[hash_key].count(sort_key) != 0) {
                    continue;
                }
                value = random_string();
                int ret = client->set(hash_key, sort_key, value);
                ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                        << ", sort_key=" << sort_key
                                        << ", error=" << client->get_error_string(ret);
                i++;
                base[hash_key][sort_key] = value;
            }
        }
        // 1000 + 5000 kvs

        // Fill data with <hash_key> : <sort_key> -> <value> with TTL, except <expected_hash_key>
        // and normal kvs, there are total 500 hash keys and 10 sortkeys in each <hash_key>.
        while (base.size() < 1001) {
            hash_key = random_string();
            if (base.count(hash_key) != 0) {
                continue;
            }
            while (base[hash_key].size() < 10) {
                sort_key = random_string();
                if (base[hash_key].count(sort_key) != 0) {
                    continue;
                }
                value = random_string();
                auto expire_ts_seconds = static_cast<uint32_t>(ttl_seconds) + utils::epoch_now();
                int ret = client->set(hash_key, sort_key, value, 5000, ttl_seconds, nullptr);
                i++;
                ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                        << ", sort_key=" << sort_key
                                        << ", error=" << client->get_error_string(ret);
                i++;
                base[hash_key][sort_key] = value;
                ttl_base[hash_key][sort_key] =
                    std::pair<std::string, uint32_t>(value, expire_ts_seconds);
            }
        }
        // 1000 + 5000 + 5000 kvs

        ddebug_f("Database filled with kv count {}.", i);
    }

public:
    std::string expected_hash_key;
    std::map<std::string, std::map<std::string, std::string>> base;
    std::map<std::string, std::map<std::string, std::pair<std::string, uint32_t>>> ttl_base;
};

TEST_F(scan, OVERALL_COUNT_ONLY)
{
    ddebug("TEST OVERALL_SCAN_COUNT_ONLY...");
    pegasus_client::scan_options options;
    options.only_return_count = true;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = client->get_unordered_scanners(3, options, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_LE(scanners.size(), 3);

    int i = 0;
    int32_t data_count = 0;
    for (auto scanner : scanners) {
        ASSERT_NE(nullptr, scanner);
        int32_t kv_count;
        while (PERR_OK == (ret = (scanner->next(kv_count)))) {
            data_count += kv_count;
            i++;
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client->get_error_string(ret);
        delete scanner;
    }
    ddebug_f("scan count {}", i);
    int base_data_count = 0;
    for (auto &m : base) {
        base_data_count += m.second.size();
    }
    ASSERT_EQ(base_data_count, data_count);
}

TEST_F(scan, ALL_SORT_KEY)
{
    ddebug("TESTING_HASH_SCAN, ALL SORT_KEYS ....");
    pegasus_client::scan_options options;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, "", "", options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    ASSERT_NO_FATAL_FAILURE(compare(base[expected_hash_key], data, expected_hash_key));
}

TEST_F(scan, BOUND_INCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, [start, stop]...");
    auto it1 = base[expected_hash_key].begin();
    std::advance(it1, random() % 500); // [0,499]
    std::string start = it1->first;

    auto it2 = it1;
    std::advance(it2, random() % 400 + 50); // [0,499] + [50, 449] = [50, 948]
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, stop, options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    ++it2; // to be the 'end' iterator
    ASSERT_NO_FATAL_FAILURE(
        compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key));
}

TEST_F(scan, BOUND_EXCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, (start, stop)...");
    auto it1 = base[expected_hash_key].begin();
    std::advance(it1, random() % 500); // [0,499]
    std::string start = it1->first;

    auto it2 = it1;
    std::advance(it2, random() % 400 + 50); // [0,499] + [50, 449] = [50, 948]
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = false;
    options.stop_inclusive = false;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, stop, options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    ++it1;
    ASSERT_NO_FATAL_FAILURE(
        compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key));
}

TEST_F(scan, ONE_POINT)
{
    ddebug("TESTING_HASH_SCAN, [start, start]...");
    auto it1 = base[expected_hash_key].begin();
    std::advance(it1, random() % 800); // [0,799]
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, start, options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when scan. error=" << client->get_error_string(ret);
    ASSERT_EQ(expected_hash_key, hash_key);
    ASSERT_EQ(start, sort_key);
    ASSERT_EQ(it1->second, value);
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST_F(scan, HALF_INCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, [start, start)...");
    auto it1 = base[expected_hash_key].begin();
    std::advance(it1, random() % 800); // [0,799]
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = false;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, start, options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST_F(scan, VOID_SPAN)
{
    ddebug("TESTING_HASH_SCAN, [stop, start]...");
    auto it1 = base[expected_hash_key].begin();
    std::advance(it1, random() % 500); // [0,499]
    std::string start = it1->first;

    auto it2 = it1;
    std::advance(it2, random() % 400 + 50); // [0,499] + [50, 449] = [50, 948]
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, stop, start, options, scanner);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when getting scanner. error="
                            << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST_F(scan, OVERALL)
{
    ddebug("TEST OVERALL_SCAN...");
    pegasus_client::scan_options options;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = client->get_unordered_scanners(3, options, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_LE(scanners.size(), 3);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    std::map<std::string, std::map<std::string, std::string>> data;
    for (auto scanner : scanners) {
        ASSERT_NE(nullptr, scanner);
        while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
            check_and_put(data, hash_key, sort_key, value);
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client->get_error_string(ret);
        delete scanner;
    }
    ASSERT_NO_FATAL_FAILURE(compare(base, data));
}

TEST_F(scan, REQUEST_EXPIRE_TS)
{
    pegasus_client::scan_options options;
    options.return_expire_ts = true;
    std::vector<pegasus_client::pegasus_scanner *> raw_scanners;
    int ret = client->get_unordered_scanners(3, options, raw_scanners);
    ASSERT_EQ(pegasus::PERR_OK, ret) << "Error occurred when getting scanner. error="
                                     << client->get_error_string(ret);

    std::vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto raw_scanner : raw_scanners) {
        ASSERT_NE(nullptr, raw_scanner);
        scanners.push_back(raw_scanner->get_smart_wrapper());
    }
    raw_scanners.clear();
    ASSERT_LE(scanners.size(), 3);

    std::map<std::string, std::map<std::string, std::string>> data;
    std::map<std::string, std::map<std::string, std::pair<std::string, uint32_t>>> ttl_data;
    for (auto scanner : scanners) {
        std::atomic_bool split_completed(false);
        while (!split_completed.load()) {
            dsn::utils::notify_event op_completed;
            scanner->async_next([&](int err,
                                    std::string &&hash_key,
                                    std::string &&sort_key,
                                    std::string &&value,
                                    pegasus::pegasus_client::internal_info &&info,
                                    uint32_t expire_ts_seconds,
                                    int32_t kv_count) {
                if (err == pegasus::PERR_OK) {
                    check_and_put(data, hash_key, sort_key, value);
                    if (expire_ts_seconds > 0) {
                        check_and_put(ttl_data, hash_key, sort_key, value, expire_ts_seconds);
                    }
                } else if (err == pegasus::PERR_SCAN_COMPLETE) {
                    split_completed.store(true);
                } else {
                    ASSERT_TRUE(false) << "Error occurred when scan. error="
                                       << client->get_error_string(err);
                }
                op_completed.notify();
            });
            op_completed.wait();
        }
    }

    ASSERT_NO_FATAL_FAILURE(compare(base, data));
    ASSERT_NO_FATAL_FAILURE(compare(ttl_base, ttl_data));
}

TEST_F(scan, ITERATION_TIME_LIMIT)
{
    // update iteration threshold to 1ms
    auto response = ddl_client->set_app_envs(
        client->get_app_name(), {ROCKSDB_ITERATION_THRESHOLD_TIME_MS}, {std::to_string(1)});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(dsn::ERR_OK, response.get_value().err);
    // wait envs to be synced.
    std::this_thread::sleep_for(std::chrono::seconds(30));

    // write data into table
    int32_t i = 0;
    std::string sort_key;
    std::string value;
    while (i < 9000) {
        sort_key = random_string();
        value = random_string();
        int ret = client->set(expected_hash_key, sort_key, value);
        ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << expected_hash_key
                                << ", sort_key=" << sort_key
                                << ", error=" << client->get_error_string(ret);
        i++;
    }

    // get sortkey count timeout
    int64_t count = 0;
    int ret = client->sortkey_count(expected_hash_key, count);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(count, -1);

    // set iteration threshold to 100ms
    response = ddl_client->set_app_envs(
        client->get_app_name(), {ROCKSDB_ITERATION_THRESHOLD_TIME_MS}, {std::to_string(100)});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(dsn::ERR_OK, response.get_value().err);
    // wait envs to be synced.
    std::this_thread::sleep_for(std::chrono::seconds(30));
}
