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

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"
#include "utils.h"

using namespace ::pegasus;

extern pegasus_client *client;
extern std::shared_ptr<dsn::replication::replication_ddl_client> ddl_client;
static const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static char buffer[256];
static std::map<std::string, std::map<std::string, std::string>> base;
static std::string expected_hash_key;
static constexpr int ttl_seconds = 24 * 60 * 60;
static std::map<std::string, std::map<std::string, std::pair<std::string, uint32_t>>> ttl_base;

// REQUIRED: 'buffer' has been filled with random chars.
static const std::string random_string()
{
    int pos = random() % sizeof(buffer);
    buffer[pos] = CCH[random() % sizeof(CCH)];
    unsigned int length = random() % sizeof(buffer) + 1;
    if (pos + length < sizeof(buffer)) {
        return std::string(buffer + pos, length);
    } else {
        return std::string(buffer + pos, sizeof(buffer) - pos) +
               std::string(buffer, length + pos - sizeof(buffer));
    }
}
static void clear_database()
{
    ddebug("CLEARING_DATABASE...");
    pegasus_client::scan_options option;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = client->get_unordered_scanners(1, option, scanners);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when get scanners, error="
                            << client->get_error_string(ret);
    ASSERT_EQ(1, scanners.size());
    ASSERT_NE(nullptr, scanners[0]);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (PERR_OK == (ret = (scanners[0]->next(hash_key, sort_key, value)))) {
        int r = client->del(hash_key, sort_key);
        ASSERT_EQ(PERR_OK, r) << "Error occurred when del, hash_key=" << hash_key
                              << ", sort_key=" << sort_key
                              << ", error=" << client->get_error_string(r);
    }
    delete scanners[0];

    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when next() in clearing database. error="
                                       << client->get_error_string(ret);
    ret = client->get_unordered_scanners(1, option, scanners);
    ASSERT_EQ(PERR_OK, ret) << "Error occurred when get scanners, error="
                            << client->get_error_string(ret);
    ASSERT_EQ(1, scanners.size());
    ASSERT_NE(nullptr, scanners[0]);

    ret = scanners[0]->next(hash_key, sort_key, value);
    delete scanners[0];
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when clearing database. error="
                                       << client->get_error_string(ret);

    base.clear();

    ddebug("Database cleared.");
}

// REQUIRED: 'base' is empty
static void fill_database()
{
    ddebug("FILLING_DATABASE...");

    srandom((unsigned int)time(nullptr));
    for (auto &c : buffer) {
        c = CCH[random() % sizeof(CCH)];
    }

    expected_hash_key = random_string();
    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (base[expected_hash_key].size() < 1000) {
        sort_key = random_string();
        value = random_string();
        int ret = client->set(expected_hash_key, sort_key, value);
        ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                << ", sort_key=" << sort_key
                                << ", error=" << client->get_error_string(ret);
        base[expected_hash_key][sort_key] = value;
    }

    while (base.size() < 500) {
        hash_key = random_string();
        while (base[hash_key].size() < 10) {
            sort_key = random_string();
            value = random_string();
            int ret = client->set(hash_key, sort_key, value);
            ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                    << ", sort_key=" << sort_key
                                    << ", error=" << client->get_error_string(ret);
            base[hash_key][sort_key] = value;
        }
    }

    while (base.size() < 1000) {
        hash_key = random_string();
        while (base[hash_key].size() < 10) {
            sort_key = random_string();
            value = random_string();
            auto expire_ts_seconds = static_cast<uint32_t>(ttl_seconds) + utils::epoch_now();
            int ret = client->set(hash_key, sort_key, value, 5000, ttl_seconds, nullptr);
            ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                    << ", sort_key=" << sort_key
                                    << ", error=" << client->get_error_string(ret);
            base[hash_key][sort_key] = value;
            ttl_base[hash_key][sort_key] =
                std::pair<std::string, uint32_t>(value, expire_ts_seconds);
        }
    }

    ddebug("Database filled.");
}

class scan : public testing::Test
{
public:
    static void SetUpTestCase()
    {
        ddebug("SetUp...");
        clear_database();
        fill_database();
    }

    static void TearDownTestCase()
    {
        ddebug("TearDown...");
        clear_database();
    }
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

    int32_t data_count = 0;
    for (auto scanner : scanners) {
        ASSERT_NE(nullptr, scanner);
        int32_t kv_count;
        while (PERR_OK == (ret = (scanner->next(kv_count)))) {
            data_count += kv_count;
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client->get_error_string(ret);
        delete scanner;
    }
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
    compare(data, base[expected_hash_key], expected_hash_key);
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
    compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key);
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
    compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key);
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
    compare(data, base);
}

TEST_F(scan, REQUEST_EXPIRE_TS)
{
    ddebug("TEST REQUEST_EXPIRE_TS...");

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

    compare(data, base);
    compare(ttl_data, ttl_base);
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
