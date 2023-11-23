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

#include <string.h>
#include <time.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"
#include "client/replication_ddl_client.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "meta_admin_types.h"
#include "pegasus/error.h"
#include "test/function_test/utils/test_util.h"
#include "test/function_test/utils/utils.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

using namespace ::pegasus;

class scan_test : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(app_name_, 0));
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->create_app(app_name_, "pegasus", 8, 3, {}, false));
        client_ = pegasus_client_factory::get_client(cluster_name_.c_str(), app_name_.c_str());
        ASSERT_TRUE(client_ != nullptr);
        ASSERT_NO_FATAL_FAILURE(fill_database());
    }

    void TearDown() override { ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(app_name_, 0)); }

    // REQUIRED: 'buffer_' has been filled with random chars.
    const std::string random_string() const
    {
        int pos = random() % sizeof(buffer_);
        unsigned int length = random() % sizeof(buffer_) + 1;
        if (pos + length < sizeof(buffer_)) {
            return std::string(buffer_ + pos, length);
        } else {
            return std::string(buffer_ + pos, sizeof(buffer_) - pos) +
                   std::string(buffer_, length + pos - sizeof(buffer_));
        }
    }

    // REQUIRED: 'expect_kvs_' is empty
    void fill_database()
    {
        srandom((unsigned int)time(nullptr));
        for (auto &c : buffer_) {
            c = CCH[random() % strlen(CCH)];
        }

        int i = 0;
        // Fill data with <expected_hash_key_> : <sort_key> -> <value>, there are 1000 sort keys in
        // the unique <expected_hash_key_>.
        expected_hash_key_ = random_string();
        std::string sort_key;
        std::string value;
        while (expect_kvs_[expected_hash_key_].size() < 1000) {
            sort_key = random_string();
            value = random_string();
            ASSERT_EQ(PERR_OK, client_->set(expected_hash_key_, sort_key, value))
                << "hash_key=" << expected_hash_key_ << ", sort_key=" << sort_key;
            i++;
            expect_kvs_[expected_hash_key_][sort_key] = value;
        }
        // 1000 kvs

        std::string hash_key;
        // Fill data with <hash_key> : <sort_key> -> <value>, except <expected_hash_key_>, there are
        // total 499 hash keys and 10 sortkeys in each <hash_key>.
        while (expect_kvs_.size() < 501) {
            hash_key = random_string();
            if (expect_kvs_.count(hash_key) != 0) {
                continue;
            }
            while (expect_kvs_[hash_key].size() < 10) {
                sort_key = random_string();
                if (expect_kvs_[hash_key].count(sort_key) != 0) {
                    continue;
                }
                value = random_string();
                ASSERT_EQ(PERR_OK, client_->set(hash_key, sort_key, value))
                    << "hash_key=" << hash_key << ", sort_key=" << sort_key;
                i++;
                expect_kvs_[hash_key][sort_key] = value;
            }
        }
        // 1000 + 5000 kvs

        // Fill data with <hash_key> : <sort_key> -> <value> with TTL, except <expected_hash_key_>
        // and normal kvs, there are total 500 hash keys and 10 sortkeys in each <hash_key>.
        while (expect_kvs_.size() < 1001) {
            hash_key = random_string();
            if (expect_kvs_.count(hash_key) != 0) {
                continue;
            }
            while (expect_kvs_[hash_key].size() < 10) {
                sort_key = random_string();
                if (expect_kvs_[hash_key].count(sort_key) != 0) {
                    continue;
                }
                value = random_string();
                auto expire_ts_seconds = static_cast<uint32_t>(ttl_seconds) + utils::epoch_now();
                ASSERT_EQ(PERR_OK,
                          client_->set(hash_key, sort_key, value, 5000, ttl_seconds, nullptr))
                    << "hash_key=" << hash_key << ", sort_key=" << sort_key;
                i += 2;
                expect_kvs_[hash_key][sort_key] = value;
                expect_kvs_with_ttl_[hash_key][sort_key] =
                    std::pair<std::string, uint32_t>(value, expire_ts_seconds);
            }
        }
        // 1000 + 5000 + 5000 kvs

        LOG_INFO("Database filled with kv count {}.", i);
    }

protected:
    static const char CCH[];
    static constexpr int ttl_seconds = 24 * 60 * 60;

    char buffer_[256];

    std::string expected_hash_key_;
    std::map<std::string, std::map<std::string, std::string>> expect_kvs_;
    std::map<std::string, std::map<std::string, std::pair<std::string, uint32_t>>>
        expect_kvs_with_ttl_;
};
const char scan_test::CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

TEST_F(scan_test, OVERALL_COUNT_ONLY)
{
    pegasus_client::scan_options options;
    options.only_return_count = true;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(3, options, scanners));
    ASSERT_LE(scanners.size(), 3);

    int i = 0;
    int32_t data_count = 0;
    for (auto scanner : scanners) {
        ASSERT_NE(nullptr, scanner);
        int32_t kv_count;
        int ret;
        while (PERR_OK == (ret = (scanner->next(kv_count)))) {
            data_count += kv_count;
            i++;
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client_->get_error_string(ret);
        delete scanner;
    }
    LOG_INFO("scan count {}", i);
    int base_data_count = 0;
    for (auto &m : expect_kvs_) {
        base_data_count += m.second.size();
    }
    ASSERT_EQ(base_data_count, data_count);
}

TEST_F(scan_test, ALL_SORT_KEY)
{
    pegasus_client::scan_options options;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, "", "", options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    int ret;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key_, hash_key);
        check_and_put(data, expected_hash_key_, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client_->get_error_string(ret);
    ASSERT_NO_FATAL_FAILURE(compare(expect_kvs_[expected_hash_key_], data, expected_hash_key_));
}

TEST_F(scan_test, BOUND_INCLUSIVE)
{
    auto it1 = expect_kvs_[expected_hash_key_].begin();
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
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, start, stop, options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    int ret;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key_, hash_key);
        check_and_put(data, expected_hash_key_, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret);
    ++it2; // to be the 'end' iterator
    ASSERT_NO_FATAL_FAILURE(
        compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key_));
}

TEST_F(scan_test, BOUND_EXCLUSIVE)
{
    auto it1 = expect_kvs_[expected_hash_key_].begin();
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
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, start, stop, options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    int ret;
    while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key_, hash_key);
        check_and_put(data, expected_hash_key_, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client_->get_error_string(ret);
    ++it1;
    ASSERT_NO_FATAL_FAILURE(
        compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key_));
}

TEST_F(scan_test, ONE_POINT)
{
    auto it1 = expect_kvs_[expected_hash_key_].begin();
    std::advance(it1, random() % 800); // [0,799]
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, start, start, options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ASSERT_EQ(PERR_OK, scanner->next(hash_key, sort_key, value));
    ASSERT_EQ(expected_hash_key_, hash_key);
    ASSERT_EQ(start, sort_key);
    ASSERT_EQ(it1->second, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, scanner->next(hash_key, sort_key, value));
    delete scanner;
}

TEST_F(scan_test, HALF_INCLUSIVE)
{
    auto it1 = expect_kvs_[expected_hash_key_].begin();
    std::advance(it1, random() % 800); // [0,799]
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = false;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, start, start, options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ASSERT_EQ(PERR_SCAN_COMPLETE, scanner->next(hash_key, sort_key, value));
    delete scanner;
}

TEST_F(scan_test, VOID_SPAN)
{
    auto it1 = expect_kvs_[expected_hash_key_].begin();
    std::advance(it1, random() % 500); // [0,499]
    std::string start = it1->first;

    auto it2 = it1;
    std::advance(it2, random() % 400 + 50); // [0,499] + [50, 449] = [50, 948]
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    ASSERT_EQ(PERR_OK, client_->get_scanner(expected_hash_key_, stop, start, options, scanner));
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ASSERT_EQ(PERR_SCAN_COMPLETE, scanner->next(hash_key, sort_key, value));
    delete scanner;
}

TEST_F(scan_test, OVERALL)
{
    pegasus_client::scan_options options;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(3, options, scanners));
    ASSERT_LE(scanners.size(), 3);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    std::map<std::string, std::map<std::string, std::string>> data;
    for (auto scanner : scanners) {
        ASSERT_NE(nullptr, scanner);
        int ret;
        while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
            check_and_put(data, hash_key, sort_key, value);
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client_->get_error_string(ret);
        delete scanner;
    }
    ASSERT_NO_FATAL_FAILURE(compare(expect_kvs_, data));
}

TEST_F(scan_test, REQUEST_EXPIRE_TS)
{
    pegasus_client::scan_options options;
    options.return_expire_ts = true;
    std::vector<pegasus_client::pegasus_scanner *> raw_scanners;
    ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(3, options, raw_scanners));

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
                                       << client_->get_error_string(err);
                }
                op_completed.notify();
            });
            op_completed.wait();
        }
    }

    ASSERT_NO_FATAL_FAILURE(compare(expect_kvs_, data));
    ASSERT_NO_FATAL_FAILURE(compare(expect_kvs_with_ttl_, ttl_data));
}

TEST_F(scan_test, ITERATION_TIME_LIMIT)
{
    // update iteration threshold to 1ms
    auto response = ddl_client_->set_app_envs(
        client_->get_app_name(), {ROCKSDB_ITERATION_THRESHOLD_TIME_MS}, {std::to_string(1)});
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
        ASSERT_EQ(PERR_OK, client_->set(expected_hash_key_, sort_key, value))
            << "hash_key=" << expected_hash_key_ << ", sort_key=" << sort_key;
        i++;
    }

    // get sortkey count timeout
    int64_t count = 0;
    ASSERT_EQ(PERR_OK, client_->sortkey_count(expected_hash_key_, count));
    ASSERT_EQ(-1, count);

    // set iteration threshold to 100ms
    response = ddl_client_->set_app_envs(
        client_->get_app_name(), {ROCKSDB_ITERATION_THRESHOLD_TIME_MS}, {std::to_string(100)});
    ASSERT_TRUE(response.is_ok());
    ASSERT_EQ(dsn::ERR_OK, response.get_value().err);
}
