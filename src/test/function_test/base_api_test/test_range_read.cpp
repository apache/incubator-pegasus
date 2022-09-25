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

#include <dsn/service_api_c.h>
#include <gtest/gtest.h>
#include "include/pegasus/client.h"
#include "include/pegasus/error.h"
#include "test/function_test/utils/test_util.h"

using namespace ::dsn;
using namespace pegasus;

class range_read_test : public test_util
{
public:
    void prepare(const int32_t total_count, const int32_t expire_count)
    {
        ASSERT_GE(total_count, expire_count);

        if (expire_count > 0) {
            // set expire values
            for (auto i = 0; i < expire_count; i++) {
                std::string sort_key = "1-" + std::to_string(i);
                expect_sortkeys_.insert(sort_key);
                expect_kvs_[sort_key] = value;
            }
            ASSERT_EQ(PERR_OK, client_->multi_set(expect_hashkey, expect_kvs_, 5000, 1));
            std::this_thread::sleep_for(std::chrono::seconds(2));
            // all data is expired, so clear 'expect_kvs_'.
            expect_kvs_.clear();
        }

        if (total_count > expire_count) {
            // set normal values
            for (auto i = expire_count; i < total_count; i++) {
                std::string sort_key = "2-" + std::to_string(i);
                expect_sortkeys_.insert(sort_key);
                expect_kvs_[sort_key] = value;
            }
            ASSERT_EQ(PERR_OK, client_->multi_set(expect_hashkey, expect_kvs_));
        }
    }

    void cleanup(const int32_t expected_deleted_count)
    {
        int64_t deleted_count;
        ASSERT_EQ(PERR_OK, client_->multi_del(expect_hashkey, expect_sortkeys_, deleted_count));
        ASSERT_EQ(expected_deleted_count, deleted_count);
        expect_sortkeys_.clear();
        expect_kvs_.clear();
    }

    void test_scan(const int32_t expire_count,
                   const int32_t total_count,
                   const int32_t batch_count,
                   const int32_t expected_scan_count)
    {
        pegasus::pegasus_client::scan_options options;
        options.batch_size = batch_count;
        pegasus::pegasus_client::pegasus_scanner *scanner;
        ASSERT_EQ(PERR_OK, client_->get_scanner(expect_hashkey, "", "", options, scanner));

        std::map<std::string, std::string> actual_kvs;
        std::string hash_key;
        std::string sort_key;
        std::string actual_value;
        auto i = expire_count;
        while (i < total_count) {
            ASSERT_EQ(PERR_OK, scanner->next(hash_key, sort_key, actual_value));
            ASSERT_EQ(expect_hashkey, hash_key);
            actual_kvs[sort_key] = actual_value;
            i++;
        }
        ASSERT_EQ(PERR_SCAN_COMPLETE, scanner->next(hash_key, sort_key, actual_value));
        ASSERT_EQ(expected_scan_count, actual_kvs.size());
        delete scanner;

        ASSERT_EQ(expect_kvs_, actual_kvs);
    }

public:
    const std::string expect_hashkey = "range_read_hashkey";
    const std::string value = "value";

    std::set<std::string> expect_sortkeys_;
    std::map<std::string, std::string> expect_kvs_;
};

// TODO(yingchun): use TEST_P to refact
TEST_F(range_read_test, multiget_test)
{
    pegasus::pegasus_client::multi_get_options options;
    struct test_struct
    {
        int32_t expire_count;
        int32_t total_count;
        int32_t get_max_kv_count;
        int32_t expected_error;
        int32_t expected_value_count;
    } tests[]{// total_count < max_kv_count <= max_iteration_count
              {50, 50, 100, PERR_OK, 0},
              {20, 50, 100, PERR_OK, 30},
              {0, 50, 100, PERR_OK, 50},
              // total_count > max_kv_count >= max_iteration
              {3000, 4000, 3500, PERR_INCOMPLETE, 0},
              {500, 4000, 3500, PERR_INCOMPLETE, 2500},
              {0, 4000, 3500, PERR_INCOMPLETE, 3000},
              //  total_count > max_iteration_count > max_kv_count
              {3000, 4000, 100, PERR_INCOMPLETE, 0},
              {2950, 4000, 100, PERR_INCOMPLETE, 50},
              {100, 4000, 100, PERR_INCOMPLETE, 100},
              {20, 4000, 100, PERR_INCOMPLETE, 100},
              {0, 4000, 100, PERR_INCOMPLETE, 100}};

    for (auto test : tests) {
        std::map<std::string, std::string> new_values;
        ASSERT_NO_FATAL_FAILURE(prepare(test.total_count, test.expire_count));
        ASSERT_EQ(
            test.expected_error,
            client_->multi_get(expect_hashkey, "", "", options, new_values, test.get_max_kv_count));
        ASSERT_EQ(test.expected_value_count, new_values.size());
        ASSERT_NO_FATAL_FAILURE(cleanup(test.total_count));
    }
}

TEST_F(range_read_test, sortkeycount_test)
{
    int64_t count;
    struct test_struct
    {
        int32_t expire_count;
        int32_t total_count;
        int32_t expected_error;
        int64_t expected_count;
    } tests[]{{0, 500, PERR_OK, 500}, {500, 4000, PERR_OK, 3500}};

    for (auto test : tests) {
        ASSERT_NO_FATAL_FAILURE(prepare(test.total_count, test.expire_count));
        ASSERT_EQ(test.expected_error, client_->sortkey_count(expect_hashkey, count));
        ASSERT_EQ(test.expected_count, count);
        ASSERT_NO_FATAL_FAILURE(cleanup(test.total_count));
    }
}

TEST_F(range_read_test, scan_test)
{
    struct test_struct
    {
        int32_t expire_count;
        int32_t total_count;
        int32_t batch_size;
        int32_t expected_scan_count;
    } tests[]{// total_count < max_kv_count <= max_iteration_count
              {50, 50, 100, 0},
              {20, 50, 100, 30},
              {0, 50, 100, 50},
              // total_count > max_kv_count >= max_iteration
              {3000, 4000, 3500, 1000},
              {500, 4000, 3500, 3500},
              {0, 4000, 3500, 4000},
              //  total_count > max_iteration_count > max_kv_count
              {3000, 4000, 100, 1000},
              {2950, 4000, 100, 1050},
              {100, 4000, 100, 3900},
              {20, 4000, 100, 3980},
              {0, 4000, 100, 4000}};

    for (auto test : tests) {
        ASSERT_NO_FATAL_FAILURE(prepare(test.total_count, test.expire_count));
        ASSERT_NO_FATAL_FAILURE(test_scan(
            test.expire_count, test.total_count, test.batch_size, test.expected_scan_count));
        ASSERT_NO_FATAL_FAILURE(cleanup(test.total_count));
    }
}
