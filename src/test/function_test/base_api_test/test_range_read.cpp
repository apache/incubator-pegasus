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

using namespace ::dsn;
using namespace pegasus;

class range_read_test : public testing::Test
{
public:
    void prepare(const int32_t total_count, const int32_t expire_count)
    {
        if (expire_count > 0) {
            // set expire values
            for (auto i = 0; i < expire_count; i++) {
                std::string sort_key = "1-" + std::to_string(i);
                sortkeys.insert(sort_key);
                kvs[sort_key] = value;
            }
            auto ret = pg_client->multi_set(hashkey, kvs, timeoutms, 1);
            ASSERT_EQ(PERR_OK, ret);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            kvs.clear();
        }

        if (total_count > expire_count) {
            // set normal values
            for (auto i = expire_count; i < total_count; i++) {
                std::string sort_key = "2-" + std::to_string(i);
                sortkeys.insert(sort_key);
                kvs[sort_key] = value;
            }
            auto ret = pg_client->multi_set(hashkey, kvs);
            ASSERT_EQ(PERR_OK, ret);
        }
    }

    void cleanup(const int32_t expected_deleted_count)
    {
        int64_t deleted_count;
        auto ret = pg_client->multi_del(hashkey, sortkeys, deleted_count);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ(deleted_count, expected_deleted_count);
        sortkeys.clear();
        kvs.clear();
    }

    void test_scan(const int32_t expire_count,
                   const int32_t total_count,
                   const int32_t batch_count,
                   const int32_t expected_scan_count)
    {
        pegasus::pegasus_client::scan_options options;
        options.batch_size = batch_count;
        pegasus::pegasus_client::pegasus_scanner *scanner;
        auto ret = pg_client->get_scanner(hashkey, "", "", options, scanner);
        ASSERT_EQ(ret, PERR_OK);

        std::map<std::string, std::string> scan_kvs;
        std::string hash_key;
        std::string sort_key;
        std::string act_value;
        auto i = expire_count;
        while (i < total_count) {
            ret = scanner->next(hash_key, sort_key, act_value);
            ASSERT_EQ(ret, PERR_OK);
            ASSERT_EQ(hash_key, hashkey);
            scan_kvs[sort_key] = act_value;
            i++;
        }
        ret = scanner->next(hash_key, sort_key, act_value);
        ASSERT_EQ(ret, PERR_SCAN_COMPLETE);
        ASSERT_EQ(expected_scan_count, scan_kvs.size());
        delete scanner;

        // compare scan result
        for (auto it1 = kvs.begin(), it2 = scan_kvs.begin();; ++it1, ++it2) {
            if (it1 == kvs.end()) {
                ASSERT_EQ(it2, scan_kvs.end());
                break;
            }
            ASSERT_NE(it2, scan_kvs.end());
            ASSERT_EQ(*it1, *it2);
        }
    }

public:
    const std::string table = "temp";
    const std::string hashkey = "range_read_hashkey";
    const std::string sortkey_prefix = "1-";
    const std::string value = "value";
    const int32_t timeoutms = 5000;
    pegasus_client *pg_client = pegasus_client_factory::get_client("mycluster", table.c_str());
    std::set<std::string> sortkeys;
    std::map<std::string, std::string> kvs;
};

TEST_F(range_read_test, multiget_test)
{
    pegasus::pegasus_client::multi_get_options options;
    std::map<std::string, std::string> new_values;
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
        new_values.clear();
        prepare(test.total_count, test.expire_count);
        auto ret =
            pg_client->multi_get(hashkey, "", "", options, new_values, test.get_max_kv_count);
        ASSERT_EQ(ret, test.expected_error);
        ASSERT_EQ(new_values.size(), test.expected_value_count);
        cleanup(test.total_count);
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
        prepare(test.total_count, test.expire_count);
        auto ret = pg_client->sortkey_count(hashkey, count);
        ASSERT_EQ(ret, test.expected_error);
        ASSERT_EQ(count, test.expected_count);
        cleanup(test.total_count);
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
        prepare(test.total_count, test.expire_count);
        test_scan(test.expire_count, test.total_count, test.batch_size, test.expected_scan_count);
        cleanup(test.total_count);
    }
}
