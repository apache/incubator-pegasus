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
#include <climits>
#include <map>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include <unistd.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>
#include <atomic>

#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;

typedef pegasus_client::internal_info internal_info;

class basic : public test_util
{
};

TEST_F(basic, set_get_del)
{
    // set
    ASSERT_EQ(PERR_OK,
              client_->set("basic_test_hash_key_1", "basic_test_sort_key_1", "basic_test_value_1"));

    // exist
    ASSERT_EQ(PERR_OK, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_1"));
    ASSERT_EQ(PERR_NOT_FOUND, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_2"));

    // sortkey_count
    int64_t count;
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(1, count);

    // get
    std::string new_value;
    ASSERT_EQ(PERR_OK, client_->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value));
    ASSERT_EQ("basic_test_value_1", new_value);

    ASSERT_EQ(PERR_NOT_FOUND,
              client_->get("basic_test_hash_key_1", "basic_test_sort_key_2", new_value));

    // del
    ASSERT_EQ(PERR_OK, client_->del("basic_test_hash_key_1", "basic_test_sort_key_1"));

    // exist
    ASSERT_EQ(PERR_NOT_FOUND, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_1"));

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(0, count);

    // get
    ASSERT_EQ(PERR_NOT_FOUND,
              client_->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value));
}

TEST_F(basic, multi_get)
{
    const std::map<std::string, std::string> kvs({{"", "0"},
                                                  {"1", "1"},
                                                  {"1-abcdefg", "1-abcdefg"},
                                                  {"2", "2"},
                                                  {"2-abcdefg", "2-abcdefg"},
                                                  {"3", "3"},
                                                  {"3-efghijk", "3-efghijk"},
                                                  {"4", "4"},
                                                  {"4-hijklmn", "4-hijklmn"},
                                                  {"5", "5"},
                                                  {"5-hijklmn", "5-hijklmn"},
                                                  {"6", "6"},
                                                  {"7", "7"}});

    // multi_set
    ASSERT_EQ(PERR_OK, client_->multi_set("basic_test_multi_get", kvs));

    // sortkey_count
    int64_t count;
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_multi_get", count));
    ASSERT_EQ(13, count);

    // [null, null)
    {
        pegasus::pegasus_client::multi_get_options options;
        ASSERT_TRUE(options.start_inclusive);
        ASSERT_FALSE(options.stop_inclusive);
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_EQ(kvs, new_values);
    }

    // [null, null]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_EQ(kvs, new_values);
    }

    // (null, null)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        auto expect_kvs(kvs);
        expect_kvs.erase("");
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, null]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        auto expect_kvs(kvs);
        expect_kvs.erase("");
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [null, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"", "0"}, {"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [null, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"", "0"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // [1, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [1, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // (1, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // (1, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // [2, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "2", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-anywhere("-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "-";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({
            {"1-abcdefg", "1-abcdefg"},
            {"2-abcdefg", "2-abcdefg"},
            {"3-efghijk", "3-efghijk"},
            {"4-hijklmn", "4-hijklmn"},
            {"5-hijklmn", "5-hijklmn"},
        });
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "1";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "1-";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "abc";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs(
            {{"1-abcdefg", "1-abcdefg"}, {"2-abcdefg", "2-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({
            {"1", "1"}, {"1-abcdefg", "1-abcdefg"},
        });
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in [0, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = false;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "0", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("1") in [0, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "0", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in [1, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "2", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in (1, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = false;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get", "1", "2", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in (1-abcdefg, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = false;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get", "1-abcdefg", "2", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1-";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1-x")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1-x";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "abc";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("efg")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "efg";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("ijk")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "ijk";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("lmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "lmn";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("5-hijklmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "5-hijklmn";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1-";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("1-x")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1-x";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "abc";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("efg")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "efg";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs(
            {{"1-abcdefg", "1-abcdefg"}, {"2-abcdefg", "2-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("ijk")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "ijk";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"3-efghijk", "3-efghijk"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("lmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "lmn";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs(
            {{"4-hijklmn", "4-hijklmn"}, {"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("5-hijklmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "5-hijklmn";
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_multi_get", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // maxCount = 4
    {
        pegasus::pegasus_client::multi_get_options options;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get("basic_test_multi_get", "", "", options, new_values, 4, -1));
        std::map<std::string, std::string> expect_kvs(
            {{"", "0"}, {"1", "1"}, {"1-abcdefg", "1-abcdefg"}, {"2", "2"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // maxCount = 1
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get("basic_test_multi_get", "5", "6", options, new_values, 1, -1));
        std::map<std::string, std::string> expect_kvs({{"5", "5"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // set a expired value
    {
        pegasus::pegasus_client::multi_get_options options;
        ASSERT_EQ(PERR_OK, client_->set("basic_test_multi_get", "", "expire_value", 5000, 1));
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get("basic_test_multi_get", "", "", options, new_values, 2));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // multi_del
    {
        std::set<std::string> sortkeys({"",
                                        "1",
                                        "1-abcdefg",
                                        "2",
                                        "2-abcdefg",
                                        "3",
                                        "3-efghijk",
                                        "4",
                                        "4-hijklmn",
                                        "5",
                                        "5-hijklmn",
                                        "6",
                                        "7"});
        int64_t deleted_count;
        ASSERT_EQ(PERR_OK, client_->multi_del("basic_test_multi_get", sortkeys, deleted_count));
        ASSERT_EQ(13, deleted_count);
    }

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_multi_get", count));
    ASSERT_EQ(0, count);
}

TEST_F(basic, multi_get_reverse)
{
    // multi_set
    const std::map<std::string, std::string> kvs({{"", "0"},
                                                  {"1", "1"},
                                                  {"1-abcdefg", "1-abcdefg"},
                                                  {"2", "2"},
                                                  {"2-abcdefg", "2-abcdefg"},
                                                  {"3", "3"},
                                                  {"3-efghijk", "3-efghijk"},
                                                  {"4", "4"},
                                                  {"4-hijklmn", "4-hijklmn"},
                                                  {"5", "5"},
                                                  {"5-hijklmn", "5-hijklmn"},
                                                  {"6", "6"},
                                                  {"7", "7"}});
    ASSERT_EQ(PERR_OK, client_->multi_set("basic_test_multi_get_reverse", kvs));

    // sortkey_count
    int64_t count;
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_multi_get_reverse", count));
    ASSERT_EQ(13, count);

    // [null, null)
    {
        pegasus::pegasus_client::multi_get_options options;
        ASSERT_TRUE(options.start_inclusive);
        ASSERT_FALSE(options.stop_inclusive);
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        auto expect_kvs(kvs);
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [null, null]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        auto expect_kvs(kvs);
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, null)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        auto expect_kvs(kvs);
        expect_kvs.erase("");
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, null]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        auto expect_kvs(kvs);
        expect_kvs.erase("");
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [null, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"", "0"}, {"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [null, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"", "0"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // (null, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // [1, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // [1, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // (1, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // (1, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = false;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // [2, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "2", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-anywhere("-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "-";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        const std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"},
                                                             {"2-abcdefg", "2-abcdefg"},
                                                             {"3-efghijk", "3-efghijk"},
                                                             {"4-hijklmn", "4-hijklmn"},
                                                             {"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "1";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "1-";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-anywhere("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        options.sort_key_filter_pattern = "abc";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        const std::map<std::string, std::string> expect_kvs(
            {{"1-abcdefg", "1-abcdefg"}, {"2-abcdefg", "2-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in [0, 1)
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = false;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "0", "1", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("1") in [0, 1]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "0", "1", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in [1, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "2", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}, {"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in (1, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = false;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_get("basic_test_multi_get_reverse", "1", "2", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1") in (1-abcdefg, 2]
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1";
        options.start_inclusive = false;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get(
                      "basic_test_multi_get_reverse", "1-abcdefg", "2", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1-";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1-abcdefg", "1-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-prefix("1-x")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "1-x";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "abc";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("efg")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "efg";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("ijk")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "ijk";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("lmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "lmn";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-prefix("5-hijklmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "5-hijklmn";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("1")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"1", "1"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("1-")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1-";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("1-x")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "1-x";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("abc")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "abc";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        ASSERT_TRUE(new_values.empty());
    }

    // match-postfix("efg")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "efg";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs(
            {{"1-abcdefg", "1-abcdefg"}, {"2-abcdefg", "2-abcdefg"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("ijk")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "ijk";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"3-efghijk", "3-efghijk"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("lmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "lmn";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs(
            {{"4-hijklmn", "4-hijklmn"}, {"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // match-postfix("5-hijklmn")
    {
        pegasus::pegasus_client::multi_get_options options;
        options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        options.sort_key_filter_pattern = "5-hijklmn";
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values));
        std::map<std::string, std::string> expect_kvs({{"5-hijklmn", "5-hijklmn"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // maxCount = 4
    {
        pegasus::pegasus_client::multi_get_options options;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(
            PERR_INCOMPLETE,
            client_->multi_get("basic_test_multi_get_reverse", "", "", options, new_values, 4, -1));
        std::map<std::string, std::string> expect_kvs(
            {{"5", "5"}, {"5-hijklmn", "5-hijklmn"}, {"6", "6"}, {"7", "7"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // maxCount = 1
    {
        pegasus::pegasus_client::multi_get_options options;
        options.start_inclusive = true;
        options.stop_inclusive = true;
        options.reverse = true;
        std::map<std::string, std::string> new_values;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get(
                      "basic_test_multi_get_reverse", "5", "6", options, new_values, 1, -1));
        std::map<std::string, std::string> expect_kvs({{"6", "6"}});
        ASSERT_EQ(expect_kvs, new_values);
    }

    // multi_del
    {
        const std::set<std::string> sortkeys_to_delete({"",
                                                        "1",
                                                        "1-abcdefg",
                                                        "2",
                                                        "2-abcdefg",
                                                        "3",
                                                        "3-efghijk",
                                                        "4",
                                                        "4-hijklmn",
                                                        "5",
                                                        "5-hijklmn",
                                                        "6",
                                                        "7"});
        int64_t deleted_count;
        ASSERT_EQ(
            PERR_OK,
            client_->multi_del("basic_test_multi_get_reverse", sortkeys_to_delete, deleted_count));
        ASSERT_EQ(13, deleted_count);
    }

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_multi_get_reverse", count));
    ASSERT_EQ(0, count);
}

TEST_F(basic, multi_set_get_del)
{
    // multi_set
    const std::map<std::string, std::string> kvs({{"basic_test_sort_key_1", "basic_test_value_1"},
                                                  {"basic_test_sort_key_2", "basic_test_value_2"},
                                                  {"basic_test_sort_key_3", "basic_test_value_3"},
                                                  {"basic_test_sort_key_4", "basic_test_value_4"}});
    ASSERT_EQ(PERR_OK, client_->multi_set("basic_test_hash_key_1", kvs));

    // sortkey_count
    int64_t count;
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(4, count);

    const std::set<std::string> expect_sortkeys({"basic_test_sort_key_1",
                                                 "basic_test_sort_key_2",
                                                 "basic_test_sort_key_3",
                                                 "basic_test_sort_key_4"});

    // multi_get
    const std::set<std::string> sortkeys({"basic_test_sort_key_0",
                                          "basic_test_sort_key_1",
                                          "basic_test_sort_key_2",
                                          "basic_test_sort_key_3",
                                          "basic_test_sort_key_4"});
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_hash_key_1", sortkeys, actual_kvs));
        ASSERT_EQ(kvs, actual_kvs);
    }

    // multi_get with limit count 4
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_hash_key_1", sortkeys, actual_kvs, 4));
        ASSERT_EQ(kvs, actual_kvs);
    }

    // multi_get with limit count 3
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get("basic_test_hash_key_1", sortkeys, actual_kvs, 3));
        auto expect_kvs(kvs);
        expect_kvs.erase("basic_test_sort_key_4");
        ASSERT_EQ(expect_kvs, actual_kvs);
    }

    // multi_get with limit count 1
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get("basic_test_hash_key_1", sortkeys, actual_kvs, 1));
        std::map<std::string, std::string> expect_kvs(
            {{"basic_test_sort_key_1", "basic_test_value_1"}});
        ASSERT_EQ(expect_kvs, actual_kvs);
    }

    // multi_get with empty sortkeys
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_hash_key_1", {}, actual_kvs));
        ASSERT_EQ(kvs, actual_kvs);
    }

    // multi_get_sortkeys with no limit count
    {
        std::set<std::string> actual_sortkeys;
        ASSERT_EQ(PERR_OK,
                  client_->multi_get_sortkeys("basic_test_hash_key_1", actual_sortkeys, -1));
        ASSERT_EQ(expect_sortkeys, actual_sortkeys);
    }

    // multi_get_sortkeys with limit count
    {
        std::set<std::string> actual_sortkeys;
        ASSERT_EQ(PERR_INCOMPLETE,
                  client_->multi_get_sortkeys("basic_test_hash_key_1", actual_sortkeys, 1));
        std::set<std::string> expect_sortkeys({"basic_test_sort_key_1"});
        ASSERT_EQ(expect_sortkeys, actual_sortkeys);
    }

    // multi_del with empty sortkeys
    int64_t deleted_count;
    {
        ASSERT_EQ(PERR_INVALID_VALUE,
                  client_->multi_del("basic_test_hash_key_1", {}, deleted_count));
        ASSERT_EQ(0, deleted_count);
    }

    // multi_del
    {
        std::set<std::string> sortkeys_to_delete(
            {"basic_test_sort_key_0", "basic_test_sort_key_1", "basic_test_sort_key_2"});
        ASSERT_EQ(PERR_OK,
                  client_->multi_del("basic_test_hash_key_1", sortkeys_to_delete, deleted_count));
        ASSERT_EQ(3, deleted_count);
    }

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(2, count);

    // check deleted
    {
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_hash_key_1", {}, actual_kvs));
        std::map<std::string, std::string> expect_kvs(
            {{"basic_test_sort_key_3", "basic_test_value_3"},
             {"basic_test_sort_key_4", "basic_test_value_4"}});
        ASSERT_EQ(expect_kvs, actual_kvs);
    }

    // multi_del
    {
        std::set<std::string> sortkeys_to_delete(
            {"basic_test_sort_key_3", "basic_test_sort_key_4"});
        ASSERT_EQ(PERR_OK,
                  client_->multi_del("basic_test_hash_key_1", sortkeys_to_delete, deleted_count));
        ASSERT_EQ(2, deleted_count);
    }

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(0, count);
}

TEST_F(basic, set_get_del_async)
{
    std::string new_value;

    // set_async
    {
        std::atomic<bool> callbacked(false);
        client_->async_set("basic_test_hash_key_1",
                           "basic_test_sort_key_1",
                           "basic_test_value_1",
                           [&](int err, internal_info &&info) {
                               ASSERT_EQ(PERR_OK, err);
                               ASSERT_GT(info.app_id, 0);
                               ASSERT_GT(info.partition_index, 0);
                               ASSERT_GT(info.decree, 0);
                               ASSERT_FALSE(info.server.empty());
                               callbacked.store(true, std::memory_order_seq_cst);
                           });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // exist
    ASSERT_EQ(PERR_OK, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_1"));
    ASSERT_EQ(PERR_NOT_FOUND, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_2"));

    // sortkey_count
    int64_t count;
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(1, count);

    // get_async
    {
        std::atomic<bool> callbacked(false);
        client_->async_get("basic_test_hash_key_1",
                           "basic_test_sort_key_1",
                           [&](int err, std::string &&value, internal_info &&info) {
                               ASSERT_EQ(PERR_OK, err);
                               ASSERT_GT(info.app_id, 0);
                               ASSERT_GT(info.partition_index, 0);
                               ASSERT_EQ(info.decree, -1);
                               ASSERT_FALSE(info.server.empty());
                               ASSERT_EQ("basic_test_value_1", value);
                               callbacked.store(true, std::memory_order_seq_cst);
                           });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    {
        std::atomic<bool> callbacked(false);
        client_->async_get("basic_test_hash_key_1",
                           "basic_test_sort_key_2",
                           [&](int err, std::string &&value, internal_info &&info) {
                               ASSERT_EQ(PERR_NOT_FOUND, err);
                               ASSERT_GT(info.app_id, 0);
                               ASSERT_GT(info.partition_index, 0);
                               ASSERT_EQ(info.decree, -1);
                               ASSERT_FALSE(info.server.empty());
                               callbacked.store(true, std::memory_order_seq_cst);
                           });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // del_async
    {
        std::atomic<bool> callbacked(false);
        client_->async_del(
            "basic_test_hash_key_1", "basic_test_sort_key_1", [&](int err, internal_info &&info) {
                ASSERT_EQ(PERR_OK, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_GT(info.decree, 0);
                ASSERT_FALSE(info.server.empty());
                callbacked.store(true, std::memory_order_seq_cst);
            });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // exist
    ASSERT_EQ(PERR_NOT_FOUND, client_->exist("basic_test_hash_key_1", "basic_test_sort_key_1"));

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(0, count);

    // get -- finally, using get_sync to get the key-value.
    ASSERT_EQ(PERR_NOT_FOUND,
              client_->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value));
}

TEST_F(basic, multi_set_get_del_async)
{
    std::map<std::string, std::string> actual_kvs;
    int64_t count;

    const std::map<std::string, std::string> kvs({{"basic_test_sort_key_1", "basic_test_value_1"},
                                                  {"basic_test_sort_key_2", "basic_test_value_2"},
                                                  {"basic_test_sort_key_3", "basic_test_value_3"},
                                                  {"basic_test_sort_key_4", "basic_test_value_4"}});
    const std::set<std::string> expect_sortkeys({"basic_test_sort_key_1",
                                                 "basic_test_sort_key_2",
                                                 "basic_test_sort_key_3",
                                                 "basic_test_sort_key_4"});
    const std::set<std::string> sortkeys_to_get({"basic_test_sort_key_0",
                                                 "basic_test_sort_key_1",
                                                 "basic_test_sort_key_2",
                                                 "basic_test_sort_key_3",
                                                 "basic_test_sort_key_4"});

    // multi_set_async
    {
        std::atomic<bool> callbacked(false);
        client_->async_multi_set("basic_test_hash_key_1", kvs, [&](int err, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_GT(info.decree, 0);
            ASSERT_FALSE(info.server.empty());
            callbacked.store(true, std::memory_order_seq_cst);
        });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);

        // sortkey_count
        ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
        ASSERT_EQ(4, count);
    }

    // multi_get_async
    {
        std::atomic<bool> callbacked(false);
        client_->async_multi_get(
            "basic_test_hash_key_1",
            sortkeys_to_get,
            [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
                ASSERT_EQ(PERR_OK, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_EQ(info.decree, -1);
                ASSERT_FALSE(info.server.empty());
                ASSERT_EQ(kvs, values);
                callbacked.store(true, std::memory_order_seq_cst);
            });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_get_async with limit count
    {
        std::atomic<bool> callbacked(false);
        client_->async_multi_get(
            "basic_test_hash_key_1",
            sortkeys_to_get,
            [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
                ASSERT_EQ(PERR_INCOMPLETE, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_EQ(info.decree, -1);
                ASSERT_FALSE(info.server.empty());
                std::map<std::string, std::string> expect_kvs(
                    {{"basic_test_sort_key_1", "basic_test_value_1"}});
                ASSERT_EQ(expect_kvs, values);
                callbacked.store(true, std::memory_order_seq_cst);
            },
            1);
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_get with empty sortkeys
    {
        std::atomic<bool> callbacked(false);
        client_->async_multi_get(
            "basic_test_hash_key_1",
            {},
            [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
                ASSERT_EQ(PERR_OK, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_EQ(info.decree, -1);
                ASSERT_FALSE(info.server.empty());
                ASSERT_EQ(kvs, values);
                callbacked.store(true, std::memory_order_seq_cst);
            });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_get_sortkeys_async with limit count
    {
        std::atomic<bool> callbacked(false);
        const std::set<std::string> expect_sortkeys({"basic_test_sort_key_1"});
        std::set<std::string> actual_sortkeys;
        client_->async_multi_get_sortkeys(
            "basic_test_hash_key_1",
            [&](int err, std::set<std::string> &&actual_sortkeys, internal_info &&info) {
                ASSERT_EQ(PERR_INCOMPLETE, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_EQ(info.decree, -1);
                ASSERT_FALSE(info.server.empty());
                ASSERT_EQ(expect_sortkeys, actual_sortkeys);
                callbacked.store(true, std::memory_order_seq_cst);
            },
            1);
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_get_sortkeys_async with no limit count
    {
        std::atomic<bool> callbacked(false);
        std::set<std::string> actual_sortkeys;
        client_->async_multi_get_sortkeys(
            "basic_test_hash_key_1",
            [&](int err, std::set<std::string> &&actual_sortkeys, internal_info &&info) {
                ASSERT_EQ(PERR_OK, err);
                ASSERT_GT(info.app_id, 0);
                ASSERT_GT(info.partition_index, 0);
                ASSERT_EQ(info.decree, -1);
                ASSERT_FALSE(info.server.empty());
                ASSERT_EQ(expect_sortkeys, actual_sortkeys);
                callbacked.store(true, std::memory_order_seq_cst);
            },
            -1);
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_del_async with empty sortkeys
    {
        std::atomic<bool> callbacked(false);
        client_->async_multi_del(
            "basic_test_hash_key_1", {}, [&](int err, int64_t deleted_count, internal_info &&info) {
                ASSERT_EQ(PERR_INVALID_VALUE, err);
                callbacked.store(true, std::memory_order_seq_cst);
            });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // multi_del_async
    {
        std::atomic<bool> callbacked(false);
        std::set<std::string> sortkeys_to_delete(
            {"basic_test_sort_key_0", "basic_test_sort_key_1", "basic_test_sort_key_2"});
        client_->async_multi_del("basic_test_hash_key_1",
                                 sortkeys_to_delete,
                                 [&](int err, int64_t deleted_count, internal_info &&info) {
                                     ASSERT_EQ(PERR_OK, err);
                                     ASSERT_GT(info.app_id, 0);
                                     ASSERT_GT(info.partition_index, 0);
                                     ASSERT_GT(info.decree, 0);
                                     ASSERT_FALSE(info.server.empty());
                                     ASSERT_EQ(3, deleted_count);
                                     callbacked.store(true, std::memory_order_seq_cst);
                                 });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);

        // sortkey_count
        ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
        ASSERT_EQ(2, count);

        // check deleted  --- using multi_get to check.
        std::map<std::string, std::string> actual_kvs;
        ASSERT_EQ(PERR_OK, client_->multi_get("basic_test_hash_key_1", {}, actual_kvs));
        std::map<std::string, std::string> expect_kvs(
            {{"basic_test_sort_key_3", "basic_test_value_3"},
             {"basic_test_sort_key_4", "basic_test_value_4"}});
        ASSERT_EQ(expect_kvs, actual_kvs);
    }

    // multi_del_async
    {
        std::atomic<bool> callbacked(false);
        std::set<std::string> sortkeys_to_delete(
            {"basic_test_sort_key_3", "basic_test_sort_key_4"});
        client_->async_multi_del("basic_test_hash_key_1",
                                 sortkeys_to_delete,
                                 [&](int err, int64_t deleted_count, internal_info &&info) {
                                     ASSERT_EQ(PERR_OK, err);
                                     ASSERT_GT(info.app_id, 0);
                                     ASSERT_GT(info.partition_index, 0);
                                     ASSERT_GT(info.decree, 0);
                                     ASSERT_FALSE(info.server.empty());
                                     ASSERT_EQ(2, deleted_count);
                                     callbacked.store(true, std::memory_order_seq_cst);
                                 });
        while (!callbacked.load(std::memory_order_seq_cst))
            usleep(100);
    }

    // sortkey_count
    ASSERT_EQ(PERR_OK, client_->sortkey_count("basic_test_hash_key_1", count));
    ASSERT_EQ(0, count);
}

TEST_F(basic, scan_with_filter)
{
    int ret = 0;
    const std::map<std::string, std::string> kvs({{"m_1", "a"},
                                                  {"m_2", "a"},
                                                  {"m_3", "a"},
                                                  {"m_4", "a"},
                                                  {"m_5", "a"},
                                                  {"n_1", "b"},
                                                  {"n_2", "b"},
                                                  {"n_3", "b"}});
    const std::map<std::string, std::string> expect_kvs_prefixed_by_m(
        {{"m_1", "a"}, {"m_2", "a"}, {"m_3", "a"}, {"m_4", "a"}, {"m_5", "a"}});

    // multi_set
    ASSERT_EQ(PERR_OK, client_->multi_set("xyz", kvs));

    // scan with batch_size = 10
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 10;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ASSERT_EQ(PERR_OK, client_->get_scanner("xyz", "", "", options, scanner));
        ASSERT_NE(nullptr, scanner);
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            ASSERT_EQ("a", value);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(expect_kvs_prefixed_by_m, data);
    }

    // scan with batch_size = 3
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 3;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ASSERT_EQ(PERR_OK, client_->get_scanner("xyz", "", "", options, scanner));
        ASSERT_NE(nullptr, scanner);
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            ASSERT_EQ("a", value);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(expect_kvs_prefixed_by_m, data);
    }

    // scan with batch_size = 10
    {
        pegasus_client::scan_options options;
        options.hash_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.hash_key_filter_pattern = "xy";
        options.batch_size = 10;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ASSERT_EQ(PERR_OK, client_->get_scanner("xyz", "", "", options, scanner));
        ASSERT_NE(nullptr, scanner);
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(kvs, data);
    }

    // multi_del
    std::set<std::string> sortkeys;
    for (auto kv : kvs) {
        sortkeys.insert(kv.first);
    }
    int64_t deleted_count;
    ASSERT_EQ(PERR_OK, client_->multi_del("x", sortkeys, deleted_count));
    ASSERT_EQ(8, deleted_count);
}

TEST_F(basic, full_scan_with_filter)
{
    int ret = 0;
    // multi_set
    const std::map<std::string, std::string> kvs({{"m_1", "a"},
                                                  {"m_2", "a"},
                                                  {"m_3", "a"},
                                                  {"m_4", "a"},
                                                  {"m_5", "a"},
                                                  {"n_1", "b"},
                                                  {"n_2", "b"},
                                                  {"n_3", "b"}});
    ASSERT_EQ(PERR_OK, client_->multi_set("xyz", kvs));

    const std::map<std::string, std::string> expect_kvs_prefixed_by_m(
        {{"m_1", "a"}, {"m_2", "a"}, {"m_3", "a"}, {"m_4", "a"}, {"m_5", "a"}});

    // scan with sort key filter and batch_size = 10
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 10;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(1, options, scanners));
        ASSERT_EQ(1, scanners.size());
        pegasus_client::pegasus_scanner *scanner = scanners[0];
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            ASSERT_EQ("a", value);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(expect_kvs_prefixed_by_m, data);
    }

    // scan with sort key filter and batch_size = 3
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 3;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(1, options, scanners));
        ASSERT_EQ(1, scanners.size());
        pegasus_client::pegasus_scanner *scanner = scanners[0];
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            ASSERT_EQ("a", value);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(expect_kvs_prefixed_by_m, data);
    }

    // scan with hash key filter and batch_size = 10
    {
        pegasus_client::scan_options options;
        options.hash_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.hash_key_filter_pattern = "xy";
        options.batch_size = 10;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(1, options, scanners));
        ASSERT_EQ(1, scanners.size());
        pegasus_client::pegasus_scanner *scanner = scanners[0];
        std::map<std::string, std::string> data;
        std::string hash_key;
        std::string sort_key;
        std::string value;
        while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
            ASSERT_EQ("xyz", hash_key);
            data[sort_key] = value;
        }
        delete scanner;
        ASSERT_EQ(kvs, data);
    }

    // multi_del
    std::set<std::string> sortkeys;
    for (auto kv : kvs) {
        sortkeys.insert(kv.first);
    }
    int64_t deleted_count;
    ASSERT_EQ(PERR_OK, client_->multi_del("x", sortkeys, deleted_count));
    ASSERT_EQ(8, deleted_count);
}
