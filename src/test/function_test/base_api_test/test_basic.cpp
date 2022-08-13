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

#include <dsn/service_api_c.h>
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
    ASSERT_STREQ("mycluster", client->get_cluster_name());

    // set
    int ret = client->set("basic_test_hash_key_1", "basic_test_sort_key_1", "basic_test_value_1");
    ASSERT_EQ(PERR_OK, ret);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_2");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, count);

    // get
    std::string new_value_str;
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ("basic_test_value_1", new_value_str);

    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_2", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // del
    ret = client->del("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);

    // get
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);
}

TEST_F(basic, multi_get)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs[""] = "0";
    kvs["1"] = "1";
    kvs["1-abcdefg"] = "1-abcdefg";
    kvs["2"] = "2";
    kvs["2-abcdefg"] = "2-abcdefg";
    kvs["3"] = "3";
    kvs["3-efghijk"] = "3-efghijk";
    kvs["4"] = "4";
    kvs["4-hijklmn"] = "4-hijklmn";
    kvs["5"] = "5";
    kvs["5-hijklmn"] = "5-hijklmn";
    kvs["6"] = "6";
    kvs["7"] = "7";
    int ret = client->multi_set("basic_test_multi_get", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_multi_get", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, count);

    // [null, null)
    pegasus::pegasus_client::multi_get_options options;
    ASSERT_TRUE(options.start_inclusive);
    ASSERT_FALSE(options.stop_inclusive);
    std::map<std::string, std::string> new_values;
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // [null, null]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // (null, null)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(12, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // (null, null]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(12, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // [null, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);

    // [null, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);

    // (null, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // (null, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // [1, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // [1, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // (1, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // (1, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // [2, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "2", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-anywhere("-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "-";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(5, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-anywhere("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "1";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-anywhere("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "1-";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-anywhere("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "abc";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);

    // match-prefix("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in [0, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = false;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "0", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("1") in [0, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "0", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // match-prefix("1") in [1, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in (1, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = false;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in (1-abcdefg, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = false;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "1-abcdefg", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1-";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1-x")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1-x";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "abc";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("efg")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "efg";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("ijk")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "ijk";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("lmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "lmn";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("5-hijklmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "5-hijklmn";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-postfix("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // match-postfix("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1-";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("1-x")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1-x";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "abc";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("efg")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "efg";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);

    // match-postfix("ijk")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "ijk";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);

    // match-postfix("lmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "lmn";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-postfix("5-hijklmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "5-hijklmn";
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // maxCount = 4
    options = pegasus::pegasus_client::multi_get_options();
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values, 4, -1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(4, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);

    // maxCount = 1
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "5", "6", options, new_values, 1, -1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("5", new_values["5"]);

    // set a expired value
    ret = client->set("basic_test_multi_get", "", "expire_value", 5000, 1);
    ASSERT_EQ(PERR_OK, ret);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get", "", "", options, new_values, 2);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // multi_del
    std::set<std::string> sortkeys;
    sortkeys.insert("");
    sortkeys.insert("1");
    sortkeys.insert("1-abcdefg");
    sortkeys.insert("2");
    sortkeys.insert("2-abcdefg");
    sortkeys.insert("3");
    sortkeys.insert("3-efghijk");
    sortkeys.insert("4");
    sortkeys.insert("4-hijklmn");
    sortkeys.insert("5");
    sortkeys.insert("5-hijklmn");
    sortkeys.insert("6");
    sortkeys.insert("7");
    int64_t deleted_count;
    ret = client->multi_del("basic_test_multi_get", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_multi_get", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

TEST_F(basic, multi_get_reverse)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs[""] = "0";
    kvs["1"] = "1";
    kvs["1-abcdefg"] = "1-abcdefg";
    kvs["2"] = "2";
    kvs["2-abcdefg"] = "2-abcdefg";
    kvs["3"] = "3";
    kvs["3-efghijk"] = "3-efghijk";
    kvs["4"] = "4";
    kvs["4-hijklmn"] = "4-hijklmn";
    kvs["5"] = "5";
    kvs["5-hijklmn"] = "5-hijklmn";
    kvs["6"] = "6";
    kvs["7"] = "7";
    int ret = client->multi_set("basic_test_multi_get_reverse", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_multi_get_reverse", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, count);

    // [null, null)
    pegasus::pegasus_client::multi_get_options options;
    ASSERT_TRUE(options.start_inclusive);
    ASSERT_FALSE(options.stop_inclusive);
    options.reverse = true;
    std::map<std::string, std::string> new_values;
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // [null, null]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // (null, null)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(12, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // (null, null]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(12, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2", new_values["2"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3", new_values["3"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4", new_values["4"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // [null, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);
    ASSERT_EQ("1", new_values["1"]);

    // [null, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("0", new_values[""]);

    // (null, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // (null, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // [1, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // [1, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // (1, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // (1, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = false;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // [2, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "2", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-anywhere("-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "-";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(5, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-anywhere("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "1";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-anywhere("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "1-";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-anywhere("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
    options.sort_key_filter_pattern = "abc";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);

    // match-prefix("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in [0, 1)
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = false;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "0", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("1") in [0, 1]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "0", "1", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // match-prefix("1") in [1, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in (1, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = false;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1") in (1-abcdefg, 2]
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1";
    options.start_inclusive = false;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "1-abcdefg", "2", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1-";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);

    // match-prefix("1-x")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "1-x";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "abc";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("efg")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "efg";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("ijk")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "ijk";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("lmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "lmn";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-prefix("5-hijklmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
    options.sort_key_filter_pattern = "5-hijklmn";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-postfix("1")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("1", new_values["1"]);

    // match-postfix("1-")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1-";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("1-x")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "1-x";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("abc")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "abc";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, (int)new_values.size());

    // match-postfix("efg")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "efg";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("1-abcdefg", new_values["1-abcdefg"]);
    ASSERT_EQ("2-abcdefg", new_values["2-abcdefg"]);

    // match-postfix("ijk")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "ijk";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("3-efghijk", new_values["3-efghijk"]);

    // match-postfix("lmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "lmn";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, (int)new_values.size());
    ASSERT_EQ("4-hijklmn", new_values["4-hijklmn"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // match-postfix("5-hijklmn")
    options = pegasus::pegasus_client::multi_get_options();
    options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_POSTFIX;
    options.sort_key_filter_pattern = "5-hijklmn";
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);

    // maxCount = 4
    options = pegasus::pegasus_client::multi_get_options();
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "", "", options, new_values, 4, -1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(4, (int)new_values.size());
    ASSERT_EQ("5", new_values["5"]);
    ASSERT_EQ("5-hijklmn", new_values["5-hijklmn"]);
    ASSERT_EQ("6", new_values["6"]);
    ASSERT_EQ("7", new_values["7"]);

    // maxCount = 1
    options = pegasus::pegasus_client::multi_get_options();
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.reverse = true;
    new_values.clear();
    ret = client->multi_get("basic_test_multi_get_reverse", "5", "6", options, new_values, 1, -1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, (int)new_values.size());
    ASSERT_EQ("6", new_values["6"]);

    // multi_del
    std::set<std::string> sortkeys;
    sortkeys.insert("");
    sortkeys.insert("1");
    sortkeys.insert("1-abcdefg");
    sortkeys.insert("2");
    sortkeys.insert("2-abcdefg");
    sortkeys.insert("3");
    sortkeys.insert("3-efghijk");
    sortkeys.insert("4");
    sortkeys.insert("4-hijklmn");
    sortkeys.insert("5");
    sortkeys.insert("5-hijklmn");
    sortkeys.insert("6");
    sortkeys.insert("7");
    int64_t deleted_count;
    ret = client->multi_del("basic_test_multi_get_reverse", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(13, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_multi_get_reverse", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

TEST_F(basic, multi_set_get_del)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs["basic_test_sort_key_1"] = "basic_test_value_1";
    kvs["basic_test_sort_key_2"] = "basic_test_value_2";
    kvs["basic_test_sort_key_3"] = "basic_test_value_3";
    kvs["basic_test_sort_key_4"] = "basic_test_value_4";
    int ret = client->multi_set("basic_test_hash_key_1", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, count);

    // multi_get
    std::set<std::string> sortkeys;
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    std::map<std::string, std::string> new_kvs;
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, new_kvs.size());
    auto it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_get with limit count 4
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs, 4);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_get with limit count 3
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs, 3);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(3, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);

    // multi_get with limit count 1
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs, 1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);

    // multi_get with empty sortkeys
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_get_sortkeys with no limit count
    sortkeys.clear();
    ret = client->multi_get_sortkeys("basic_test_hash_key_1", sortkeys, -1);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, sortkeys.size());
    auto it2 = sortkeys.begin();
    ASSERT_EQ("basic_test_sort_key_1", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_2", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_3", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_4", *it2);

    // multi_get_sortkeys with limit count
    sortkeys.clear();
    ret = client->multi_get_sortkeys("basic_test_hash_key_1", sortkeys, 1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, sortkeys.size());
    it2 = sortkeys.begin();
    ASSERT_EQ("basic_test_sort_key_1", *it2);

    // multi_del with empty sortkeys
    sortkeys.clear();
    int64_t deleted_count;
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_INVALID_VALUE, ret);

    // multi_del
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(3, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, count);

    // check deleted
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_del
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

TEST_F(basic, set_get_del_async)
{
    std::atomic<bool> callbacked(false);
    int ret = 0;
    std::string new_value_str;
    // set_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_set("basic_test_hash_key_1",
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

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_2");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, count);

    // get_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_get("basic_test_hash_key_1",
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

    callbacked.store(false, std::memory_order_seq_cst);
    client->async_get("basic_test_hash_key_1",
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

    // del_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_del(
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

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);

    // get -- finally, using get_sync to get the key-value.
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);
}

TEST_F(basic, multi_set_get_del_async)
{
    std::atomic<bool> callbacked(false);
    int ret = 0;
    std::map<std::string, std::string> new_kvs;
    // multi_set_async
    std::map<std::string, std::string> kvs;
    kvs["basic_test_sort_key_1"] = "basic_test_value_1";
    kvs["basic_test_sort_key_2"] = "basic_test_value_2";
    kvs["basic_test_sort_key_3"] = "basic_test_value_3";
    kvs["basic_test_sort_key_4"] = "basic_test_value_4";
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_set("basic_test_hash_key_1", kvs, [&](int err, internal_info &&info) {
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
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, count);

    // multi_get_async
    std::set<std::string> sortkeys;
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");

    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", it->first);
            ASSERT_EQ("basic_test_value_2", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", it->first);
            ASSERT_EQ("basic_test_value_3", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", it->first);
            ASSERT_EQ("basic_test_value_4", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_async with limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_INCOMPLETE, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(1, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get with empty sortkeys
    sortkeys.clear();
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", it->first);
            ASSERT_EQ("basic_test_value_2", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", it->first);
            ASSERT_EQ("basic_test_value_3", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", it->first);
            ASSERT_EQ("basic_test_value_4", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_sortkeys_async with limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get_sortkeys(
        "basic_test_hash_key_1",
        [&](int err, std::set<std::string> &&sortkeys, internal_info &&info) {
            ASSERT_EQ(PERR_INCOMPLETE, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(1, sortkeys.size());
            auto it = sortkeys.begin();
            ASSERT_EQ("basic_test_sort_key_1", *it);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_sortkeys_async with no limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get_sortkeys(
        "basic_test_hash_key_1",
        [&](int err, std::set<std::string> &&sortkeys, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, sortkeys.size());
            auto it = sortkeys.begin();
            ASSERT_EQ("basic_test_sort_key_1", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", *it);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        -1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_del_async with empty sortkeys
    sortkeys.clear();
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
                            [&](int err, int64_t deleted_count, internal_info &&info) {
                                ASSERT_EQ(PERR_INVALID_VALUE, err);
                                callbacked.store(true, std::memory_order_seq_cst);
                            });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_del_async
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
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
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, count);

    // check deleted  --- using multi_get to check.
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, new_kvs.size());
    auto it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_del_async
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
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

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

TEST_F(basic, scan_with_filter)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs["m_1"] = "a";
    kvs["m_2"] = "a";
    kvs["m_3"] = "a";
    kvs["m_4"] = "a";
    kvs["m_5"] = "a";
    kvs["n_1"] = "b";
    kvs["n_2"] = "b";
    kvs["n_3"] = "b";
    int ret = client->multi_set("xyz", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // scan with batch_size = 10
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 10;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ret = client->get_scanner("xyz", "", "", options, scanner);
        ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                          << client->get_error_string(ret);
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
        ASSERT_EQ(5, data.size());
        ASSERT_NE(data.end(), data.find("m_1"));
        ASSERT_NE(data.end(), data.find("m_2"));
        ASSERT_NE(data.end(), data.find("m_3"));
        ASSERT_NE(data.end(), data.find("m_4"));
        ASSERT_NE(data.end(), data.find("m_5"));
    }

    // scan with batch_size = 3
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 3;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ret = client->get_scanner("xyz", "", "", options, scanner);
        ASSERT_EQ(PERR_OK, ret);
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
        ASSERT_EQ(5, data.size());
        ASSERT_NE(data.end(), data.find("m_1"));
        ASSERT_NE(data.end(), data.find("m_2"));
        ASSERT_NE(data.end(), data.find("m_3"));
        ASSERT_NE(data.end(), data.find("m_4"));
        ASSERT_NE(data.end(), data.find("m_5"));
    }

    // scan with batch_size = 10
    {
        pegasus_client::scan_options options;
        options.hash_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.hash_key_filter_pattern = "xy";
        options.batch_size = 10;
        pegasus_client::pegasus_scanner *scanner = nullptr;
        ret = client->get_scanner("xyz", "", "", options, scanner);
        ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                          << client->get_error_string(ret);
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
    ret = client->multi_del("x", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(8, deleted_count);
}

TEST_F(basic, full_scan_with_filter)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs["m_1"] = "a";
    kvs["m_2"] = "a";
    kvs["m_3"] = "a";
    kvs["m_4"] = "a";
    kvs["m_5"] = "a";
    kvs["n_1"] = "b";
    kvs["n_2"] = "b";
    kvs["n_3"] = "b";
    int ret = client->multi_set("xyz", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // scan with sort key filter and batch_size = 10
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 10;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ret = client->get_unordered_scanners(1, options, scanners);
        ASSERT_EQ(PERR_OK, ret);
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
        ASSERT_EQ(5, data.size());
        ASSERT_NE(data.end(), data.find("m_1"));
        ASSERT_NE(data.end(), data.find("m_2"));
        ASSERT_NE(data.end(), data.find("m_3"));
        ASSERT_NE(data.end(), data.find("m_4"));
        ASSERT_NE(data.end(), data.find("m_5"));
    }

    // scan with sort key filter and batch_size = 3
    {
        pegasus_client::scan_options options;
        options.sort_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.sort_key_filter_pattern = "m";
        options.batch_size = 3;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ret = client->get_unordered_scanners(1, options, scanners);
        ASSERT_EQ(PERR_OK, ret);
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
        ASSERT_EQ(5, data.size());
        ASSERT_NE(data.end(), data.find("m_1"));
        ASSERT_NE(data.end(), data.find("m_2"));
        ASSERT_NE(data.end(), data.find("m_3"));
        ASSERT_NE(data.end(), data.find("m_4"));
        ASSERT_NE(data.end(), data.find("m_5"));
    }

    // scan with hash key filter and batch_size = 10
    {
        pegasus_client::scan_options options;
        options.hash_key_filter_type = pegasus_client::FT_MATCH_PREFIX;
        options.hash_key_filter_pattern = "xy";
        options.batch_size = 10;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        ret = client->get_unordered_scanners(1, options, scanners);
        ASSERT_EQ(PERR_OK, ret);
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
    ret = client->multi_del("x", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(8, deleted_count);
}
