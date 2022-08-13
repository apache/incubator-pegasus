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

#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;

class check_and_mutate : public test_util
{
};

TEST_F(check_and_mutate, value_not_exist)
{
    std::string hash_key("check_and_mutate_test_value_not_exist");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = false;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_FALSE(results.check_value_returned);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->del(hash_key, "k2");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k2", "");
        ret = client->check_and_mutate(hash_key,
                                       "k2",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k2", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        mutations.set("k2", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k2",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k2", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        ret = client->del(hash_key, "k2");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_exist)
{
    std::string hash_key("check_and_mutate_test_value_exist");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_not_empty)
{
    std::string hash_key("check_and_mutate_test_value_not_empty");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        ret = client->set(hash_key, "k1", "v1");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}
TEST_F(check_and_mutate, value_match_anywhere)
{
    std::string hash_key("check_and_mutate_test_value_match_anywhere");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v111v");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "111",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "y",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "v2v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                       "333",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_match_prefix)
{
    std::string hash_key("check_and_mutate_test_value_match_prefix");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v111v");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "111",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v111",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "y",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v2v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                       "v333",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_match_postfix)
{
    std::string hash_key("check_and_mutate_test_value_match_postfix");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v111v");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "111",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "111v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "y",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "2v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        mutations.set("k1", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                       "333v",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_bytes_compare)
{
    std::string hash_key("check_and_mutate_test_value_bytes_compare");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_EQUAL,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_EQUAL,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_EQUAL,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        mutations.set("k1", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_EQUAL,
                                       "v1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_EQUAL,
                                       "v3",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k5", "v1");
        ASSERT_EQ(PERR_OK, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        // v1 < v2
        options.return_check_value = true;
        mutations.set("k5", "v2");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_LESS,
                                       "v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        // v2 <= v2
        options.return_check_value = true;
        mutations.set("k5", "v3");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL,
                                       "v2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        // v3 <= v4
        options.return_check_value = true;
        mutations.set("k5", "v4");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_LESS_OR_EQUAL,
                                       "v4",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        // v4 >= v4
        options.return_check_value = true;
        mutations.set("k5", "v5");
        ret = client->check_and_mutate(
            hash_key,
            "k5",
            pegasus_client::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL,
            "v4",
            mutations,
            options,
            results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v4", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v5", value);

        // v5 >= v4
        options.return_check_value = true;
        mutations.set("k5", "v6");
        ret = client->check_and_mutate(
            hash_key,
            "k5",
            pegasus_client::cas_check_type::CT_VALUE_BYTES_GREATER_OR_EQUAL,
            "v4",
            mutations,
            options,
            results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v5", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v6", value);

        // v6 > v5
        options.return_check_value = true;
        mutations.set("k5", "v7");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_BYTES_GREATER,
                                       "v5",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v6", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v7", value);

        ret = client->del(hash_key, "k5");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, value_int_compare)
{
    std::string hash_key("check_and_mutate_test_value_int_compare");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        ret = client->set(hash_key, "k1", "1");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("2", value);

        options.return_check_value = true;
        mutations.set("k1", "3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("2", value);

        options.return_check_value = true;
        mutations.set("k1", "3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "v1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("2", value);

        options.return_check_value = true;
        mutations.set("k1", "3");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "88888888888888888888888888888888888888888888888",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("2", value);

        ret = client->set(hash_key, "k1", "0");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        mutations.set("k1", "-1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "0",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("0", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("-1", value);

        options.return_check_value = true;
        mutations.set("k1", "-2");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "-1",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("-1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("-2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k4", "4");
        ret = client->check_and_mutate(hash_key,
                                       "k3",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_EQUAL,
                                       "3",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k5", "1");
        ASSERT_EQ(PERR_OK, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        // 1 < 2
        options.return_check_value = true;
        mutations.set("k5", "2");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_LESS,
                                       "2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("1", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("2", value);

        // 2 <= 2
        options.return_check_value = true;
        mutations.set("k5", "3");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL,
                                       "2",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("2", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("3", value);

        // 3 <= 4
        options.return_check_value = true;
        mutations.set("k5", "4");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_LESS_OR_EQUAL,
                                       "4",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("3", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("4", value);

        // 4 >= 4
        options.return_check_value = true;
        mutations.set("k5", "5");
        ret =
            client->check_and_mutate(hash_key,
                                     "k5",
                                     pegasus_client::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL,
                                     "4",
                                     mutations,
                                     options,
                                     results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("4", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("5", value);

        // 5 >= 4
        options.return_check_value = true;
        mutations.set("k5", "6");
        ret =
            client->check_and_mutate(hash_key,
                                     "k5",
                                     pegasus_client::cas_check_type::CT_VALUE_INT_GREATER_OR_EQUAL,
                                     "4",
                                     mutations,
                                     options,
                                     results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("5", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("6", value);

        // 6 > 5
        options.return_check_value = true;
        mutations.set("k5", "7");
        ret = client->check_and_mutate(hash_key,
                                       "k5",
                                       pegasus_client::cas_check_type::CT_VALUE_INT_GREATER,
                                       "5",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("6", results.check_value);
        ret = client->get(hash_key, "k5", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("7", value);

        ret = client->del(hash_key, "k5");
        ASSERT_EQ(0, ret);
    }
}

TEST_F(check_and_mutate, invalid_type)
{
    std::string hash_key("check_and_mutate_test_value_invalid_type");

    {
        int ret = 0;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        ret = client->check_and_mutate(
            hash_key, "k1", (pegasus_client::cas_check_type)100, "v", mutations, options, results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.mutate_succeed);
        ASSERT_FALSE(results.check_value_returned);
    }
}

TEST_F(check_and_mutate, set_del)
{
    std::string hash_key("check_and_mutate_test_set_del");

    {
        int ret = 0;
        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1");
        mutations.del("k1");
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);

        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);
    }
}

TEST_F(check_and_mutate, multi_get_mutations)
{
    std::string hash_key("check_and_mutate_test_multi_get_mutations");

    {
        int ret = 0;
        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1", 10);
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);

        ::sleep(12);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);

        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);
    }
}

TEST_F(check_and_mutate, expire_seconds)
{
    std::string hash_key("check_and_mutate_test_expire_seconds");

    {
        int ret = 0;
        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::mutations mutations;
        pegasus_client::check_and_mutate_options options;
        pegasus_client::check_and_mutate_results results;

        options.return_check_value = true;
        mutations.set("k1", "v1", 10);
        ::sleep(12);
        mutations.set("k2", "v2", 10);
        ret = client->check_and_mutate(hash_key,
                                       "k1",
                                       pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                       "",
                                       mutations,
                                       options,
                                       results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.mutate_succeed);
        ASSERT_TRUE(results.check_value_returned);

        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);
    }
}
