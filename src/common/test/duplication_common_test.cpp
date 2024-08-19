/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "common//duplication_common.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/flags.h"

DSN_DECLARE_string(dup_cluster_name);
DSN_DECLARE_bool(dup_ignore_other_cluster_ids);

namespace dsn {
namespace replication {

std::string config_file = "config-test.ini";
std::string unkown_file = "unknown.ini";
std::string err_config_file = "err-config-test.ini";
std::string new_config_file = "new-config-test.ini";


TEST(duplication_common, get_duplication_cluster_id)
{
    ASSERT_EQ(1, get_duplication_cluster_id("master-cluster").get_value());
    ASSERT_EQ(2, get_duplication_cluster_id("slave-cluster").get_value());

    ASSERT_EQ(ERR_INVALID_PARAMETERS, get_duplication_cluster_id("").get_error().code());
    ASSERT_EQ(ERR_OBJECT_NOT_FOUND, get_duplication_cluster_id("unknown").get_error().code());
}

TEST(duplication_common, get_current_dup_cluster_id)
{
    ASSERT_EQ(1, get_current_dup_cluster_id());

    // Current cluster id is static, thus updating dup cluster name should never change
    // current cluster id.
    PRESERVE_FLAG(dup_cluster_name);
    FLAGS_dup_cluster_name = "slave-cluster";
    ASSERT_EQ(1, get_current_dup_cluster_id());
}

TEST(duplication_common, get_distinct_cluster_id_set)
{
    ASSERT_EQ(std::set<uint8_t>({1, 2}), get_distinct_cluster_id_set());
}

TEST(duplication_common, is_dup_cluster_id_configured)
{
    ASSERT_FALSE(is_dup_cluster_id_configured(0));
    ASSERT_TRUE(is_dup_cluster_id_configured(1));
    ASSERT_TRUE(is_dup_cluster_id_configured(2));
    ASSERT_FALSE(is_dup_cluster_id_configured(3));
}

TEST(duplication_common, dup_ignore_other_cluster_ids)
{
    PRESERVE_FLAG(dup_ignore_other_cluster_ids);
    FLAGS_dup_ignore_other_cluster_ids = true;

    for (uint8_t id = 0; id < 4; ++id) {
        ASSERT_TRUE(is_dup_cluster_id_configured(id));
    }
}

TEST(duplication_common, reload_config_file){
    ASSERT_EQ(make_reloading_duplication_config(unkown_file).get_error().code(), ERR_OBJECT_NOT_FOUND);
    ASSERT_EQ(make_reloading_duplication_config(err_config_file).get_error().code(), ERR_INVALID_PARAMETERS);
}


TEST(duplication_common, reload_get_duplication_cluster_id)
{
    make_reloading_duplication_config(config_file);
    ASSERT_EQ(get_duplication_cluster_id("master-cluster").get_value(), 1);
    ASSERT_EQ(get_duplication_cluster_id("slave-cluster").get_value(), 2);
    ASSERT_EQ(get_duplication_cluster_id("strange-cluster").get_error().code(), ERR_OBJECT_NOT_FOUND);

    make_reloading_duplication_config(new_config_file);
    ASSERT_EQ(get_duplication_cluster_id("master-cluster").get_value(), 1);
    ASSERT_EQ(get_duplication_cluster_id("slave-cluster").get_value(), 2);
    ASSERT_EQ(get_duplication_cluster_id("strange-cluster").get_value(), 3);
}

TEST(duplication_common, reload_get_meta_list)
{
    make_reloading_duplication_config(config_file);
    replica_helper replica;
    std::vector<dsn::rpc_address> addr_vec;
    replica.load_meta_servers(addr_vec,"pegasus.clusters","strange-cluster");
    ASSERT_EQ(addr_vec.size(), 0);
    addr_vec.clear();

    make_reloading_duplication_config(new_config_file);
    replica.load_meta_servers(addr_vec,"pegasus.clusters","strange-cluster");
    ASSERT_EQ(addr_vec.size(), 2);

    std::string addr0 = addr_vec[0].to_string();
    std::string addr1 = addr_vec[1].to_string();
    ASSERT_EQ(addr0, "127.0.0.1:37001");
    ASSERT_EQ(addr1, "127.0.0.2:37001");

    addr_vec.clear();
    replica.load_meta_servers(addr_vec,"pegasus.clusters","unknow-cluster");
    ASSERT_EQ(addr_vec.size(), 0);
}

} // namespace replication
} // namespace dsn
