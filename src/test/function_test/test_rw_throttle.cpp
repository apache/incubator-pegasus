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

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include <dsn/utility/TokenBucket.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>

#include "base/pegasus_const.h"
#include "global_env.h"
#include "utils.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

enum class throttle_type
{
    read_by_qps,
    read_by_size,
    write_by_qps,
    write_by_size
};

enum class operation_type
{
    get,
    multi_get,
    set,
    multi_set
};

struct throttle_test_plan
{
    std::string test_plan_case;
    operation_type ot;
    int single_value_sz;
    int multi_count;
    int limit_qps;
};

struct throttle_test_result
{
    uint64_t duration_s;
    uint64_t query_times_per_s;
    uint64_t reject_times_per_s;
    uint64_t query_size_per_s;
    uint64_t reject_size_per_s;
};

const int test_hashkey_len = 50;
const int test_sortkey_len = 50;

class test_rw_throttle : public testing::Test
{
public:
    virtual void SetUp() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("pwd");
        system("./run.sh clear_onebox");
        system("cp src/server/config.min.ini config-server-test-hotspot.ini");
        system("sed -i \"/^\\s*enable_detect_hotkey/c enable_detect_hotkey = "
               "true\" config-server-test-hotspot.ini");
        system("./run.sh start_onebox -c -w --config_path config-server-test-hotspot.ini");
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::vector<dsn::rpc_address> meta_list;
        replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster");

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        pg_client =
            pegasus::pegasus_client_factory::get_client("single_master_cluster", app_name.c_str());

        auto err = ddl_client->create_app(app_name.c_str(), "pegasus", 4, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);
    }

    virtual void TearDown() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox -w");
        chdir(global_env::instance()._working_dir.c_str());
    }

    void set_throttle(throttle_type type, uint64_t value)
    {
        if (type == throttle_type::read_by_qps) {
            auto resp = ddl_client->set_app_envs(
                app_name, {"replica.read_throttling"}, {fmt::format("{}*reject*200", value)});
            dassert_f(resp.get_error().code() == ERR_OK,
                      "Set env failed: {}",
                      resp.get_value().hint_message);
        } else if (type == throttle_type::read_by_size) {
            auto resp = ddl_client->set_app_envs(
                app_name, {"replica.read_throttling_by_size"}, {fmt::format("{}", value)});
            dassert_f(resp.get_error().code() == ERR_OK,
                      "Set env failed: {}",
                      resp.get_value().hint_message);
        } else if (type == throttle_type::write_by_qps) {
            auto resp = ddl_client->set_app_envs(
                app_name, {"replica.write_throttling"}, {fmt::format("{}*reject*200", value)});
            dassert_f(resp.get_error().code() == ERR_OK,
                      "Set env failed: {}",
                      resp.get_value().hint_message);
        } else if (type == throttle_type::write_by_size) {
            auto resp = ddl_client->set_app_envs(app_name,
                                                 {"replica.write_throttling_by_size"},
                                                 {fmt::format("{}*reject*200", value)});
            dassert_f(resp.get_error().code() == ERR_OK,
                      "Set env failed: {}",
                      resp.get_value().hint_message);
        }
    }

    throttle_test_result start_test(throttle_test_plan test_plan, uint64_t time_duration_s = 30)
    {
        std::cout << fmt::format("start test, on {}", test_plan.test_plan_case) << std::endl;

        int64_t start = dsn_now_ns();
        int err = PERR_OK;
        dassert(pg_client, "pg_client is nullptr");

        auto token_bucket = std::make_unique<folly::BasicTokenBucket<std::chrono::steady_clock>>(
            test_plan.limit_qps, test_plan.limit_qps);
        int query_times = 0;
        int reject_times = 0;
        int query_size = 0;
        int reject_size = 0;

        while (dsn_now_ns() - start < time_duration_s * 1000000000) {
            token_bucket->consumeWithBorrowAndWait(1);

            auto h_key = generate_hash_key(test_hashkey_len);
            auto s_key = generate_hash_key(test_sortkey_len);
            auto value = generate_hash_key(test_plan.single_value_sz);
            auto sortkey_value_pairs = generate_sortkey_value_map(
                generate_str_vector_by_random(test_sortkey_len, test_plan.multi_count),
                generate_str_vector_by_random(test_plan.single_value_sz, test_plan.multi_count));

            if (test_plan.ot == operation_type::set) {
                err = pg_client->set(h_key, s_key, value);
            } else if (test_plan.ot == operation_type::multi_set) {
                err = pg_client->multi_set(h_key, sortkey_value_pairs);
            } else if (test_plan.ot == operation_type::get) {
                err = pg_client->set(h_key, s_key, value);
                err = pg_client->get(h_key, s_key, value);
            } else if (test_plan.ot == operation_type::multi_get) {
                err = pg_client->multi_set(h_key, sortkey_value_pairs);
                std::set<std::string> sortkeys;
                err = pg_client->multi_get(h_key, sortkeys, sortkey_value_pairs);
            }

            query_times++;
            query_size += test_plan.single_value_sz * test_plan.multi_count;

            if (err == PERR_APP_BUSY) {
                reject_times++;
                reject_size += test_plan.single_value_sz * test_plan.multi_count;
            } else {
                dassert_f(err == PERR_OK, "get/set data failed, error code:{}", err);
            }
        }

        throttle_test_result result = {time_duration_s,
                                       query_times / time_duration_s,
                                       reject_times / time_duration_s,
                                       query_size / time_duration_s,
                                       reject_size / time_duration_s};

        std::cout << fmt::format(
                         "{} test result: \n time_duration_s:{} \n query_times_per_s:{} \n "
                         "reject_times_per_s:{} \n query_size_per_s:{} \n reject_size_per_s:{} \n",
                         test_plan.test_plan_case,
                         result.duration_s,
                         result.query_times_per_s,
                         result.reject_times_per_s,
                         result.query_size_per_s,
                         result.reject_size_per_s)
                  << std::endl;

        return result;
    }

    const std::string app_name = "throttle_test";
    const int64_t max_detection_second = 100;
    const int64_t warmup_second = 30;
    int32_t app_id;
    int32_t partition_count;
    dsn::replication::detect_hotkey_response resp;
    dsn::replication::detect_hotkey_request req;
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
};

TEST_F(test_rw_throttle, test_rw_throttle)
{
    throttle_test_plan plan = {"set qps test", operation_type::set, 1024, 1, 500};
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 15s for setting env" << std::endl;
    sleep(15);
    start_test(plan);
}
