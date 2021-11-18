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
#include <dsn/utility/rand.h>

#include "base/pegasus_const.h"
#include "global_env.h"
#include "utils.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

enum class operation_type
{
    get,
    multi_get,
    set,
    multi_set
};

struct throttle_test_plan{
    operation_type dt;
    int single_value_sz;
    int multi_count;
};

struct throttle_test_result{
    int duration_s;
    int query_times;
    int reject_times;
};

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

        auto err = ddl_client->create_app(app_name.c_str(), "pegasus", 8, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);
    }

    virtual void TearDown() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox -w");
        chdir(global_env::instance()._working_dir.c_str());
    }

    void start_test(int64_t time_duration, throttle_test_plan kt)
    {
        auto a = generate_str_vector_by_random(1,1,1);
//        int64_t start = dsn_now_s();
//        int err = PERR_OK;
//        ASSERT_NE(pg_client, nullptr);
//
//        while (dsn_now_s() - start < time_duration) {
//            if (kt == detection_type::write_data) {
//                err = pg_client->set(h_key, s_key, value);
//                ASSERT_EQ(err, PERR_OK);
//            } else if  {
//                err = pg_client->get(h_key, s_key, value);
//                ASSERT_TRUE((err == PERR_OK) || err == (PERR_NOT_FOUND));
//            }
//        }
        return ;
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
    throttle_test_plan kt = {
        operation_type::get,
        1,
        1
    };
    start_test(100,kt);
}
