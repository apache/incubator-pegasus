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

#include <libgen.h>

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>

#include "base/pegasus_const.h"
#include "test/function_test/utils/utils.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;
using std::string;

enum detection_type
{
    read_data,
    write_data
};
enum key_type
{
    random_dataset,
    hotspot_dataset
};

class detect_hotspot_test : public testing::Test
{
public:
    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        string cmd = "curl 'localhost:34101/updateConfig?enable_detect_hotkey=true'";
        std::stringstream ss;
        ASSERT_EQ(0, dsn::utils::pipe_execute(cmd.c_str(), ss)) << cmd << " output: " << ss.str()
                                                                << std::endl;

        std::vector<dsn::rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster"));
        ASSERT_FALSE(meta_list.empty());

        ddl_client_ = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(ddl_client_ != nullptr);

        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(app_name.c_str(), "pegasus", 8, 3, {}, false));

        pg_client =
            pegasus::pegasus_client_factory::get_client("single_master_cluster", app_name.c_str());
        ASSERT_TRUE(pg_client != nullptr);

        int32_t partition_count = 0;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->list_app(app_name, app_id, partition_count, partitions));
    }

    void generate_dataset(int64_t time_duration, detection_type dt, key_type kt)
    {
        int64_t start = dsn_now_s();
        for (int i = 0; dsn_now_s() - start < time_duration; ++i %= 1000) {
            std::string index = std::to_string(i);
            std::string h_key = generate_hotkey(kt, 50);
            std::string s_key = "sortkey_" + index;
            std::string value = "value_" + index;
            if (dt == detection_type::write_data) {
                ASSERT_EQ(PERR_OK, pg_client->set(h_key, s_key, value));
            } else {
                int err = pg_client->get(h_key, s_key, value);
                ASSERT_TRUE(err == PERR_OK || err == PERR_NOT_FOUND);
            }
        }
    }

    void get_result(detection_type dt, key_type expect_hotspot)
    {
        dsn::replication::detect_hotkey_request req;
        if (dt == detection_type::write_data) {
            req.type = dsn::replication::hotkey_type::type::WRITE;
        } else {
            req.type = dsn::replication::hotkey_type::type::READ;
        }
        req.action = dsn::replication::detect_action::QUERY;

        bool find_hotkey = false;
        int partition_index;
        dsn::replication::detect_hotkey_response resp;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions[partition_index].primary, req, resp));
            if (!resp.hotkey_result.empty()) {
                find_hotkey = true;
                break;
            }
        }
        if (expect_hotspot == key_type::hotspot_dataset) {
            ASSERT_TRUE(find_hotkey);
            ASSERT_EQ(dsn::ERR_OK, resp.err);
            ASSERT_EQ("ThisisahotkeyThisisahotkey", resp.hotkey_result);
        } else {
            ASSERT_FALSE(find_hotkey);
        }

        // Wait for collector sending the next start detecting command
        sleep(15);

        req.action = dsn::replication::detect_action::STOP;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions[partition_index].primary, req, resp));
            ASSERT_EQ(dsn::ERR_OK, resp.err);
        }

        req.action = dsn::replication::detect_action::QUERY;
        for (partition_index = 0; partition_index < partitions.size(); partition_index++) {
            req.pid = dsn::gpid(app_id, partition_index);
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions[partition_index].primary, req, resp));
            ASSERT_EQ("Can't get hotkey now, now state: hotkey_collector_state::STOPPED",
                      resp.err_hint);
        }
    }

    void write_hotspot_data()
    {
        ASSERT_NO_FATAL_FAILURE(
            generate_dataset(warmup_second, detection_type::write_data, key_type::random_dataset));
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_detection_second, detection_type::write_data, key_type::hotspot_dataset));
        ASSERT_NO_FATAL_FAILURE(get_result(detection_type::write_data, key_type::hotspot_dataset));
    }

    void write_random_data()
    {
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_detection_second, detection_type::write_data, key_type::random_dataset));
        ASSERT_NO_FATAL_FAILURE(get_result(detection_type::write_data, key_type::random_dataset));
    }

    void capture_until_maxtime()
    {
        int target_partition = 2;
        dsn::replication::detect_hotkey_request req;
        req.type = dsn::replication::hotkey_type::type::WRITE;
        req.action = dsn::replication::detect_action::START;
        req.pid = dsn::gpid(app_id, target_partition);

        dsn::replication::detect_hotkey_response resp;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions[target_partition].primary, req, resp));
        ASSERT_EQ(dsn::ERR_OK, resp.err);

        req.action = dsn::replication::detect_action::QUERY;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions[target_partition].primary, req, resp));
        ASSERT_EQ("Can't get hotkey now, now state: hotkey_collector_state::COARSE_DETECTING",
                  resp.err_hint);

        // max_detection_second > max_seconds_to_detect_hotkey
        int max_seconds_to_detect_hotkey = 160;
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_seconds_to_detect_hotkey, detection_type::write_data, key_type::random_dataset));

        req.action = dsn::replication::detect_action::QUERY;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions[target_partition].primary, req, resp));
        ASSERT_EQ("Can't get hotkey now, now state: hotkey_collector_state::STOPPED",
                  resp.err_hint);
    }

    void read_hotspot_data()
    {
        ASSERT_NO_FATAL_FAILURE(
            generate_dataset(warmup_second, detection_type::read_data, key_type::hotspot_dataset));
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_detection_second, detection_type::read_data, key_type::hotspot_dataset));
        ASSERT_NO_FATAL_FAILURE(get_result(detection_type::read_data, key_type::hotspot_dataset));
    }

    void read_random_data()
    {
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_detection_second, detection_type::read_data, key_type::random_dataset));
        ASSERT_NO_FATAL_FAILURE(get_result(detection_type::read_data, key_type::random_dataset));
    }

    const std::string app_name = "hotspot_test";
    const int64_t max_detection_second = 100;
    const int64_t warmup_second = 30;
    int32_t app_id;
    std::vector<dsn::partition_configuration> partitions;
    std::shared_ptr<replication_ddl_client> ddl_client_;
    pegasus::pegasus_client *pg_client;
};

TEST_F(detect_hotspot_test, write_hotspot_data)
{
    std::cout << "start testing write hotspot data..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(write_hotspot_data());
    std::cout << "write hotspot data passed....." << std::endl;
    std::cout << "start testing write random data..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(write_random_data());
    std::cout << "write random data passed....." << std::endl;
    std::cout << "start testing max detection time..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(capture_until_maxtime());
    std::cout << "max detection time passed....." << std::endl;
    std::cout << "start testing read hotspot data..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(read_hotspot_data());
    std::cout << "read hotspot data passed....." << std::endl;
    std::cout << "start testing read random data..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(read_random_data());
    std::cout << "read random data passed....." << std::endl;
}
