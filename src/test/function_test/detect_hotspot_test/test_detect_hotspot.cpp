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

#include <gtest/gtest.h>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "client/replication_ddl_client.h"
#include "utils/filesystem.h"

#include "include/pegasus/client.h"
#include "include/pegasus/error.h"

#include "base/pegasus_const.h"
#include "test/function_test/utils/utils.h"
#include "test/function_test/utils/test_util.h"

using namespace ::dsn;
using namespace pegasus;

class detect_hotspot_test : public test_util
{
protected:
    const int64_t max_detection_second = 100;
    const int64_t warmup_second = 30;

protected:
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

    void SetUp() override
    {
        TRICKY_CODE_TO_AVOID_LINK_ERROR;
        test_util::SetUp();

        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
            "curl 'localhost:34101/updateConfig?enable_detect_hotkey=true'"));
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
                ASSERT_EQ(PERR_OK, client_->set(h_key, s_key, value));
            } else {
                int err = client_->get(h_key, s_key, value);
                ASSERT_TRUE(err == PERR_OK || err == PERR_NOT_FOUND) << err;
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
        dsn::replication::detect_hotkey_response resp;
        for (int partition_index = 0; partition_index < partitions_.size(); partition_index++) {
            req.pid = dsn::gpid(app_id_, partition_index);
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions_[partition_index].primary, req, resp));
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
        for (int partition_index = 0; partition_index < partitions_.size(); partition_index++) {
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions_[partition_index].primary, req, resp));
            ASSERT_EQ(dsn::ERR_OK, resp.err);
        }

        req.action = dsn::replication::detect_action::QUERY;
        for (int partition_index = 0; partition_index < partitions_.size(); partition_index++) {
            req.pid = dsn::gpid(app_id_, partition_index);
            ASSERT_EQ(dsn::ERR_OK,
                      ddl_client_->detect_hotkey(partitions_[partition_index].primary, req, resp));
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
        req.pid = dsn::gpid(app_id_, target_partition);

        dsn::replication::detect_hotkey_response resp;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions_[target_partition].primary, req, resp));
        ASSERT_EQ(dsn::ERR_OK, resp.err);

        req.action = dsn::replication::detect_action::QUERY;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions_[target_partition].primary, req, resp));
        ASSERT_EQ("Can't get hotkey now, now state: hotkey_collector_state::COARSE_DETECTING",
                  resp.err_hint);

        // max_detection_second > max_seconds_to_detect_hotkey
        int max_seconds_to_detect_hotkey = 160;
        ASSERT_NO_FATAL_FAILURE(generate_dataset(
            max_seconds_to_detect_hotkey, detection_type::write_data, key_type::random_dataset));

        req.action = dsn::replication::detect_action::QUERY;
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->detect_hotkey(partitions_[target_partition].primary, req, resp));
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
};

TEST_F(detect_hotspot_test, write_hotspot_data_test)
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
