// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "metadata_types.h"
#include "partition_split_types.h"
#include "pegasus/error.h"
#include "test/function_test/utils/test_util.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/test_macros.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

class partition_split_test : public test_util
{
public:
    partition_split_test() : test_util()
    {
        TRICKY_CODE_TO_AVOID_LINK_ERROR;
        static int32_t test_case = 0;
        table_name_ = table_name_prefix + std::to_string(test_case++);
    }

    void SetUp() override
    {
        SET_UP_BASE(test_util);
        NO_FATALS(write_data_before_split());
        ASSERT_EQ(
            ERR_OK,
            ddl_client_->start_partition_split(table_name_, partition_count_ * 2).get_value().err);
    }

    void TearDown() override { ASSERT_EQ(ERR_OK, ddl_client_->drop_app(table_name_, 0)); }

    bool is_split_finished()
    {
        auto err_resp = ddl_client_->query_partition_split(table_name_);
        auto status_map = err_resp.get_value().status;
        return err_resp.get_value().err == ERR_INVALID_STATE;
    }

    bool check_partition_split_status(int32_t target_pidx, split_status::type target_status)
    {
        auto err_resp = ddl_client_->query_partition_split(table_name_);
        auto status_map = err_resp.get_value().status;
        // is_single_partition
        if (target_pidx > 0) {
            return (status_map.find(target_pidx) != status_map.end() &&
                    status_map[target_pidx] == target_status);
        }

        // all partitions
        int32_t finish_count = 0;
        for (auto i = 0; i < partition_count_; ++i) {
            if (status_map.find(i) != status_map.end() && status_map[i] == target_status) {
                finish_count++;
            }
        }
        return finish_count == partition_count_;
    }

    error_code control_partition_split(split_control_type::type type,
                                       int32_t parent_pidx,
                                       int32_t old_partition_count = 0)
    {
        auto err_resp = ddl_client_->control_partition_split(
            table_name_, type, parent_pidx, old_partition_count);
        return err_resp.get_value().err;
    }

    void write_data_before_split()
    {
        for (int32_t i = 0; i < dataset_count; ++i) {
            std::string hash_key = dataset_hashkey_prefix + std::to_string(i);
            std::string sort_key = dataset_sortkey_prefix + std::to_string(i);
            ASSERT_EQ(PERR_OK, client_->set(hash_key, sort_key, data_value));
            expected_kvs_[hash_key][sort_key] = data_value;
        }
    }

    bool is_valid(int ret)
    {
        return (ret == PERR_OK || ret == PERR_TIMEOUT || ret == PERR_APP_SPLITTING);
    }

    void write_data_during_split()
    {
        std::string hash = splitting_hashkey_prefix + std::to_string(count_during_split_);
        std::string sort = splitting_sortkey_prefix + std::to_string(count_during_split_);
        auto ret = client_->set(hash, sort, data_value);
        ASSERT_TRUE(is_valid(ret)) << ret << ", " << hash << " : " << sort;
        if (ret == PERR_OK) {
            expected_kvs_[hash][sort] = data_value;
            count_during_split_++;
        }
    }

    void read_data_during_split()
    {
        auto index = rand() % dataset_count;
        std::string hash = dataset_hashkey_prefix + std::to_string(index);
        std::string sort = dataset_sortkey_prefix + std::to_string(index);
        std::string actual_value;
        auto ret = client_->get(hash, sort, actual_value);
        ASSERT_TRUE(is_valid(ret)) << ret << ", " << hash << " : " << sort;
        if (ret == PERR_OK) {
            ASSERT_EQ(data_value, actual_value);
        }
    }

    void verify_data_after_split()
    {
        std::cout << "Verify data(count=" << dataset_count + count_during_split_
                  << ") after partition split......" << std::endl;
        NO_FATALS(verify_data(dataset_hashkey_prefix, dataset_sortkey_prefix, dataset_count));
        NO_FATALS(
            verify_data(splitting_hashkey_prefix, splitting_sortkey_prefix, count_during_split_));
    }

    void verify_data(const std::string &hashkey_prefix,
                     const std::string &sortkey_prefix,
                     const int32_t count)
    {
        for (auto i = 0; i < count; ++i) {
            std::string hash_key = hashkey_prefix + std::to_string(i);
            std::string sort_key = sortkey_prefix + std::to_string(i);
            std::string value;
            ASSERT_EQ(PERR_OK, client_->get(hash_key, sort_key, value));
            ASSERT_EQ(expected_kvs_[hash_key][sort_key], value);
        }
    }

    void hash_scan_during_split(int32_t count)
    {
        if (count % 3 != 0) {
            return;
        }

        pegasus_client::pegasus_scanner *scanner = nullptr;
        pegasus_client::scan_options options;
        auto ret = client_->get_scanner(
            dataset_hashkey_prefix + std::to_string(count), "", "", options, scanner);
        ASSERT_TRUE(is_valid(ret)) << ret;
        if (ret == PERR_OK) {
            std::string hash_key;
            std::string sort_key;
            std::string actual_value;
            while (scanner->next(hash_key, sort_key, actual_value) == 0) {
                ASSERT_EQ(expected_kvs_[hash_key][sort_key], actual_value);
            }
        }
        delete scanner;
    }

    void full_scan_after_split()
    {
        std::cout << "Start full scan after partition split......" << std::endl;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        pegasus_client::scan_options options;
        options.timeout_ms = 30000;
        ASSERT_EQ(PERR_OK, client_->get_unordered_scanners(10000, options, scanners));
        int32_t count = 0;
        for (auto i = 0; i < scanners.size(); i++) {
            std::string hash_key;
            std::string sort_key;
            std::string actual_value;
            pegasus_client::internal_info info;
            pegasus_client::pegasus_scanner *scanner = scanners[i];
            while (scanner->next(hash_key, sort_key, actual_value, &info) == 0) {
                ASSERT_EQ(expected_kvs_[hash_key][sort_key], actual_value);
                count++;
            }
        }
        ASSERT_EQ(dataset_count, count);
        for (auto scanner : scanners) {
            delete scanner;
        }
    }

public:
    const std::string table_name_prefix = "split_table_test_";
    const std::string dataset_hashkey_prefix = "hashkey";
    const std::string dataset_sortkey_prefix = "sortkey";
    const std::string splitting_hashkey_prefix = "keyh_";
    const std::string splitting_sortkey_prefix = "keys_";
    const std::string data_value = "vaule";

    const int32_t dataset_count = 1000;

    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> expected_kvs_;
    int32_t count_during_split_ = 0;
};

TEST_F(partition_split_test, split_with_write)
{
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            // write data during partition split
            // TODO(yingchun): better to write data background continuously
            NO_FATALS(write_data_during_split());
            ASSERT_TRUE(is_split_finished());
        },
        300);

    std::cout << "Partition split succeed" << std::endl;
    NO_FATALS(verify_data_after_split());
}

TEST_F(partition_split_test, split_with_read)
{
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            // read data during partition split
            // TODO(yingchun): better to read data background continuously
            NO_FATALS(read_data_during_split());
            ASSERT_TRUE(is_split_finished());
        },
        300);

    std::cout << "Partition split succeed" << std::endl;
    NO_FATALS(verify_data_after_split());
}

TEST_F(partition_split_test, split_with_scan)
{
    int32_t count = 0;
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            // TODO(yingchun): better to scan data background continuously
            NO_FATALS(hash_scan_during_split(count++));
            ASSERT_TRUE(is_split_finished());
        },
        300);

    fmt::print(stdout, "Partition split succeed\n");
    NO_FATALS(verify_data_after_split());
    std::this_thread::sleep_for(std::chrono::seconds(30));
    NO_FATALS(full_scan_after_split());
}

TEST_F(partition_split_test, pause_split)
{
    bool already_pause = false, already_restart = false;
    const int32_t target_partition = 2, count = 30;
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            NO_FATALS(write_data_during_split());
            // pause target partition split
            if (!already_pause && check_partition_split_status(-1, split_status::SPLITTING)) {
                ASSERT_EQ(ERR_OK,
                          control_partition_split(split_control_type::PAUSE, target_partition));
                fmt::print(stdout,
                           "Table({}) pause partition[{}] split succeed\n",
                           table_name_,
                           target_partition);
                already_pause = true;
            }
            // restart target partition split
            if (!already_restart && count_during_split_ >= count &&
                check_partition_split_status(target_partition, split_status::PAUSED)) {
                ASSERT_EQ(ERR_OK,
                          control_partition_split(split_control_type::RESTART, target_partition));
                fmt::print(stdout,
                           "Table({}) restart split partition[{}] succeed\n",
                           table_name_,
                           target_partition);
                already_restart = true;
            }
            ASSERT_TRUE(is_split_finished());
        },
        300);

    fmt::print(stdout, "Partition split succeed\n");
    NO_FATALS(verify_data_after_split());
}

TEST_F(partition_split_test, cancel_split)
{
    // pause partition split
    bool already_pause = false;
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            NO_FATALS(write_data_during_split());
            // pause all partition split
            if (!already_pause && check_partition_split_status(-1, split_status::SPLITTING)) {
                ASSERT_EQ(ERR_OK, control_partition_split(split_control_type::PAUSE, -1));
                fmt::print(stdout, "Table({}) pause all partitions split succeed\n", table_name_);
                already_pause = true;
            }
            ASSERT_TRUE(check_partition_split_status(-1, split_status::PAUSED));
        },
        300);

    // cancel partition split
    ASSERT_EQ(ERR_OK, control_partition_split(split_control_type::CANCEL, -1, partition_count_));
    fmt::print(stdout, "Table({}) cancel partitions split succeed\n", table_name_);
    // write data during cancel partition split
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            NO_FATALS(write_data_during_split());
            ASSERT_TRUE(is_split_finished());
        },
        300);

    fmt::print(stdout, "Partition split succeed\n");
    NO_FATALS(verify_data_after_split());
}
