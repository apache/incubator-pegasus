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

#include <gtest/gtest.h>
#include <pegasus/client.h>
#include <boost/lexical_cast.hpp>

#include <dsn/dist/replication/replication_ddl_client.h>

#include "base/pegasus_const.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

class partition_split_test : public testing::Test
{
public:
    void prepare(const std::string &table_name)
    {
        std::vector<rpc_address> meta_list;
        replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        create_table(table_name);

        pg_client = pegasus_client_factory::get_client("mycluster", table_name.c_str());
        write_data_before_split();

        start_partition_split(table_name);
    }

    void cleanup(const std::string &table_name)
    {
        count_during_split = 0;
        expected.clear();
        drop_table(table_name);
        delete pg_client;
    }

    void create_table(const std::string &table_name)
    {
        error_code error =
            ddl_client->create_app(table_name, "pegasus", partition_count, 3, {}, false);
        ASSERT_EQ(ERR_OK, error);
    }

    void drop_table(const std::string &table_name)
    {
        error_code error = ddl_client->drop_app(table_name, 1);
        ASSERT_EQ(ERR_OK, error);
    }

    void start_partition_split(const std::string &table_name)
    {
        auto err_resp = ddl_client->start_partition_split(table_name, partition_count * 2);
        ASSERT_EQ(ERR_OK, err_resp.get_value().err);
        std::cout << "Table(" << table_name << ") start partition split succeed" << std::endl;
    }

    bool is_split_finished(const std::string &table_name)
    {
        auto err_resp = ddl_client->query_partition_split(table_name);
        return err_resp.get_value().err == ERR_INVALID_STATE;
    }

    bool is_single_partition(int32_t target_pidx) { return target_pidx > 0; }

    bool check_partition_split_status(const std::string &table_name,
                                      int32_t target_pidx,
                                      split_status::type target_status)
    {
        auto err_resp = ddl_client->query_partition_split(table_name);
        auto status_map = err_resp.get_value().status;
        if (is_single_partition(target_pidx)) {
            return (status_map.find(target_pidx) != status_map.end() &&
                    status_map[target_pidx] == target_status);
        }

        // all partitions
        int32_t finish_count = 0;
        for (auto i = 0; i < partition_count; ++i) {
            if (status_map.find(i) != status_map.end() && status_map[i] == target_status) {
                finish_count++;
            }
        }
        return finish_count == partition_count;
    }

    error_code control_partition_split(const std::string &table_name,
                                       split_control_type::type type,
                                       int32_t parent_pidx,
                                       int32_t old_partition_count = 0)
    {
        auto err_resp =
            ddl_client->control_partition_split(table_name, type, parent_pidx, old_partition_count);
        return err_resp.get_value().err;
    }

    void write_data_before_split()
    {
        std::cout << "Write data before partition split......" << std::endl;
        for (int32_t i = 0; i < dataset_count; ++i) {
            std::string hash_key = dataset_hashkey_prefix + std::to_string(i);
            std::string sort_key = dataset_sortkey_prefix + std::to_string(i);
            auto ret = pg_client->set(hash_key, sort_key, data_value);
            ASSERT_EQ(ret, PERR_OK);
            expected[hash_key][sort_key] = data_value;
        }
    }

    bool is_valid_ret(int ret)
    {
        return (ret == PERR_OK || ret == PERR_TIMEOUT || ret == PERR_APP_SPLITTING);
    }

    void write_data_during_split()
    {
        std::string hash = splitting_hashkey_prefix + std::to_string(count_during_split);
        std::string sort = splitting_sortkey_prefix + std::to_string(count_during_split);
        auto ret = pg_client->set(hash, sort, data_value);
        ASSERT_TRUE(is_valid_ret(ret));
        if (ret == PERR_OK) {
            expected[hash][sort] = data_value;
            count_during_split++;
        }
    }

    void read_data_during_split()
    {
        auto index = rand() % dataset_count;
        std::string hash = dataset_hashkey_prefix + std::to_string(index);
        std::string sort = dataset_sortkey_prefix + std::to_string(index);
        std::string expected_value;
        auto ret = pg_client->get(hash, sort, expected_value);
        ASSERT_TRUE(is_valid_ret(ret));
        if (ret == PERR_OK) {
            ASSERT_EQ(expected_value, data_value);
        }
    }

    void verify_data_after_split()
    {
        std::cout << "Verify data(count=" << dataset_count + count_during_split
                  << ") after partition split......" << std::endl;
        verify_data(dataset_hashkey_prefix, dataset_sortkey_prefix, dataset_count);
        verify_data(splitting_hashkey_prefix, splitting_sortkey_prefix, count_during_split);
    }

    void verify_data(const std::string &hashkey_prefix,
                     const std::string &sortkey_prefix,
                     const int32_t count)
    {
        for (auto i = 0; i < count; ++i) {
            std::string hash_key = hashkey_prefix + std::to_string(i);
            std::string sort_key = sortkey_prefix + std::to_string(i);
            std::string expected_value;
            auto ret = pg_client->get(hash_key, sort_key, expected_value);
            ASSERT_EQ(ret, PERR_OK);
            ASSERT_EQ(expected[hash_key][sort_key], expected_value);
        }
    }

    void hash_scan_during_split(int32_t count)
    {
        if (count % 3 != 0) {
            return;
        }

        pegasus_client::pegasus_scanner *scanner = nullptr;
        pegasus_client::scan_options options;
        auto ret = pg_client->get_scanner(
            dataset_hashkey_prefix + std::to_string(count), "", "", options, scanner);
        ASSERT_TRUE(is_valid_ret(ret));
        if (ret == PERR_OK) {
            std::string hash_key;
            std::string sort_key;
            std::string expected_value;
            while (scanner->next(hash_key, sort_key, expected_value) == 0) {
                ASSERT_EQ(expected[hash_key][sort_key], expected_value);
            }
        }
        delete scanner;
    }

    void full_scan_after_split()
    {
        std::cout << "Start full scan after partition split......" << std::endl;
        std::vector<pegasus_client::pegasus_scanner *> scanners;
        pegasus_client::scan_options options;
        auto ret = pg_client->get_unordered_scanners(10000, options, scanners);
        ASSERT_EQ(ret, PERR_OK);
        int32_t count;
        for (auto i = 0; i < scanners.size(); i++) {
            std::string hash_key;
            std::string sort_key;
            std::string expected_value;
            pegasus_client::internal_info info;
            pegasus_client::pegasus_scanner *scanner = scanners[i];
            while (scanner->next(hash_key, sort_key, expected_value, &info) == 0) {
                ASSERT_EQ(expected[hash_key][sort_key], expected_value);
                count++;
            }
        }
        ASSERT_EQ(count, dataset_count);
        for (auto scanner : scanners) {
            delete scanner;
        }
    }

public:
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus_client *pg_client;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> expected;
    const int32_t partition_count = 4;
    const int32_t dataset_count = 1000;
    const std::string dataset_hashkey_prefix = "hashkey";
    const std::string dataset_sortkey_prefix = "sortkey";
    const std::string splitting_hashkey_prefix = "keyh_";
    const std::string splitting_sortkey_prefix = "keys_";
    const std::string data_value = "vaule";
    int32_t count_during_split = 0;
};

TEST_F(partition_split_test, split_with_write)
{
    const std::string table_name = "split_table_1";
    prepare(table_name);

    // write data during partition split
    do {
        write_data_during_split();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (!is_split_finished(table_name));
    std::cout << "Partition split succeed" << std::endl;

    verify_data_after_split();
    cleanup(table_name);
}

TEST_F(partition_split_test, split_with_read)
{
    const std::string table_name = "split_table_2";
    prepare(table_name);

    // read data during partition split
    do {
        read_data_during_split();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (!is_split_finished(table_name));
    std::cout << "Partition split succeed" << std::endl;
    verify_data_after_split();
    cleanup(table_name);
}

TEST_F(partition_split_test, split_with_scan)
{
    const std::string table_name = "split_table_3";
    prepare(table_name);

    int32_t count = 0;
    do {
        hash_scan_during_split(count);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ++count;
    } while (!is_split_finished(table_name));
    std::cout << "Partition split succeed" << std::endl;
    verify_data_after_split();
    std::this_thread::sleep_for(std::chrono::seconds(30));
    full_scan_after_split();
    cleanup(table_name);
}

TEST_F(partition_split_test, pause_split)
{
    const std::string table_name = "split_table_4";
    prepare(table_name);

    bool already_pause = false, already_restart = false;
    int32_t target_partition = 2, count = 30;
    do {
        write_data_during_split();
        // pause target partition split
        if (!already_pause &&
            check_partition_split_status(table_name, -1, split_status::SPLITTING)) {
            error_code error =
                control_partition_split(table_name, split_control_type::PAUSE, target_partition);
            ASSERT_EQ(ERR_OK, error);
            std::cout << "Table(" << table_name << ") pause partition[" << target_partition
                      << "] split succeed" << std::endl;
            already_pause = true;
        }
        // restart target partition split
        if (!already_restart && count_during_split >= count &&
            check_partition_split_status(table_name, target_partition, split_status::PAUSED)) {
            error_code error =
                control_partition_split(table_name, split_control_type::RESTART, target_partition);
            ASSERT_EQ(ERR_OK, error);
            std::cout << "Table(" << table_name << ") restart split partition[" << target_partition
                      << "] succeed" << std::endl;
            already_restart = true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (!is_split_finished(table_name));
    std::cout << "Partition split succeed" << std::endl;

    verify_data_after_split();
    cleanup(table_name);
}

TEST_F(partition_split_test, cancel_split)
{
    const std::string table_name = "split_table_5";
    prepare(table_name);

    // pause partition split
    bool already_pause = false;
    error_code error = ERR_OK;
    do {
        write_data_during_split();
        // pause all partition split
        if (!already_pause &&
            check_partition_split_status(table_name, -1, split_status::SPLITTING)) {
            error = control_partition_split(table_name, split_control_type::PAUSE, -1);
            ASSERT_EQ(ERR_OK, error);
            std::cout << "Table(" << table_name << ") pause all partitions split succeed"
                      << std::endl;
            already_pause = true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (!check_partition_split_status(table_name, -1, split_status::PAUSED));

    // cancel partition split
    error = control_partition_split(table_name, split_control_type::CANCEL, -1, partition_count);
    ASSERT_EQ(ERR_OK, error);
    std::cout << "Table(" << table_name << ") cancel partitions split succeed" << std::endl;
    // write data during cancel partition split
    do {
        write_data_during_split();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (!is_split_finished(table_name));

    verify_data_after_split();
    cleanup(table_name);
}
