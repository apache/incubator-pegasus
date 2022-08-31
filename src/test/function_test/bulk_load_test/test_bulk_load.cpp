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

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/utility/filesystem.h>

#include "include/pegasus/client.h"
#include "include/pegasus/error.h"
#include <gtest/gtest.h>

#include "base/pegasus_const.h"
#include "test/function_test/utils/global_env.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;
using std::string;

///
/// Files:
/// `pegasus-bulk-load-function-test-files` folder stores sst files and metadata files used for
/// bulk load function tests
///  - `mock_bulk_load_info` sub-directory stores stores wrong bulk_load_info
///  - `bulk_load_root` sub-directory stores right data
///     - Please do not rename any files or directories under this folder
///
/// The app who is executing bulk load:
/// - app_name is `temp`, app_id is 2, partition_count is 8
///
/// Data:
/// hashkey: hashi sortkey: sorti value: newValue       i=[0, 1000]
/// hashkey: hashkeyj sortkey: sortkeyj value: newValue j=[0, 1000]
///
class bulk_load_test : public testing::Test
{
protected:
    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        pegasus_root_dir = global_env::instance()._pegasus_root;
        working_root_dir = global_env::instance()._working_dir;
        bulk_load_local_root =
            utils::filesystem::path_combine("onebox/block_service/local_service/", LOCAL_ROOT);

        // initialize the clients
        std::vector<rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster"));
        ASSERT_FALSE(meta_list.empty());

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(ddl_client != nullptr);

        auto ret = ddl_client->drop_app(APP_NAME, 0);
        ASSERT_EQ(ERR_OK, ret);

        ret = ddl_client->create_app(
            APP_NAME, "pegasus", 8, 3, {{"rocksdb.allow_ingest_behind", "true"}}, false);
        ASSERT_EQ(ERR_OK, ret);
        int32_t new_app_id;
        int32_t partition_count;
        std::vector<partition_configuration> partitions;
        ret = ddl_client->list_app(APP_NAME, _app_id, partition_count, partitions);
        ASSERT_EQ(ERR_OK, ret);
        pg_client = pegasus::pegasus_client_factory::get_client("mycluster", APP_NAME.c_str());
        ASSERT_TRUE(pg_client != nullptr);

        // copy bulk_load files
        copy_bulk_load_files();
    }

    void TearDown() override
    {
        chdir(pegasus_root_dir.c_str());
        string cmd = "rm -rf onebox/block_service";
        std::stringstream ss;
        int iret = dsn::utils::pipe_execute(cmd.c_str(), ss);
        std::cout << cmd << " output: " << ss.str() << std::endl;
        ASSERT_EQ(iret, 0);
    }

public:
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
    std::string pegasus_root_dir;
    std::string working_root_dir;
    std::string bulk_load_local_root;
    enum operation
    {
        GET,
        SET,
        DEL,
        NO_VALUE
    };

public:
    void copy_bulk_load_files()
    {
        chdir(pegasus_root_dir.c_str());
        system("mkdir onebox/block_service");
        system("mkdir onebox/block_service/local_service");
        std::string copy_file_cmd =
            "cp -r src/test/function_test/bulk_load_test/pegasus-bulk-load-function-test-files/" +
            LOCAL_ROOT + " onebox/block_service/local_service";
        system(copy_file_cmd.c_str());

        std::stringstream cmd;
        cmd << "echo '{\"app_id\":";
        cmd << _app_id;
        cmd << ",\"app_name\":\"temp\",\"partition_count\":8}' > "
               "onebox/block_service/local_service/bulk_load_root/cluster/temp/bulk_load_info";
        std::stringstream ss;
        int ret = dsn::utils::pipe_execute(cmd.str().c_str(), ss);
        std::cout << cmd.str() << " output: " << ss.str() << std::endl;
        ASSERT_EQ(ret, 0);
    }

    error_code start_bulk_load(bool ingest_behind = false)
    {
        auto err_resp =
            ddl_client->start_bulk_load(APP_NAME, CLUSTER, PROVIDER, LOCAL_ROOT, ingest_behind);
        return err_resp.get_value().err;
    }

    void remove_file(const std::string &file_path)
    {
        std::string cmd = "rm " + file_path;
        system(cmd.c_str());
    }

    void replace_bulk_load_info()
    {
        chdir(pegasus_root_dir.c_str());
        std::string cmd =
            "cp -R "
            "src/test/function_test/bulk_load_test/pegasus-bulk-load-function-test-files/"
            "mock_bulk_load_info/. " +
            bulk_load_local_root + "/" + CLUSTER + "/" + APP_NAME + "/";
        system(cmd.c_str());
    }

    void update_allow_ingest_behind(const std::string &allow_ingest_behind)
    {
        // update app envs
        std::vector<std::string> keys;
        keys.emplace_back(ROCKSDB_ALLOW_INGEST_BEHIND);
        std::vector<std::string> values;
        values.emplace_back(allow_ingest_behind);
        auto err_resp = ddl_client->set_app_envs(APP_NAME, keys, values);
        ASSERT_EQ(err_resp.get_value().err, ERR_OK);
        std::cout << "sleep 31s to wait app_envs update" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(31));
        chdir(working_root_dir.c_str());
    }

    bulk_load_status::type wait_bulk_load_finish(int64_t seconds)
    {
        int64_t sleep_time = 5;
        error_code err = ERR_OK;

        bulk_load_status::type last_status = bulk_load_status::BLS_INVALID;
        // when bulk load end, err will be ERR_INVALID_STATE
        while (seconds > 0 && err == ERR_OK) {
            sleep_time = sleep_time > seconds ? seconds : sleep_time;
            seconds -= sleep_time;
            std::cout << "sleep " << sleep_time << "s to query bulk status" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));

            auto resp = ddl_client->query_bulk_load(APP_NAME).get_value();
            err = resp.err;
            if (err == ERR_OK) {
                last_status = resp.app_status;
            }
        }
        return last_status;
    }

    void verify_bulk_load_data()
    {
        ASSERT_NO_FATAL_FAILURE(verify_data("hashkey", "sortkey"));
        ASSERT_NO_FATAL_FAILURE(verify_data(HASHKEY_PREFIX, SORTKEY_PREFIX));
    }

    void verify_data(const std::string &hashkey_prefix, const std::string &sortkey_prefix)
    {
        const std::string &expected_value = VALUE;
        for (int i = 0; i < COUNT; ++i) {
            std::string hash_key = hashkey_prefix + std::to_string(i);
            for (int j = 0; j < COUNT; ++j) {
                std::string sort_key = sortkey_prefix + std::to_string(j);
                std::string act_value;
                int ret = pg_client->get(hash_key, sort_key, act_value);
                ASSERT_EQ(PERR_OK, ret) << "Failed to get [" << hash_key << "," << sort_key
                                        << "], error is " << ret << std::endl;
                ASSERT_EQ(expected_value, act_value)
                    << "get [" << hash_key << "," << sort_key << "], value = " << act_value
                    << ", but expected_value = " << expected_value << std::endl;
            }
        }
    }

    void operate_data(bulk_load_test::operation op, const std::string &value, int count)
    {
        for (int i = 0; i < count; ++i) {
            std::string hash_key = HASHKEY_PREFIX + std::to_string(i);
            std::string sort_key = SORTKEY_PREFIX + std::to_string(i);
            switch (op) {
            case bulk_load_test::operation::GET: {
                std::string act_value;
                int ret = pg_client->get(hash_key, sort_key, act_value);
                ASSERT_EQ(ret, PERR_OK);
                ASSERT_EQ(act_value, value);
            } break;
            case bulk_load_test::operation::DEL: {
                ASSERT_EQ(pg_client->del(hash_key, sort_key), PERR_OK);
            } break;
            case bulk_load_test::operation::SET: {
                ASSERT_EQ(pg_client->set(hash_key, sort_key, value), PERR_OK);
            } break;
            case bulk_load_test::operation::NO_VALUE: {
                std::string act_value;
                ASSERT_EQ(pg_client->get(hash_key, sort_key, act_value), PERR_NOT_FOUND);
            } break;
            default:
                ASSERT_TRUE(false);
                break;
            }
        }
    }

    const std::string LOCAL_ROOT = "bulk_load_root";
    const std::string CLUSTER = "cluster";
    const std::string APP_NAME = "temp";
    const std::string PROVIDER = "local_service";

    const std::string HASHKEY_PREFIX = "hash";
    const std::string SORTKEY_PREFIX = "sort";
    const std::string VALUE = "newValue";
    const int32_t COUNT = 1000;

    int32_t _app_id = 0;
};

///
/// case1: lack of `bulk_load_info` file
/// case2: `bulk_load_info` file inconsistent with app_info
///
TEST_F(bulk_load_test, bulk_load_test_failed)
{
    // bulk load failed because `bulk_load_info` file is missing
    remove_file(bulk_load_local_root + "/" + CLUSTER + "/" + APP_NAME + "/bulk_load_info");
    ASSERT_EQ(start_bulk_load(), ERR_OBJECT_NOT_FOUND);

    // bulk load failed because `bulk_load_info` file inconsistent with current app_info
    replace_bulk_load_info();
    ASSERT_EQ(start_bulk_load(), ERR_INCONSISTENT_STATE);
}

///
/// case1: lack of `bulk_load_metadata` file
/// case2: bulk load succeed with data verfied
/// case3: bulk load data consistent:
///     - old data will be overrided by bulk load data
///     - get/set/del succeed after bulk load
///
TEST_F(bulk_load_test, bulk_load_tests)
{
    // bulk load failed because partition[0] `bulk_load_metadata` file is missing
    remove_file(bulk_load_local_root + "/" + CLUSTER + "/" + APP_NAME + "/0/bulk_load_metadata");
    ASSERT_EQ(start_bulk_load(), ERR_OK);
    // bulk load will get FAILED
    ASSERT_EQ(wait_bulk_load_finish(300), bulk_load_status::BLS_FAILED);

    // recover complete files
    copy_bulk_load_files();

    // write old data
    operate_data(operation::SET, "oldValue", 10);
    operate_data(operation::GET, "oldValue", 10);

    ASSERT_EQ(start_bulk_load(), ERR_OK);
    ASSERT_EQ(wait_bulk_load_finish(300), bulk_load_status::BLS_SUCCEED);
    std::cout << "Start to verify data..." << std::endl;
    verify_bulk_load_data();

    // value overide by bulk_loaded_data
    operate_data(operation::GET, VALUE, 10);

    // write data after bulk load succeed
    operate_data(operation::SET, "valueAfterBulkLoad", 20);
    operate_data(operation::GET, "valueAfterBulkLoad", 20);

    // del data after bulk load succeed
    operate_data(operation::DEL, "", 15);
    operate_data(operation::NO_VALUE, "", 15);
}

///
/// case1: inconsistent ingest_behind
/// case2: bulk load(ingest_behind) succeed with data verfied
/// case3: bulk load data consistent:
///     - bulk load data will be overrided by old data
///     - get/set/del succeed after bulk load
///
TEST_F(bulk_load_test, bulk_load_ingest_behind_tests)
{
    update_allow_ingest_behind("false");

    // app envs allow_ingest_behind = false, request ingest_behind = true
    ASSERT_EQ(start_bulk_load(true), ERR_INCONSISTENT_STATE);

    update_allow_ingest_behind("true");

    // write old data
    operate_data(operation::SET, "oldValue", 10);
    operate_data(operation::GET, "oldValue", 10);

    ASSERT_EQ(start_bulk_load(true), ERR_OK);
    ASSERT_EQ(wait_bulk_load_finish(300), bulk_load_status::BLS_SUCCEED);

    std::cout << "Start to verify data..." << std::endl;
    // value overide by bulk_loaded_data
    operate_data(operation::GET, "oldValue", 10);
    ASSERT_NO_FATAL_FAILURE(verify_data("hashkey", "sortkey"));

    // write data after bulk load succeed
    operate_data(operation::SET, "valueAfterBulkLoad", 20);
    operate_data(operation::GET, "valueAfterBulkLoad", 20);

    // del data after bulk load succeed
    operate_data(operation::DEL, "", 15);
    operate_data(operation::NO_VALUE, "", 15);
}
