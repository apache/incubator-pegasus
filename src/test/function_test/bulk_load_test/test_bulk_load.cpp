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

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/utility/filesystem.h>

#include "include/pegasus/client.h"
#include "include/pegasus/error.h"

#include "base/pegasus_const.h"
#include "test/function_test/utils/test_util.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;
using std::map;
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
class bulk_load_test : public test_util
{
protected:
    bulk_load_test() : test_util(map<string, string>({{"rocksdb.allow_ingest_behind", "true"}}))
    {
        TRICKY_CODE_TO_AVOID_LINK_ERROR;
        bulk_load_local_root_ =
            utils::filesystem::path_combine("onebox/block_service/local_service/", LOCAL_ROOT);
    }

    void SetUp() override
    {
        test_util::SetUp();
        ASSERT_NO_FATAL_FAILURE(copy_bulk_load_files());
    }

    void TearDown() override
    {
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(app_name_, 0));
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root("rm -rf onebox/block_service"));
    }

    void copy_bulk_load_files()
    {
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root("mkdir -p onebox/block_service"));
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("mkdir -p onebox/block_service/local_service"));
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
            "cp -r src/test/function_test/bulk_load_test/pegasus-bulk-load-function-test-files/" +
            LOCAL_ROOT + " onebox/block_service/local_service"));
        string cmd = "echo '{\"app_id\":" + std::to_string(app_id_) +
                     ",\"app_name\":\"temp\",\"partition_count\":8}' > "
                     "onebox/block_service/local_service/bulk_load_root/cluster/temp/"
                     "bulk_load_info";
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(cmd));
    }

    error_code start_bulk_load(bool ingest_behind = false)
    {
        auto err_resp =
            ddl_client_->start_bulk_load(app_name_, CLUSTER, PROVIDER, LOCAL_ROOT, ingest_behind);
        return err_resp.get_value().err;
    }

    void remove_file(const string &file_path)
    {
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root("rm " + file_path));
    }

    void replace_bulk_load_info()
    {
        string cmd = "cp -R "
                     "src/test/function_test/bulk_load_test/pegasus-bulk-load-function-test-files/"
                     "mock_bulk_load_info/. " +
                     bulk_load_local_root_ + "/" + CLUSTER + "/" + app_name_ + "/";
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(cmd));
    }

    void update_allow_ingest_behind(const string &allow_ingest_behind)
    {
        // update app envs
        std::vector<string> keys;
        keys.emplace_back(ROCKSDB_ALLOW_INGEST_BEHIND);
        std::vector<string> values;
        values.emplace_back(allow_ingest_behind);
        ASSERT_EQ(ERR_OK, ddl_client_->set_app_envs(app_name_, keys, values).get_value().err);
        std::cout << "sleep 31s to wait app_envs update" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(31));
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

            auto resp = ddl_client_->query_bulk_load(app_name_).get_value();
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

    void verify_data(const string &hashkey_prefix, const string &sortkey_prefix)
    {
        const string &expected_value = VALUE;
        for (int i = 0; i < COUNT; ++i) {
            string hash_key = hashkey_prefix + std::to_string(i);
            for (int j = 0; j < COUNT; ++j) {
                string sort_key = sortkey_prefix + std::to_string(j);
                string act_value;
                ASSERT_EQ(PERR_OK, client_->get(hash_key, sort_key, act_value)) << hash_key << ","
                                                                                << sort_key;
                ASSERT_EQ(expected_value, act_value) << hash_key << "," << sort_key;
            }
        }
    }

    enum operation
    {
        GET,
        SET,
        DEL,
        NO_VALUE
    };
    void operate_data(bulk_load_test::operation op, const string &value, int count)
    {
        for (int i = 0; i < count; ++i) {
            string hash_key = HASHKEY_PREFIX + std::to_string(i);
            string sort_key = SORTKEY_PREFIX + std::to_string(i);
            switch (op) {
            case bulk_load_test::operation::GET: {
                string act_value;
                ASSERT_EQ(PERR_OK, client_->get(hash_key, sort_key, act_value));
                ASSERT_EQ(value, act_value);
            } break;
            case bulk_load_test::operation::DEL: {
                ASSERT_EQ(PERR_OK, client_->del(hash_key, sort_key));
            } break;
            case bulk_load_test::operation::SET: {
                ASSERT_EQ(PERR_OK, client_->set(hash_key, sort_key, value));
            } break;
            case bulk_load_test::operation::NO_VALUE: {
                string act_value;
                ASSERT_EQ(PERR_NOT_FOUND, client_->get(hash_key, sort_key, act_value));
            } break;
            default:
                ASSERT_TRUE(false);
                break;
            }
        }
    }

protected:
    string bulk_load_local_root_;

    const string LOCAL_ROOT = "bulk_load_root";
    const string CLUSTER = "cluster";
    const string PROVIDER = "local_service";

    const string HASHKEY_PREFIX = "hash";
    const string SORTKEY_PREFIX = "sort";
    const string VALUE = "newValue";
    const int32_t COUNT = 1000;
};

///
/// case1: lack of `bulk_load_info` file
/// case2: `bulk_load_info` file inconsistent with app_info
///
TEST_F(bulk_load_test, bulk_load_test_failed)
{
    // bulk load failed because `bulk_load_info` file is missing
    ASSERT_NO_FATAL_FAILURE(
        remove_file(bulk_load_local_root_ + "/" + CLUSTER + "/" + app_name_ + "/bulk_load_info"));
    ASSERT_EQ(ERR_OBJECT_NOT_FOUND, start_bulk_load());

    // bulk load failed because `bulk_load_info` file inconsistent with current app_info
    ASSERT_NO_FATAL_FAILURE(replace_bulk_load_info());
    ASSERT_EQ(ERR_INCONSISTENT_STATE, start_bulk_load());
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
    ASSERT_NO_FATAL_FAILURE(remove_file(bulk_load_local_root_ + "/" + CLUSTER + "/" + app_name_ +
                                        "/0/bulk_load_metadata"));
    ASSERT_EQ(ERR_OK, start_bulk_load());
    // bulk load will get FAILED
    ASSERT_EQ(bulk_load_status::BLS_FAILED, wait_bulk_load_finish(300));

    // recover complete files
    ASSERT_NO_FATAL_FAILURE(copy_bulk_load_files());

    // write old data
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::SET, "oldValue", 10));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, "oldValue", 10));

    ASSERT_EQ(ERR_OK, start_bulk_load());
    ASSERT_EQ(bulk_load_status::BLS_SUCCEED, wait_bulk_load_finish(300));
    std::cout << "Start to verify data..." << std::endl;
    ASSERT_NO_FATAL_FAILURE(verify_bulk_load_data());

    // value overide by bulk_loaded_data
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, VALUE, 10));

    // write data after bulk load succeed
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::SET, "valueAfterBulkLoad", 20));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, "valueAfterBulkLoad", 20));

    // del data after bulk load succeed
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::DEL, "", 15));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::NO_VALUE, "", 15));
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
    ASSERT_NO_FATAL_FAILURE(update_allow_ingest_behind("false"));

    // app envs allow_ingest_behind = false, request ingest_behind = true
    ASSERT_EQ(ERR_INCONSISTENT_STATE, start_bulk_load(true));

    ASSERT_NO_FATAL_FAILURE(update_allow_ingest_behind("true"));

    // write old data
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::SET, "oldValue", 10));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, "oldValue", 10));

    ASSERT_EQ(ERR_OK, start_bulk_load(true));
    ASSERT_EQ(bulk_load_status::BLS_SUCCEED, wait_bulk_load_finish(300));

    std::cout << "Start to verify data..." << std::endl;
    // value overide by bulk_loaded_data
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, "oldValue", 10));
    ASSERT_NO_FATAL_FAILURE(verify_data("hashkey", "sortkey"));

    // write data after bulk load succeed
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::SET, "valueAfterBulkLoad", 20));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::GET, "valueAfterBulkLoad", 20));

    // del data after bulk load succeed
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::DEL, "", 15));
    ASSERT_NO_FATAL_FAILURE(operate_data(operation::NO_VALUE, "", 15));
}
