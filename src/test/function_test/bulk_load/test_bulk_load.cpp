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
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/pegasus_const.h"
#include "block_service/local/local_service.h"
#include "bulk_load_types.h"
#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/json_helper.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "include/pegasus/error.h"
#include "meta/meta_bulk_load_service.h"
#include "test/function_test/utils/test_util.h"
#include "test_util/test_util.h"
#include "utils/blob.h"
#include "utils/enum_helper.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/test_macros.h"

DSN_DECLARE_bool(encrypt_data_at_rest);

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
/// The app to test bulk load functionality:
/// - partition count should be 8
///
/// Data:
/// hashkey: hash${i} sortkey: sort${i} value: newValue       i=[0, 1000]
/// hashkey: hashkey${j} sortkey: sortkey${j} value: newValue j=[0, 1000]
///
class bulk_load_test : public test_util
{
protected:
    bulk_load_test() : test_util(map<string, string>({{"rocksdb.allow_ingest_behind", "true"}}))
    {
        TRICKY_CODE_TO_AVOID_LINK_ERROR;
        bulk_load_local_app_root_ =
            fmt::format("{}/{}/{}", kLocalBulkLoadRoot, kCluster, table_name_);
    }

    void SetUp() override
    {
        test_util::SetUp();
        NO_FATALS(copy_bulk_load_files());
    }

    void TearDown() override
    {
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(table_name_, 0));
        NO_FATALS(run_cmd_from_project_root("rm -rf " + kLocalBulkLoadRoot));
    }

    // Generate the 'bulk_load_info' file according to 'bli' to path 'bulk_load_info_path'.
    void generate_bulk_load_info(const bulk_load_info &bli, const std::string &bulk_load_info_path)
    {
        auto value = dsn::json::json_forwarder<bulk_load_info>::encode(bli);
        auto s =
            rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                       rocksdb::Slice(value.data(), value.length()),
                                       bulk_load_info_path,
                                       /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // Generate the '.bulk_load_info.meta' file according to the 'bulk_load_info' file
    // in path 'bulk_load_info_path'.
    void generate_bulk_load_info_meta(const std::string &bulk_load_info_path)
    {
        dist::block_service::file_metadata fm;
        ASSERT_TRUE(utils::filesystem::file_size(
            bulk_load_info_path, dsn::utils::FileDataType::kSensitive, fm.size));
        ASSERT_EQ(ERR_OK, utils::filesystem::md5sum(bulk_load_info_path, fm.md5));
        std::string value = nlohmann::json(fm).dump();
        auto bulk_load_info_meta_path =
            fmt::format("{}/{}/{}/.bulk_load_info.meta", kLocalBulkLoadRoot, kCluster, table_name_);
        auto s =
            rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                       rocksdb::Slice(value),
                                       bulk_load_info_meta_path,
                                       /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void copy_bulk_load_files()
    {
        // TODO(yingchun): remove the 'mock_bulk_load_info' file, because we can generate it.
        // Prepare bulk load files.
        // The source data has 8 partitions.
        ASSERT_EQ(8, partition_count_);
        NO_FATALS(run_cmd_from_project_root("mkdir -p " + kLocalBulkLoadRoot));
        NO_FATALS(run_cmd_from_project_root(
            fmt::format("cp -r {}/{} {}", kSourceFilesRoot, kBulkLoad, kLocalServiceRoot)));

        if (FLAGS_encrypt_data_at_rest) {
            std::vector<std::string> src_files;
            ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(kLocalServiceRoot, src_files, true));
            for (const auto &src_file : src_files) {
                auto s = dsn::utils::encrypt_file(src_file);
                ASSERT_TRUE(s.ok()) << s.ToString();
            }
        }

        // Generate 'bulk_load_info'.
        auto bulk_load_info_path =
            fmt::format("{}/{}/{}/bulk_load_info", kLocalBulkLoadRoot, kCluster, table_name_);
        NO_FATALS(generate_bulk_load_info(bulk_load_info(table_id_, table_name_, partition_count_),
                                          bulk_load_info_path));

        // Generate '.bulk_load_info.meta'.
        NO_FATALS(generate_bulk_load_info_meta(bulk_load_info_path));
    }

    error_code start_bulk_load(bool ingest_behind = false)
    {
        return ddl_client_
            ->start_bulk_load(table_name_, kCluster, kProvider, kBulkLoad, ingest_behind)
            .get_value()
            .err;
    }

    void remove_file(const string &file_path)
    {
        NO_FATALS(run_cmd_from_project_root("rm " + file_path));
    }

    void wait_bulk_load_finish(bulk_load_status::type expect_bls_type)
    {
        ASSERT_IN_TIME(
            [&] {
                auto last_status = bulk_load_status::BLS_INVALID;
                // When bulk load finished, 'err' will transfer to ERR_INVALID_STATE.
                auto resp = ddl_client_->query_bulk_load(table_name_).get_value();
                ASSERT_EQ(ERR_OK, resp.err);
                ASSERT_EQ(expect_bls_type, resp.app_status);
            },
            300);
    }

    void verify_bulk_load_data()
    {
        NO_FATALS(verify_data(kBulkLoadHashKeyPrefix1, kBulkLoadSortKeyPrefix1));
        NO_FATALS(verify_data(kBulkLoadHashKeyPrefix2, kBulkLoadSortKeyPrefix2));
    }

    void verify_data(const string &hashkey_prefix, const string &sortkey_prefix)
    {
        for (int i = 0; i < kBulkLoadItemCount; ++i) {
            string hash_key = hashkey_prefix + std::to_string(i);
            for (int j = 0; j < kBulkLoadItemCount; ++j) {
                string sort_key = sortkey_prefix + std::to_string(j);
                string actual_value;
                ASSERT_EQ(PERR_OK, client_->get(hash_key, sort_key, actual_value))
                    << hash_key << "," << sort_key;
                ASSERT_EQ(kBulkLoadValue, actual_value) << hash_key << "," << sort_key;
            }
        }
    }

    enum class operation
    {
        GET,
        SET,
        DEL,
        NO_VALUE
    };
    void operate_data(operation op, const string &value, int count)
    {
        for (int i = 0; i < count; ++i) {
            auto hash_key = fmt::format("{}{}", kBulkLoadHashKeyPrefix2, i);
            auto sort_key = fmt::format("{}{}", kBulkLoadSortKeyPrefix2, i);
            switch (op) {
            case operation::GET: {
                string actual_value;
                ASSERT_EQ(PERR_OK, client_->get(hash_key, sort_key, actual_value));
                ASSERT_EQ(value, actual_value);
            } break;
            case operation::DEL: {
                ASSERT_EQ(PERR_OK, client_->del(hash_key, sort_key));
            } break;
            case operation::SET: {
                ASSERT_EQ(PERR_OK, client_->set(hash_key, sort_key, value));
            } break;
            case operation::NO_VALUE: {
                string actual_value;
                ASSERT_EQ(PERR_NOT_FOUND, client_->get(hash_key, sort_key, actual_value));
            } break;
            default:
                ASSERT_TRUE(false);
                break;
            }
        }
    }

    void check_bulk_load(bool ingest_behind,
                         const std::string &value_before_bulk_load,
                         const std::string &value_after_bulk_load)
    {
        // Write some data before bulk load.
        NO_FATALS(operate_data(operation::SET, value_before_bulk_load, 10));
        NO_FATALS(operate_data(operation::GET, value_before_bulk_load, 10));

        // Start bulk load and wait until it complete.
        ASSERT_EQ(ERR_OK, start_bulk_load(ingest_behind));
        NO_FATALS(wait_bulk_load_finish(bulk_load_status::BLS_SUCCEED));

        std::cout << "Start to verify data..." << std::endl;
        if (ingest_behind) {
            // Values have NOT been overwritten by the bulk load data.
            NO_FATALS(operate_data(operation::GET, value_before_bulk_load, 10));
            NO_FATALS(verify_data(kBulkLoadHashKeyPrefix1, kBulkLoadSortKeyPrefix1));
        } else {
            // Values have been overwritten by the bulk load data.
            NO_FATALS(operate_data(operation::GET, kBulkLoadValue, 10));
            NO_FATALS(verify_bulk_load_data());
        }

        // Write new data succeed after bulk load.
        NO_FATALS(operate_data(operation::SET, value_after_bulk_load, 20));
        NO_FATALS(operate_data(operation::GET, value_after_bulk_load, 20));

        // Delete data succeed after bulk load.
        NO_FATALS(operate_data(operation::DEL, "", 15));
        NO_FATALS(operate_data(operation::NO_VALUE, "", 15));
    }

protected:
    string bulk_load_local_app_root_;
    const string kSourceFilesRoot =
        "src/test/function_test/bulk_load/pegasus-bulk-load-function-test-files";
    const string kLocalServiceRoot = "onebox/block_service/local_service";
    const string kLocalBulkLoadRoot = "onebox/block_service/local_service/bulk_load_root";
    const string kBulkLoad = "bulk_load_root";
    const string kCluster = "cluster";
    const string kProvider = "local_service";

    const int32_t kBulkLoadItemCount = 1000;
    const string kBulkLoadHashKeyPrefix1 = "hashkey";
    const string kBulkLoadSortKeyPrefix1 = "sortkey";
    const string kBulkLoadValue = "newValue";

    // Real time write operations will use this prefix as well.
    const string kBulkLoadHashKeyPrefix2 = "hash";
    const string kBulkLoadSortKeyPrefix2 = "sort";
};

// Test bulk load failed because the 'bulk_load_info' file is missing
TEST_F(bulk_load_test, missing_bulk_load_info)
{
    NO_FATALS(remove_file(bulk_load_local_app_root_ + "/bulk_load_info"));
    ASSERT_EQ(ERR_OBJECT_NOT_FOUND, start_bulk_load());
}

// Test bulk load failed because the 'bulk_load_info' file is inconsistent with the actual app info.
TEST_F(bulk_load_test, inconsistent_bulk_load_info)
{
    // Only 'app_id' and 'partition_count' will be checked in Pegasus server, so just inject these
    // kind of inconsistencies.
    bulk_load_info tests[] = {{table_id_ + 1, table_name_, partition_count_},
                              {table_id_, table_name_, partition_count_ * 2}};
    for (const auto &test : tests) {
        // Generate inconsistent 'bulk_load_info'.
        auto bulk_load_info_path =
            fmt::format("{}/{}/{}/bulk_load_info", kLocalBulkLoadRoot, kCluster, table_name_);
        NO_FATALS(generate_bulk_load_info(test, bulk_load_info_path));

        // Generate '.bulk_load_info.meta'.
        NO_FATALS(generate_bulk_load_info_meta(bulk_load_info_path));

        ASSERT_EQ(ERR_INCONSISTENT_STATE, start_bulk_load()) << test.app_id << "," << test.app_name
                                                             << "," << test.partition_count;
    }
}

// Test bulk load failed because partition[0]'s 'bulk_load_metadata' file is missing.
TEST_F(bulk_load_test, missing_p0_bulk_load_metadata)
{
    NO_FATALS(remove_file(bulk_load_local_app_root_ + "/0/bulk_load_metadata"));
    ASSERT_EQ(ERR_OK, start_bulk_load());
    NO_FATALS(wait_bulk_load_finish(bulk_load_status::BLS_FAILED));
}

// Test bulk load failed because the allow_ingest_behind config is inconsistent.
TEST_F(bulk_load_test, allow_ingest_behind_inconsistent)
{
    NO_FATALS(update_table_env({ROCKSDB_ALLOW_INGEST_BEHIND}, {"false"}));
    ASSERT_EQ(ERR_INCONSISTENT_STATE, start_bulk_load(true));
}

// Test normal bulk load, old data will be overwritten by bulk load data.
TEST_F(bulk_load_test, normal) { check_bulk_load(false, "oldValue", "valueAfterBulkLoad"); }

// Test normal bulk load with allow_ingest_behind=true, old data will NOT be overwritten by bulk
// load data.
TEST_F(bulk_load_test, allow_ingest_behind)
{
    NO_FATALS(update_table_env({ROCKSDB_ALLOW_INGEST_BEHIND}, {"true"}));
    check_bulk_load(true, "oldValue", "valueAfterBulkLoad");
}
