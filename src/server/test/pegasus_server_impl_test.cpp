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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <base/pegasus_key_schema.h>
#include <fmt/core.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "pegasus_server_test_base.h"
#include "replica/replica_stub.h"
#include "rpc/rpc_holder.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/serverlet.h"
#include "server/pegasus_read_service.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/test_macros.h"
#include "utils_types.h"

namespace pegasus::server {

class pegasus_server_impl_test : public pegasus_server_test_base
{
protected:
    pegasus_server_impl_test() = default;

    void test_table_level_slow_query()
    {
        // The function `on_get` will sleep 10ms for the unit test. Thus when we set
        // slow_query_threshold <= 10ms, the metric `abnormal_read_requests` will increment by 1.
        struct test_case
        {
            bool is_multi_get; // false-on_get, true-on_multi_get
            uint64_t slow_query_threshold_ms;
            uint8_t expected_incr;
        } tests[] = {{false, 10, 1}, {false, 300, 0}, {true, 10, 1}, {true, 300, 0}};

        // test key
        std::string test_hash_key = "test_hash_key";
        std::string test_sort_key = "test_sort_key";
        dsn::blob test_key;
        pegasus_generate_key(test_key, test_hash_key, test_sort_key);

        // do all of the tests
        for (auto test : tests) {
            // set table level slow query threshold
            std::map<std::string, std::string> envs;
            _server->query_app_envs(envs);
            envs[dsn::replica_envs::SLOW_QUERY_THRESHOLD] =
                std::to_string(test.slow_query_threshold_ms);
            _server->update_app_envs(envs);

            // do on_get/on_multi_get operation,
            auto before_count = _server->METRIC_VAR_VALUE(abnormal_read_requests);
            if (!test.is_multi_get) {
                get_rpc rpc(std::make_unique<dsn::blob>(test_key), dsn::apps::RPC_RRDB_RRDB_GET);
                _server->on_get(rpc);
            } else {
                ::dsn::apps::multi_get_request request;
                request.__set_hash_key(dsn::blob(test_hash_key.data(), 0, test_hash_key.size()));
                request.__set_sort_keys({dsn::blob(test_sort_key.data(), 0, test_sort_key.size())});
                ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
                multi_get_rpc rpc(std::make_unique<::dsn::apps::multi_get_request>(request),
                                  dsn::apps::RPC_RRDB_RRDB_MULTI_GET);
                _server->on_multi_get(rpc);
            }
            auto after_count = _server->METRIC_VAR_VALUE(abnormal_read_requests);

            ASSERT_EQ(before_count + test.expected_incr, after_count);
        }
    }

    void test_open_db_with_rocksdb_envs(bool is_restart)
    {
        struct create_test
        {
            std::string env_key;
            std::string env_value;
            std::string expect_value;
        } tests[] = {
            {"rocksdb.num_levels", "5", "5"},
            {"rocksdb.write_buffer_size", "33554432", "33554432"},
        };

        std::map<std::string, std::string> all_test_envs;
        {
            // Make sure all rocksdb options of ROCKSDB_DYNAMIC_OPTIONS and ROCKSDB_STATIC_OPTIONS
            // are tested.
            for (const auto &test : tests) {
                all_test_envs[test.env_key] = test.env_value;
            }
            for (const auto &option : dsn::replica_envs::ROCKSDB_DYNAMIC_OPTIONS) {
                ASSERT_TRUE(all_test_envs.find(option) != all_test_envs.end());
            }
            for (const auto &option : dsn::replica_envs::ROCKSDB_STATIC_OPTIONS) {
                ASSERT_TRUE(all_test_envs.find(option) != all_test_envs.end());
            }
        }

        ASSERT_EQ(dsn::ERR_OK, start(all_test_envs));
        if (is_restart) {
            ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
            ASSERT_EQ(dsn::ERR_OK, start());
        }

        std::map<std::string, std::string> query_envs;
        _server->query_app_envs(query_envs);
        for (const auto &test : tests) {
            const auto &iter = query_envs.find(test.env_key);
            if (iter != query_envs.end()) {
                ASSERT_EQ(iter->second, test.expect_value);
            } else {
                ASSERT_TRUE(false) << fmt::format("query_app_envs not supported {}", test.env_key);
            }
        }
    }

    // Test last checkpoint query in `pegasus_server_impl_test` rather than in `replica_test`,
    // because `pegasus_server_impl::get_checkpoint()` needs to be tested instead of being
    // mocked.
    void test_query_last_checkpoint(bool create_checkpoint_dir,
                                    const dsn::gpid &pid,
                                    dsn::utils::checksum_type::type checksum_type,
                                    const dsn::error_code &expected_err,
                                    dsn::replication::decree expected_last_committed_decree,
                                    dsn::replication::decree expected_last_durable_decree,
                                    const std::vector<std::string> &expected_file_names,
                                    const std::vector<int64_t> &expected_file_sizes,
                                    const std::vector<std::string> &expected_file_checksums)
    {
        const std::string checkpoint_dir(dsn::utils::filesystem::path_combine(
            _server->data_dir(), fmt::format("checkpoint.{}", expected_last_durable_decree)));

        // After the test case is finished, remove the whole checkpoint dir.
        const auto cleanup = dsn::defer([checkpoint_dir, expected_file_names]() {
            if (!dsn::utils::filesystem::directory_exists(checkpoint_dir)) {
                return;
            }

            ASSERT_TRUE(dsn::utils::filesystem::remove_path(checkpoint_dir));
        });

        std::map<std::string, std::shared_ptr<local_test_file>> local_files;
        if (create_checkpoint_dir) {
            ASSERT_TRUE(dsn::utils::filesystem::create_directory(checkpoint_dir));

            // Generate all files in the checkpoint dir with respective specified size.
            for (size_t i = 0; i < expected_file_names.size(); ++i) {
                const auto file_path =
                    dsn::utils::filesystem::path_combine(checkpoint_dir, expected_file_names[i]);

                std::shared_ptr<local_test_file> local_file;
                NO_FATALS(local_test_file::create(
                    file_path, std::string(expected_file_sizes[i], 'a'), local_file));
                ASSERT_TRUE(local_files.emplace(file_path, local_file).second);
            }
        }

        // Build the request to get the last checkpoint info.
        auto req = std::make_unique<dsn::replication::learn_request>();
        req->pid = pid;
        req->__set_checksum_type(checksum_type);

        // Mock the RPC.
        auto rpc = dsn::replication::query_last_checkpoint_info_rpc(std::move(req),
                                                                    RPC_QUERY_LAST_CHECKPOINT_INFO);

        // Execute the last checkpoint query on server side.
        _replica_stub->on_query_last_checkpoint(rpc);

        // The error code in the response should be match the expected.
        const auto &resp = rpc.response();
        ASSERT_EQ(expected_err, resp.err);

        // No need to check others in the response once the error code is not ERR_OK.
        if (expected_err != dsn::ERR_OK) {
            return;
        }

        ASSERT_EQ(expected_last_committed_decree, resp.last_committed_decree);
        ASSERT_STR_ENDSWITH(resp.base_local_dir,
                            fmt::format("/data/checkpoint.{}", expected_last_durable_decree));

        // Check whether all file names in the response are matched.
        check_checkpoint_file_names(resp.state.files, expected_file_names);

        // The file sizes and checksums shoule be kept empty if checksum is not required.
        if (checksum_type <= dsn::utils::checksum_type::CST_NONE) {
            ASSERT_FALSE(resp.state.__isset.file_sizes);
            ASSERT_FALSE(resp.state.__isset.file_checksums);
            ASSERT_TRUE(resp.state.file_sizes.empty());
            ASSERT_TRUE(resp.state.file_checksums.empty());
            return;
        }

        // Check whether all file sizes in the response are matched.
        ASSERT_TRUE(resp.state.__isset.file_sizes);
        check_checkpoint_file_sizes(resp.state.file_sizes, resp.state.files, expected_file_sizes);

        // Check whether all file checksums in the response are matched.
        ASSERT_TRUE(resp.state.__isset.file_checksums);
        check_checkpoint_file_checksums(
            resp.state.file_checksums, resp.state.files, expected_file_checksums);
    }

    // Only test for failed cases.
    void test_query_last_checkpoint(bool create_checkpoint_dir,
                                    const dsn::gpid &pid,
                                    const dsn::error_code &expected_err)
    {
        test_query_last_checkpoint(create_checkpoint_dir,
                                   pid,
                                   dsn::utils::checksum_type::CST_INVALID,
                                   expected_err,
                                   0,
                                   0,
                                   {},
                                   {},
                                   {});
    }

    // Only test for failed cases.
    void test_query_last_checkpoint(const dsn::gpid &pid, const dsn::error_code &expected_err)
    {
        test_query_last_checkpoint(true, pid, expected_err);
    }

    // Test for each checksum type.
    void test_query_last_checkpoint_for_all_checksum_types(
        dsn::replication::decree expected_last_committed_decree,
        dsn::replication::decree expected_last_durable_decree,
        const std::vector<std::string> &expected_file_names,
        const std::vector<int64_t> &expected_file_sizes,
        const std::vector<std::string> &expected_file_checksums)
    {
        for (const auto checksum_type : {dsn::utils::checksum_type::CST_INVALID,
                                         dsn::utils::checksum_type::CST_NONE,
                                         dsn::utils::checksum_type::CST_MD5}) {
            test_query_last_checkpoint(true,
                                       _gpid,
                                       checksum_type,
                                       dsn::ERR_OK,
                                       200,
                                       100,
                                       expected_file_names,
                                       expected_file_sizes,
                                       expected_file_checksums);
        }
    }

private:
    static void check_checkpoint_file_names(const std::vector<std::string> &file_names,
                                            const std::vector<std::string> &expected_file_names)
    {
        std::vector<std::string> ordered_file_names(file_names);
        std::sort(ordered_file_names.begin(), ordered_file_names.end());
        ASSERT_EQ(expected_file_names, ordered_file_names);
    }

    // Sort `file_properties` so that its order is consistent with that of `file_names`.
    template <typename TFileProperty>
    static std::vector<TFileProperty>
    sort_checkpoint_file_properties(const std::vector<TFileProperty> &file_properties,
                                    const std::vector<std::string> &file_names)
    {
        CHECK_EQ(file_properties.size(), file_names.size());

        std::vector<size_t> order_indices(file_names.size());
        for (size_t i = 0; i < order_indices.size(); ++i) {
            order_indices[i] = i;
        }

        std::sort(order_indices.begin(), order_indices.end(), [&file_names](size_t a, size_t b) {
            return file_names[a] < file_names[b];
        });

        std::vector<TFileProperty> ordered_file_properties;
        ordered_file_properties.reserve(file_properties.size());
        for (size_t idx : order_indices) {
            ordered_file_properties.push_back(file_properties[idx]);
        }

        return ordered_file_properties;
    }

    static void check_checkpoint_file_sizes(const std::vector<int64_t> &file_sizes,
                                            const std::vector<std::string> &file_names,
                                            const std::vector<int64_t> &expected_file_sizes)
    {
        const auto ordered_file_sizes = sort_checkpoint_file_properties(file_sizes, file_names);

        ASSERT_EQ(expected_file_sizes, ordered_file_sizes);
    }

    static void
    check_checkpoint_file_checksums(const std::vector<std::string> &file_checksums,
                                    const std::vector<std::string> &file_names,
                                    const std::vector<std::string> &expected_file_checksums)
    {
        const auto ordered_file_checksums =
            sort_checkpoint_file_properties(file_checksums, file_names);

        ASSERT_EQ(expected_file_checksums, ordered_file_checksums);
    }
};

INSTANTIATE_TEST_SUITE_P(, pegasus_server_impl_test, ::testing::Values(false, true));

TEST_P(pegasus_server_impl_test, test_table_level_slow_query)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    test_table_level_slow_query();
}

TEST_P(pegasus_server_impl_test, default_data_version)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(_server->_pegasus_data_version, 1);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_latest_options)
{
    // open a new db with no app env.
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL, _server->_usage_scenario);
    // set bulk_load scenario for the db.
    ASSERT_TRUE(
        _server->set_usage_scenario(dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD));
    ASSERT_EQ(dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    rocksdb::Options opts = _server->_db->GetOptions();
    ASSERT_EQ(1000000000, opts.level0_file_num_compaction_trigger);
    ASSERT_EQ(true, opts.disable_auto_compactions);
    // reopen the db.
    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    ASSERT_EQ(opts.level0_file_num_compaction_trigger,
              _server->_db->GetOptions().level0_file_num_compaction_trigger);
    ASSERT_EQ(opts.disable_auto_compactions, _server->_db->GetOptions().disable_auto_compactions);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_app_envs)
{
    std::map<std::string, std::string> envs;
    envs[dsn::replica_envs::ROCKSDB_USAGE_SCENARIO] =
        dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;
    ASSERT_EQ(dsn::ERR_OK, start(envs));
    ASSERT_EQ(dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_rocksdb_envs)
{
    // Hint: Verify the set_rocksdb_options_before_creating function by boolean is_restart=false.
    test_open_db_with_rocksdb_envs(false);
}

TEST_P(pegasus_server_impl_test, test_restart_db_with_rocksdb_envs)
{
    // Hint: Verify the reset_rocksdb_options function by boolean is_restart=true.
    test_open_db_with_rocksdb_envs(true);
}

TEST_P(pegasus_server_impl_test, test_stop_db_twice)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_TRUE(_server->_is_open);
    ASSERT_TRUE(_server->_db != nullptr);

    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);

    // stop again
    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);
}

TEST_P(pegasus_server_impl_test, test_update_user_specified_compaction)
{
    _server->_user_specified_compaction = "";
    std::map<std::string, std::string> envs;

    _server->update_user_specified_compaction(envs);
    ASSERT_EQ("", _server->_user_specified_compaction);

    std::string user_specified_compaction = "test";
    envs[dsn::replica_envs::USER_SPECIFIED_COMPACTION] = user_specified_compaction;
    _server->update_user_specified_compaction(envs);
    ASSERT_EQ(user_specified_compaction, _server->_user_specified_compaction);
}

TEST_P(pegasus_server_impl_test, test_load_from_duplication_data)
{
    auto origin_file = fmt::format("{}/{}", _server->duplication_dir(), "checkpoint");
    dsn::utils::filesystem::create_directory(_server->duplication_dir());
    dsn::utils::filesystem::create_file(origin_file);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(origin_file));

    EXPECT_CALL(*_server, is_duplication_follower()).WillRepeatedly(testing::Return(true));

    auto tempFolder = "invalid";
    dsn::utils::filesystem::rename_path(_server->data_dir(), tempFolder);
    ASSERT_EQ(start(), dsn::ERR_FILE_OPERATION_FAILED);

    dsn::utils::filesystem::rename_path(tempFolder, _server->data_dir());
    auto rdb_path = fmt::format("{}/rdb/", _server->data_dir());
    auto new_file = fmt::format("{}/{}", rdb_path, "checkpoint");
    ASSERT_EQ(start(), dsn::ERR_LOCAL_APP_FAILURE);
    ASSERT_TRUE(dsn::utils::filesystem::directory_exists(rdb_path));
    ASSERT_FALSE(dsn::utils::filesystem::file_exists(origin_file));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(new_file));
    dsn::utils::filesystem::remove_file_name(new_file);
}

// Fail to get last checkpoint since the replica does not exist.
TEST_P(pegasus_server_impl_test, test_query_last_checkpoint_with_replica_not_found)
{
    // To make sure the replica does not exist, give a gpid that does not exist.
    test_query_last_checkpoint(dsn::gpid(101, 101), dsn::ERR_OBJECT_NOT_FOUND);
}

// Fail to get last checkpoint since it does not exist.
TEST_P(pegasus_server_impl_test, test_query_last_checkpoint_with_last_checkpoint_not_exist)
{
    // To make sure the last checkpoint does not exist, set last_durable_decree zero.
    set_last_committed_decree(0);
    set_last_durable_decree(0);

    test_query_last_checkpoint(_gpid, dsn::ERR_PATH_NOT_FOUND);
}

// Fail to get last checkpoint since its dir does not exist.
TEST_P(pegasus_server_impl_test, test_query_last_checkpoint_with_last_checkpoint_dir_not_exist)
{
    ASSERT_EQ(dsn::ERR_OK, start());

    // Make sure the last_durable_decree is not zero.
    set_last_committed_decree(200);
    set_last_durable_decree(100);

    test_query_last_checkpoint(false, _gpid, dsn::ERR_GET_LEARN_STATE_FAILED);
}

// Succeed in getting last checkpoint whose dir is empty and has no file.
TEST_P(pegasus_server_impl_test, test_query_last_checkpoint_with_empty_dir)
{
    ASSERT_EQ(dsn::ERR_OK, start());

    // Make sure the last_durable_decree is not zero.
    set_last_committed_decree(200);
    set_last_durable_decree(100);

    test_query_last_checkpoint_for_all_checksum_types(200, 100, {}, {}, {});
}

// Succeed in getting last checkpoint whose dir is not empty and has some files.
TEST_P(pegasus_server_impl_test, test_query_last_checkpoint_with_non_empty_dir)
{
    static const std::vector<std::string> kCheckpointFileNames = {
        "test_file.1", "test_file.2", "test_file.3", "test_file.4", "test_file.5", "test_file.6"};
    static const std::vector<int64_t> kCheckpointFileSizes = {0, 1, 2, 4095, 4096, 4097};
    static const std::vector<std::string> kCheckpointFileChecksums = {
        "d41d8cd98f00b204e9800998ecf8427e",
        "0cc175b9c0f1b6a831c399e269772661",
        "4124bc0a9335c27f086f24ba207a4912",
        "559110baa849c7608ee70abe1d76273e",
        "21a199c53f422a380e20b162fb6ebe9c",
        "8cfc1a0bd8cd76599e76e5e721c6e62e"};

    ASSERT_EQ(dsn::ERR_OK, start());

    set_last_committed_decree(200);
    set_last_durable_decree(100);

    test_query_last_checkpoint_for_all_checksum_types(
        200, 100, kCheckpointFileNames, kCheckpointFileSizes, kCheckpointFileChecksums);
}

} // namespace pegasus::server
