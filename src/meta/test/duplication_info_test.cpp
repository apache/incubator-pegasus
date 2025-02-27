/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "meta/duplication/duplication_info.h"

#include <boost/algorithm/string/replace.hpp>

#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "runtime/app_model.h"
#include "test_util/test_util.h"
#include "utils/flags.h"

DSN_DECLARE_uint64(dup_progress_min_update_period_ms);

namespace dsn::replication {

class duplication_info_test : public testing::Test
{
public:
    static const std::string kTestAppName;
    static const std::string kTestRemoteClusterName;
    static const std::string kTestRemoteAppName;
    static const std::string kTestMetaStorePath;
    static const int32_t kTestRemoteReplicaCount;

    void force_update_status(duplication_info &dup, duplication_status::type status)
    {
        dup._status = status;
    }

    static void test_duplication_entry_for_sync(const duplication_info &dup,
                                                int partition_index,
                                                decree expected_confirmed_decree)
    {
        const auto &entry = dup.to_partition_level_entry_for_sync();
        ASSERT_TRUE(gutil::ContainsKey(entry.progress, partition_index));
        ASSERT_EQ(expected_confirmed_decree, gutil::FindOrDie(entry.progress, partition_index));
    }

    static void test_duplication_entry_for_list(const duplication_info &dup,
                                                int partition_index,
                                                decree expected_confirmed_decree,
                                                decree expected_last_committed_decree)
    {
        const auto &entry = dup.to_partition_level_entry_for_list();
        ASSERT_TRUE(gutil::ContainsKey(entry.partition_states, partition_index));

        const auto &state = gutil::FindOrDie(entry.partition_states, partition_index);
        ASSERT_EQ(expected_confirmed_decree, state.confirmed_decree);
        ASSERT_EQ(expected_last_committed_decree, state.last_committed_decree);
    }

    static void test_duplication_entry(const duplication_info &dup,
                                       int partition_index,
                                       decree expected_confirmed_decree,
                                       decree expected_last_committed_decree)
    {
        test_duplication_entry_for_sync(dup, partition_index, expected_confirmed_decree);
        test_duplication_entry_for_list(
            dup, partition_index, expected_confirmed_decree, expected_last_committed_decree);
    }

    static void
    test_init_progress(duplication_info &dup, int partition_index, decree expected_decree)
    {
        dup.init_progress(partition_index, expected_decree);

        ASSERT_TRUE(gutil::ContainsKey(dup._progress, partition_index));

        const auto &progress = dup._progress[partition_index];
        ASSERT_EQ(invalid_decree, progress.last_committed_decree);
        ASSERT_EQ(expected_decree, progress.volatile_decree);
        ASSERT_EQ(expected_decree, progress.stored_decree);
        ASSERT_FALSE(progress.is_altering);
        ASSERT_EQ(0, progress.last_progress_update_ms);
        ASSERT_TRUE(progress.is_inited);
        ASSERT_FALSE(progress.checkpoint_prepared);

        test_duplication_entry(dup, partition_index, expected_decree, invalid_decree);
    }

    static void test_alter_progress()
    {
        duplication_info dup(1,
                             1,
                             kTestAppName,
                             2,
                             kTestRemoteReplicaCount,
                             0,
                             kTestRemoteClusterName,
                             kTestRemoteAppName,
                             std::vector<host_port>(),
                             kTestMetaStorePath);

        // Failed to alter progres for partition 0 since it has not been initialized.
        ASSERT_FALSE(dup.alter_progress(0, duplication_confirm_entry()));

        // Initialize progress for partition 0.
        test_init_progress(dup, 0, invalid_decree);

        // Alter progress with specified decrees for partition 0.
        duplication_confirm_entry entry;
        entry.__set_last_committed_decree(8);
        entry.confirmed_decree = 5;
        entry.checkpoint_prepared = true;
        ASSERT_TRUE(dup.alter_progress(0, entry));

        ASSERT_EQ(8, dup._progress[0].last_committed_decree);
        ASSERT_EQ(5, dup._progress[0].volatile_decree);
        ASSERT_EQ(invalid_decree, dup._progress[0].stored_decree);
        ASSERT_TRUE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);
        test_duplication_entry(dup, 0, invalid_decree, 8);

        // Busy updating.
        entry.__set_last_committed_decree(15);
        entry.confirmed_decree = 10;
        entry.checkpoint_prepared = false;
        ASSERT_FALSE(dup.alter_progress(0, entry));

        // last_committed_decree could be updated at any time.
        ASSERT_EQ(15, dup._progress[0].last_committed_decree);
        ASSERT_EQ(5, dup._progress[0].volatile_decree);
        ASSERT_EQ(invalid_decree, dup._progress[0].stored_decree);
        ASSERT_TRUE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);
        test_duplication_entry(dup, 0, invalid_decree, 15);

        // Persist progress for partition 0.
        dup.persist_progress(0);

        ASSERT_EQ(15, dup._progress[0].last_committed_decree);
        ASSERT_EQ(5, dup._progress[0].volatile_decree);
        ASSERT_EQ(5, dup._progress[0].stored_decree);
        ASSERT_FALSE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);
        test_duplication_entry(dup, 0, 5, 15);

        // Initialize progress for partition 1.
        test_init_progress(dup, 1, 5);

        // Alter progress for partition 1.
        ASSERT_TRUE(dup.alter_progress(1, entry));

        ASSERT_EQ(15, dup._progress[1].last_committed_decree);
        ASSERT_EQ(10, dup._progress[1].volatile_decree);
        ASSERT_EQ(5, dup._progress[1].stored_decree);
        ASSERT_TRUE(dup._progress[1].is_altering);
        ASSERT_FALSE(dup._progress[1].checkpoint_prepared);
        test_duplication_entry(dup, 1, 5, 15);

        // Persist progress for partition 1.
        dup.persist_progress(1);

        // It is too frequent to alter progress.
        PRESERVE_FLAG(dup_progress_min_update_period_ms);
        FLAGS_dup_progress_min_update_period_ms = 10000;
        entry.__set_last_committed_decree(25);
        entry.confirmed_decree = 15;
        entry.checkpoint_prepared = true;
        ASSERT_FALSE(dup.alter_progress(1, entry));
        ASSERT_EQ(25, dup._progress[1].last_committed_decree);
        // volatile_decree would be updated successfully even if it is too frequent.
        ASSERT_EQ(15, dup._progress[1].volatile_decree);
        ASSERT_EQ(10, dup._progress[1].stored_decree);
        ASSERT_FALSE(dup._progress[1].is_altering);
        // checkpoint_prepared would be updated successfully even if it is too frequent.
        ASSERT_TRUE(dup._progress[1].checkpoint_prepared);
        test_duplication_entry(dup, 1, 10, 25);

        // Reduce last update timestamp to make it infrequent.
        dup._progress[1].last_progress_update_ms -= FLAGS_dup_progress_min_update_period_ms + 100;
        entry.__set_last_committed_decree(26);
        entry.confirmed_decree = 25;

        ASSERT_TRUE(dup.alter_progress(1, entry));
        ASSERT_EQ(26, dup._progress[1].last_committed_decree);
        ASSERT_EQ(25, dup._progress[1].volatile_decree);
        ASSERT_EQ(10, dup._progress[1].stored_decree);
        ASSERT_TRUE(dup._progress[1].is_altering);
        ASSERT_TRUE(dup._progress[1].checkpoint_prepared);
        test_duplication_entry(dup, 1, 10, 26);

        // Checkpoint are ready for both partition 0 and 1.
        ASSERT_TRUE(dup.all_checkpoint_has_prepared());
    }

    static void test_init_and_start()
    {
        duplication_info dup(1,
                             1,
                             kTestAppName,
                             4,
                             kTestRemoteReplicaCount,
                             0,
                             kTestRemoteClusterName,
                             kTestRemoteAppName,
                             std::vector<host_port>(),
                             kTestMetaStorePath);
        ASSERT_FALSE(dup.is_altering());
        ASSERT_EQ(duplication_status::DS_INIT, dup._status);
        ASSERT_EQ(duplication_status::DS_INIT, dup._next_status);

        {
            const auto &entry = dup.to_partition_level_entry_for_sync();
            ASSERT_TRUE(entry.progress.empty());
            ASSERT_EQ(kTestRemoteAppName, entry.remote_app_name);
            ASSERT_EQ(kTestRemoteReplicaCount, entry.remote_replica_count);
        }

        for (int i = 0; i < 4; ++i) {
            dup.init_progress(i, invalid_decree);
        }

        {
            const auto &entry = dup.to_partition_level_entry_for_sync();
            ASSERT_EQ(4, entry.progress.size());
            for (int partition_index = 0; partition_index < 4; ++partition_index) {
                ASSERT_TRUE(gutil::ContainsKey(entry.progress, partition_index));
                ASSERT_EQ(invalid_decree, gutil::FindOrDie(entry.progress, partition_index));
            }
        }

        dup.start();
        ASSERT_TRUE(dup.is_altering());
        ASSERT_EQ(duplication_status::DS_INIT, dup._status);
        ASSERT_EQ(duplication_status::DS_PREPARE, dup._next_status);
    }

    static void test_persist_status()
    {
        duplication_info dup(1,
                             1,
                             kTestAppName,
                             4,
                             kTestRemoteReplicaCount,
                             0,
                             kTestRemoteClusterName,
                             kTestRemoteAppName,
                             std::vector<host_port>(),
                             kTestMetaStorePath);
        dup.start();

        dup.persist_status();
        ASSERT_EQ(duplication_status::DS_PREPARE, dup._status);
        ASSERT_EQ(duplication_status::DS_INIT, dup._next_status);
        ASSERT_FALSE(dup.is_altering());
    }

    static void test_encode_and_decode()
    {
        dsn_run_config("config-test.ini", false);
        duplication_info dup(1,
                             1,
                             kTestAppName,
                             4,
                             kTestRemoteReplicaCount,
                             0,
                             kTestRemoteClusterName,
                             kTestRemoteAppName,
                             std::vector<host_port>(),
                             kTestMetaStorePath);
        dup.start();
        dup.persist_status();

        dup.alter_status(duplication_status::DS_APP);
        const auto &json = dup.to_json_blob();
        dup.persist_status();

        duplication_info::json_helper copy;
        ASSERT_TRUE(json::json_forwarder<duplication_info::json_helper>::decode(json, copy));
        ASSERT_EQ(duplication_status::DS_APP, copy.status);
        ASSERT_EQ(dup.create_timestamp_ms, copy.create_timestamp_ms);
        ASSERT_EQ(dup.remote_cluster_name, copy.remote);
        ASSERT_EQ(dup.remote_app_name, copy.remote_app_name);
        ASSERT_EQ(kTestRemoteAppName, copy.remote_app_name);
        ASSERT_EQ(dup.remote_replica_count, copy.remote_replica_count);
        ASSERT_EQ(kTestRemoteReplicaCount, copy.remote_replica_count);

        auto dup_sptr = duplication_info::decode_from_blob(
            1, 1, kTestAppName, 4, kTestRemoteReplicaCount, kTestMetaStorePath, json);
        ASSERT_TRUE(dup_sptr->equals_to(dup)) << *dup_sptr << " " << dup;

        blob new_json =
            blob::create_from_bytes(boost::replace_all_copy(json.to_string(), "DS_APP", "DS_FOO"));
        ASSERT_FALSE(json::json_forwarder<duplication_info::json_helper>::decode(new_json, copy));
        ASSERT_EQ(duplication_status::DS_REMOVED, copy.status);
    }

    static void test_encode_and_decode_default()
    {
        dsn_run_config("config-test.ini", false);

        duplication_info::json_helper copy;
        copy.status = duplication_status::DS_INIT;
        copy.fail_mode = duplication_fail_mode::FAIL_SLOW;
        copy.remote = kTestRemoteClusterName;
        ASSERT_TRUE(copy.remote_app_name.empty());
        ASSERT_EQ(0, copy.remote_replica_count);

        const auto json = json::json_forwarder<duplication_info::json_helper>::encode(copy);
        auto dup = duplication_info::decode_from_blob(
            1, 1, kTestAppName, 4, kTestRemoteReplicaCount, kTestMetaStorePath, json);
        ASSERT_EQ(kTestAppName, dup->remote_app_name);
        ASSERT_EQ(kTestRemoteReplicaCount, dup->remote_replica_count);
    }
};

const std::string duplication_info_test::kTestAppName = "temp";
const std::string duplication_info_test::kTestRemoteClusterName = "slave-cluster";
const std::string duplication_info_test::kTestRemoteAppName = "remote_temp";
const std::string duplication_info_test::kTestMetaStorePath = "/meta_test/101/duplication/1";
const int32_t duplication_info_test::kTestRemoteReplicaCount = 3;

TEST_F(duplication_info_test, alter_status_when_busy)
{
    duplication_info dup(1,
                         1,
                         kTestAppName,
                         4,
                         kTestRemoteReplicaCount,
                         0,
                         kTestRemoteClusterName,
                         kTestRemoteAppName,
                         std::vector<host_port>(),
                         kTestMetaStorePath);
    dup.start();

    ASSERT_EQ(dup.alter_status(duplication_status::DS_PAUSE), ERR_BUSY);
}

TEST_F(duplication_info_test, alter_status)
{
    struct TestData
    {
        std::vector<duplication_status::type> from_list;
        std::vector<duplication_status::type> to_list;

        error_code wec;
    } tests[] = {
        {{duplication_status::DS_INIT, duplication_status::DS_PREPARE},
         {duplication_status::DS_PREPARE},
         ERR_OK},
        {{duplication_status::DS_PREPARE, duplication_status::DS_APP},
         {duplication_status::DS_APP},
         ERR_OK},
        {{duplication_status::DS_INIT,
          duplication_status::DS_APP,
          duplication_status::DS_PAUSE,
          duplication_status::DS_LOG},
         {duplication_status::DS_LOG},
         ERR_OK},
        {{duplication_status::DS_LOG, duplication_status::DS_PAUSE},
         {duplication_status::DS_PAUSE},
         ERR_OK},

        {{duplication_status::DS_INIT,
          duplication_status::DS_PREPARE,
          duplication_status::DS_APP,
          duplication_status::DS_PAUSE,
          duplication_status::DS_LOG},
         {duplication_status::DS_REMOVED},
         ERR_OK},

        {{duplication_status::DS_PREPARE,
          duplication_status::DS_APP,
          duplication_status::DS_PAUSE,
          duplication_status::DS_LOG},
         {duplication_status::DS_INIT},
         ERR_INVALID_PARAMETERS},

        {{duplication_status::DS_REMOVED},
         {duplication_status::DS_INIT,
          duplication_status::DS_PREPARE,
          duplication_status::DS_APP,
          duplication_status::DS_PAUSE,
          duplication_status::DS_LOG},
         ERR_OBJECT_NOT_FOUND},

        {{duplication_status::DS_INIT, duplication_status::DS_PREPARE, duplication_status::DS_APP},
         {duplication_status::DS_PAUSE},
         ERR_INVALID_PARAMETERS},

        {{duplication_status::DS_PREPARE}, {duplication_status::DS_LOG}, ERR_INVALID_PARAMETERS},

        {{duplication_status::DS_LOG},
         {duplication_status::DS_INIT, duplication_status::DS_PREPARE, duplication_status::DS_APP},
         ERR_INVALID_PARAMETERS}};

    for (auto tt : tests) {
        duplication_info dup(1,
                             1,
                             kTestAppName,
                             4,
                             kTestRemoteReplicaCount,
                             0,
                             kTestRemoteClusterName,
                             kTestRemoteAppName,
                             std::vector<host_port>(),
                             kTestMetaStorePath);
        for (const auto from : tt.from_list) {
            force_update_status(dup, from);
            for (const auto to : tt.to_list) {
                ASSERT_EQ(dup.alter_status(to), tt.wec);
                if (dup.is_altering()) {
                    dup.persist_status();
                }
            }
        }
    }
}

TEST_F(duplication_info_test, alter_progress) { test_alter_progress(); }

TEST_F(duplication_info_test, persist_status) { test_persist_status(); }

TEST_F(duplication_info_test, init_and_start) { test_init_and_start(); }

TEST_F(duplication_info_test, encode_and_decode) { test_encode_and_decode(); }

TEST_F(duplication_info_test, encode_and_decode_default) { test_encode_and_decode_default(); }

TEST_F(duplication_info_test, is_valid)
{
    duplication_info dup(1,
                         1,
                         kTestAppName,
                         4,
                         kTestRemoteReplicaCount,
                         0,
                         kTestRemoteClusterName,
                         kTestRemoteAppName,
                         std::vector<host_port>(),
                         kTestMetaStorePath);
    ASSERT_TRUE(dup.is_invalid_status());

    dup.start();
    dup.persist_status();
    ASSERT_FALSE(dup.is_invalid_status());

    ASSERT_EQ(dup.alter_status(duplication_status::DS_APP), ERR_OK);
    dup.persist_status();
    ASSERT_FALSE(dup.is_invalid_status());

    ASSERT_EQ(dup.alter_status(duplication_status::DS_REMOVED), ERR_OK);
    dup.persist_status();
    ASSERT_TRUE(dup.is_invalid_status());
}

} // namespace dsn::replication
