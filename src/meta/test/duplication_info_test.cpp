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
#include "runtime/app_model.h"

namespace dsn {
namespace replication {

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
        duplication_confirm_entry entry;
        ASSERT_FALSE(dup.alter_progress(0, entry));

        dup.init_progress(0, invalid_decree);
        entry.confirmed_decree = 5;
        entry.checkpoint_prepared = true;
        ASSERT_TRUE(dup.alter_progress(0, entry));
        ASSERT_EQ(dup._progress[0].volatile_decree, 5);
        ASSERT_TRUE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);

        // busy updating
        entry.confirmed_decree = 10;
        entry.checkpoint_prepared = false;
        ASSERT_FALSE(dup.alter_progress(0, entry));
        ASSERT_EQ(dup._progress[0].volatile_decree, 5);
        ASSERT_TRUE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);

        dup.persist_progress(0);
        ASSERT_EQ(dup._progress[0].stored_decree, 5);
        ASSERT_FALSE(dup._progress[0].is_altering);
        ASSERT_TRUE(dup._progress[0].checkpoint_prepared);

        // too frequent to update
        dup.init_progress(1, invalid_decree);
        ASSERT_TRUE(dup.alter_progress(1, entry));
        ASSERT_TRUE(dup._progress[1].is_altering);
        dup.persist_progress(1);

        ASSERT_FALSE(dup.alter_progress(1, entry));
        ASSERT_FALSE(dup._progress[1].is_altering);

        dup._progress[1].last_progress_update_ms -=
            duplication_info::PROGRESS_UPDATE_PERIOD_MS + 100;

        entry.confirmed_decree = 15;
        entry.checkpoint_prepared = true;
        ASSERT_TRUE(dup.alter_progress(1, entry));
        ASSERT_TRUE(dup._progress[1].is_altering);
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

        auto dup_ent = dup.to_duplication_entry();
        ASSERT_EQ(0, dup_ent.progress.size());
        ASSERT_EQ(kTestRemoteAppName, dup_ent.remote_app_name);
        ASSERT_EQ(kTestRemoteReplicaCount, dup_ent.remote_replica_count);

        for (int i = 0; i < 4; i++) {
            dup.init_progress(i, invalid_decree);
        }
        for (auto kv : dup_ent.progress) {
            ASSERT_EQ(kv.second, invalid_decree);
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
        ASSERT_EQ(dup._status, duplication_status::DS_PREPARE);
        ASSERT_EQ(dup._next_status, duplication_status::DS_INIT);
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

} // namespace replication
} // namespace dsn
