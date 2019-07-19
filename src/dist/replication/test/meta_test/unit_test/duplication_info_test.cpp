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

#include "dist/replication/meta_server/duplication/duplication_info.h"

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>

namespace dsn {
namespace replication {

class duplication_info_test : public testing::Test
{
public:
    static void test_alter_progress()
    {
        duplication_info dup(
            1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
        ASSERT_FALSE(dup.alter_progress(1, 5));

        dup.init_progress(1, invalid_decree);
        ASSERT_TRUE(dup.alter_progress(1, 5));
        ASSERT_EQ(dup._progress[1].volatile_decree, 5);
        ASSERT_TRUE(dup._progress[1].is_altering);

        // busy updating
        ASSERT_FALSE(dup.alter_progress(1, 10));
        ASSERT_EQ(dup._progress[1].volatile_decree, 5);
        ASSERT_TRUE(dup._progress[1].is_altering);

        dup.persist_progress(1);
        ASSERT_EQ(dup._progress[1].stored_decree, 5);
        ASSERT_FALSE(dup._progress[1].is_altering);

        // too frequent to update
        ASSERT_FALSE(dup.alter_progress(1, 10));
        ASSERT_FALSE(dup._progress[1].is_altering);

        dup._progress[1].last_progress_update_ms -=
            duplication_info::PROGRESS_UPDATE_PERIOD_MS + 100;
        ASSERT_TRUE(dup.alter_progress(1, 15));
        ASSERT_TRUE(dup._progress[1].is_altering);
    }

    static void test_init_and_start()
    {
        duplication_info dup(
            1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
        ASSERT_FALSE(dup.is_altering());
        ASSERT_EQ(dup._status, duplication_status::DS_INIT);
        ASSERT_EQ(dup._next_status, duplication_status::DS_INIT);

        auto dup_ent = dup.to_duplication_entry();
        ASSERT_EQ(dup_ent.progress.size(), 0);

        for (int i = 0; i < 4; i++) {
            dup.init_progress(i, invalid_decree);
        }
        for (auto kv : dup_ent.progress) {
            ASSERT_EQ(kv.second, invalid_decree);
        }

        dup.start();
        ASSERT_TRUE(dup.is_altering());
        ASSERT_EQ(dup._status, duplication_status::DS_INIT);
        ASSERT_EQ(dup._next_status, duplication_status::DS_START);
    }

    static void test_persist_status()
    {
        duplication_info dup(
            1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
        dup.start();

        dup.persist_status();
        ASSERT_EQ(dup._status, duplication_status::DS_START);
        ASSERT_EQ(dup._next_status, duplication_status::DS_INIT);
        ASSERT_FALSE(dup.is_altering());
    }

    static void test_encode_and_decode()
    {
        duplication_info dup(
            1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
        dup.start();
        dup.persist_status();

        dup.alter_status(duplication_status::DS_PAUSE);
        auto json = dup.to_json_blob();
        dup.persist_status();

        duplication_info::json_helper copy;
        ASSERT_TRUE(json::json_forwarder<duplication_info::json_helper>::decode(json, copy));
        ASSERT_EQ(copy.status, duplication_status::DS_PAUSE);
        ASSERT_EQ(copy.create_timestamp_ms, dup.create_timestamp_ms);
        ASSERT_EQ(copy.remote, dup.remote);

        auto dup_sptr =
            duplication_info::decode_from_blob(1, 1, 4, "/meta_test/101/duplication/1", json);
        ASSERT_TRUE(dup_sptr->equals_to(dup)) << dup_sptr->to_string() << " " << dup.to_string();

        blob new_json = blob::create_from_bytes(
            boost::replace_all_copy(json.to_string(), "DS_PAUSE", "DS_FOO"));
        ASSERT_FALSE(json::json_forwarder<duplication_info::json_helper>::decode(new_json, copy));
        ASSERT_EQ(copy.status, duplication_status::DS_REMOVED);
    }
};

TEST_F(duplication_info_test, alter_status_when_busy)
{
    duplication_info dup(1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
    dup.start();

    ASSERT_EQ(dup.alter_status(duplication_status::DS_PAUSE), ERR_BUSY);
}

TEST_F(duplication_info_test, alter_status)
{
    struct TestData
    {
        duplication_status::type from;
        duplication_status::type to;

        error_code wec;
    } tests[] = {
        {duplication_status::DS_PAUSE, duplication_status::DS_PAUSE, ERR_OK},
        {duplication_status::DS_PAUSE, duplication_status::DS_START, ERR_OK},
        {duplication_status::DS_PAUSE, duplication_status::DS_INIT, ERR_INVALID_PARAMETERS},
        {duplication_status::DS_PAUSE, duplication_status::DS_REMOVED, ERR_OK},
        {duplication_status::DS_START, duplication_status::DS_START, ERR_OK},
        {duplication_status::DS_START, duplication_status::DS_PAUSE, ERR_OK},
        {duplication_status::DS_START, duplication_status::DS_REMOVED, ERR_OK},
        {duplication_status::DS_START, duplication_status::DS_INIT, ERR_INVALID_PARAMETERS},

        // alter unavail dup
        {duplication_status::DS_REMOVED, duplication_status::DS_INIT, ERR_OBJECT_NOT_FOUND},
        {duplication_status::DS_REMOVED, duplication_status::DS_PAUSE, ERR_OBJECT_NOT_FOUND},
        {duplication_status::DS_REMOVED, duplication_status::DS_START, ERR_OBJECT_NOT_FOUND},

        // alter status same with the previous
        {duplication_status::DS_REMOVED, duplication_status::DS_REMOVED, ERR_OBJECT_NOT_FOUND},
        {duplication_status::DS_PAUSE, duplication_status::DS_PAUSE, ERR_OK},
        {duplication_status::DS_START, duplication_status::DS_START, ERR_OK},
    };

    for (auto tt : tests) {
        duplication_info dup(
            1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
        dup.start();
        dup.persist_status();

        ASSERT_EQ(dup.alter_status(tt.from), ERR_OK);
        if (dup.is_altering()) {
            dup.persist_status();
        }

        ASSERT_EQ(dup.alter_status(tt.to), tt.wec);
    }
}

TEST_F(duplication_info_test, alter_progress) { test_alter_progress(); }

TEST_F(duplication_info_test, persist_status) { test_persist_status(); }

TEST_F(duplication_info_test, init_and_start) { test_init_and_start(); }

TEST_F(duplication_info_test, encode_and_decode) { test_encode_and_decode(); }

TEST_F(duplication_info_test, is_valid)
{
    duplication_info dup(1, 1, 4, 0, "dsn://slave-cluster/temp", "/meta_test/101/duplication/1");
    ASSERT_FALSE(dup.is_valid());

    dup.start();
    dup.persist_status();
    ASSERT_TRUE(dup.is_valid());

    ASSERT_EQ(dup.alter_status(duplication_status::DS_PAUSE), ERR_OK);
    dup.persist_status();
    ASSERT_TRUE(dup.is_valid());

    ASSERT_EQ(dup.alter_status(duplication_status::DS_REMOVED), ERR_OK);
    dup.persist_status();
    ASSERT_FALSE(dup.is_valid());
}

} // namespace replication
} // namespace dsn
