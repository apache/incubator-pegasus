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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <sys/types.h>

#include "aio/aio_task.h"
#include "aio/file_io.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "duplication_types.h"
#include "gtest/gtest.h"
#include "replica/duplication/mutation_duplicator.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/log_file.h"
#include "replica/mutation.h"
#include "replica/mutation_log.h"
#include "replica/test/mock_utils.h"
#include "rpc/rpc_holder.h"
#include "runtime/pipeline.h"
#include "task/task_code.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/chrono_literals.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"

#define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem/operations.hpp>
#include <chrono>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#undef BOOST_NO_CXX11_SCOPED_ENUMS

#include "duplication_test_base.h"
#include "replica/duplication/load_from_private_log.h"
#include "replica/mutation_log_utils.h"
#include "test_util/test_util.h"

DSN_DECLARE_bool(plog_force_flush);

namespace dsn {
namespace replication {

DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_PUT, ALLOW_BATCH, IS_IDEMPOTENT)

class load_from_private_log_test : public duplication_test_base
{
public:
    load_from_private_log_test()
    {
        _replica->init_private_log(_log_dir);
        duplicator = create_test_duplicator();
    }

    // return number of entries written
    int generate_multiple_log_files(uint files_num = 3)
    {
        // decree ranges from [1, files_num*10)
        for (int f = 0; f < files_num; f++) {
            // each round mlog will replay the former logs, and create new file
            mutation_log_ptr mlog = create_private_log();
            for (int i = 1; i <= 10; i++) {
                auto mu = create_test_mutation(10 * f + i, "hello!");
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }
            mlog->tracker()->wait_outstanding_tasks();
            mlog->close();
        }
        return static_cast<int>(files_num * 10);
    }

    void test_find_log_file_to_start()
    {
        load_from_private_log load(_replica.get(), duplicator.get());

        std::vector<std::string> mutations;
        int max_log_file_mb = 1;

        mutation_log_ptr mlog = new mutation_log_private(
            _replica->dir(), max_log_file_mb, _replica->get_gpid(), _replica.get());
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        load.find_log_file_to_start({});
        ASSERT_FALSE(load._current);

        int num_entries = generate_multiple_log_files(3);

        auto files = open_log_file_map(_log_dir);

        load.set_start_decree(1);
        load.find_log_file_to_start(files);
        ASSERT_TRUE(load._current);
        ASSERT_EQ(load._current->index(), 1);

        load._current = nullptr;
        load.set_start_decree(5);
        load.find_log_file_to_start(files);
        ASSERT_TRUE(load._current);
        ASSERT_EQ(load._current->index(), 1);

        int last_idx = files.rbegin()->first;
        load._current = nullptr;
        load.set_start_decree(num_entries + 200);
        load.find_log_file_to_start(files);
        ASSERT_TRUE(load._current);
        ASSERT_EQ(load._current->index(), last_idx);
    }

    void test_start_duplication(int num_entries, int private_log_size_mb)
    {
        mutation_log_ptr mlog = create_private_log(private_log_size_mb, _replica->get_gpid());

        int last_commit_decree_start = 5;
        int decree_start = 10;
        {
            auto reserved_plog_force_flush = FLAGS_plog_force_flush;
            FLAGS_plog_force_flush = true;
            for (int i = decree_start; i <= num_entries + decree_start; i++) {
                //  decree - last_commit_decree  = 1 by default
                auto mu = create_test_mutation(i, "hello!");
                // mock the last_commit_decree of first mu equal with `last_commit_decree_start`
                if (i == decree_start) {
                    mu->data.header.last_committed_decree = last_commit_decree_start;
                }
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }

            // commit the last entry
            auto mu = create_test_mutation(decree_start + num_entries + 1, "hello!");
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            FLAGS_plog_force_flush = reserved_plog_force_flush;

            mlog->close();
        }

        load_and_wait_all_entries_loaded(num_entries, num_entries, decree_start);
    }

    mutation_tuple_set
    load_and_wait_all_entries_loaded(int total, int last_decree, decree start_decree)
    {
        return load_and_wait_all_entries_loaded(
            total, last_decree, _replica->get_gpid(), start_decree, -1);
    }

    mutation_tuple_set load_and_wait_all_entries_loaded(int total, int last_decree)
    {
        return load_and_wait_all_entries_loaded(total, last_decree, _replica->get_gpid(), 0, -1);
    }

    mutation_tuple_set load_and_wait_all_entries_loaded(
        int total, int last_decree, gpid id, decree start_decree, decree confirmed_decree)
    {
        mutation_log_ptr mlog = create_private_log(id);
        for (const auto &pr : mlog->get_log_file_map()) {
            EXPECT_TRUE(pr.second->file_handle() == nullptr);
        }
        _replica->init_private_log(mlog);
        duplicator = create_test_duplicator(confirmed_decree);

        load_from_private_log load(_replica.get(), duplicator.get());
        const_cast<std::chrono::milliseconds &>(load._repeat_delay) = 1_s;
        load.set_start_decree(start_decree);

        mutation_tuple_set loaded_mutations;
        pipeline::do_when<decree, mutation_tuple_set> end_stage(
            [&loaded_mutations, &load, total, last_decree](decree &&d,
                                                           mutation_tuple_set &&mutations) {
                // we create one mutation_update per mutation
                // the mutations are started from 1
                for (mutation_tuple mut : mutations) {
                    loaded_mutations.emplace(mut);
                }

                if (loaded_mutations.size() < total || d < last_decree) {
                    load.run();
                }
            });

        duplicator->from(load).link(end_stage);

        // inject some faults
        fail::setup();
        fail::cfg("open_read", "25%1*return()");
        fail::cfg("mutation_log_read_log_block", "25%1*return()");
        fail::cfg("duplication_sync_complete", "void()");
        duplicator->run_pipeline();
        duplicator->wait_all();
        fail::teardown();

        return loaded_mutations;
    }

    void test_restart_duplication()
    {
        load_from_private_log load(_replica.get(), duplicator.get());

        generate_multiple_log_files(2);

        std::vector<std::string> files;
        ASSERT_EQ(log_utils::list_all_files(_log_dir, files), error_s::ok());
        ASSERT_EQ(files.size(), 2);
        boost::filesystem::remove(_log_dir + "/log.1.0");

        mutation_log_ptr mlog = create_private_log();
        decree max_gced_dercee = mlog->max_gced_decree_no_lock(_replica->get_gpid());

        // new duplication, start_decree = max_gced_decree + 1
        // ensure we can find the first file.
        load.set_start_decree(max_gced_dercee + 1);
        load.find_log_file_to_start(mlog->get_log_file_map());
        ASSERT_TRUE(load._current);
        ASSERT_EQ(load._current->index(), 2);
    }

    mutation_log_ptr create_private_log(gpid id) { return create_private_log(1, id); }

    mutation_log_ptr create_private_log(int private_log_size_mb = 1, gpid id = gpid(1, 1))
    {
        std::map<gpid, decree> replay_condition;
        replay_condition[id] = 0; // duplicating
        mutation_log::replay_callback cb = [](int, mutation_ptr &) { return true; };
        mutation_log_ptr mlog;

        int try_cnt = 0;
        while (try_cnt < 5) {
            try_cnt++;
            mlog =
                new mutation_log_private(_replica->dir(), private_log_size_mb, id, _replica.get());
            error_code err = mlog->open(cb, nullptr, replay_condition);
            if (err == ERR_OK) {
                break;
            }
            LOG_ERROR("mlog open failed, encountered error: {}", err);
        }
        return mlog;
    }

    std::unique_ptr<replica_duplicator> duplicator;
};

INSTANTIATE_TEST_SUITE_P(, load_from_private_log_test, ::testing::Values(false, true));

TEST_P(load_from_private_log_test, find_log_file_to_start) { test_find_log_file_to_start(); }

TEST_P(load_from_private_log_test, start_duplication_10000_4MB)
{
    test_start_duplication(10000, 4);
}

TEST_P(load_from_private_log_test, start_duplication_50000_4MB)
{
    test_start_duplication(50000, 4);
}

TEST_P(load_from_private_log_test, start_duplication_10000_1MB)
{
    test_start_duplication(10000, 1);
}

TEST_P(load_from_private_log_test, start_duplication_50000_1MB)
{
    test_start_duplication(50000, 1);
}

TEST_P(load_from_private_log_test, start_duplication_100000_4MB)
{
    test_start_duplication(100000, 4);
}

// Ensure replica_duplicator can correctly handle real-world log file
TEST_P(load_from_private_log_test, handle_real_private_log)
{
    std::vector<std::string> log_files({"log.1.0.handle_real_private_log",
                                        "log.1.0.handle_real_private_log2",
                                        "log.1.0.all_loaded_are_write_empties"});
    if (FLAGS_encrypt_data_at_rest) {
        for (int i = 0; i < log_files.size(); i++) {
            auto s = dsn::utils::encrypt_file(log_files[i], log_files[i] + ".encrypted");
            ASSERT_TRUE(s.ok()) << s.ToString();
            log_files[i] += ".encrypted";
        }
    }

    struct test_data
    {
        int puts;
        int total;
        gpid id;
    } tests[] = {
        // PUT, PUT, PUT, EMPTY, PUT, EMPTY, EMPTY
        {4, 6, gpid(1, 4)},

        // EMPTY, PUT, EMPTY
        {1, 2, gpid(1, 4)},

        // EMPTY, EMPTY, EMPTY
        {0, 2, gpid(1, 5)},
    };

    ASSERT_EQ(log_files.size(), sizeof(tests) / sizeof(test_data));
    for (int i = 0; i < log_files.size(); i++) {
        // reset replica to specified gpid
        duplicator.reset(nullptr);
        _replica = create_mock_replica(
            stub.get(), tests[i].id.get_app_id(), tests[i].id.get_partition_index());

        // Update '_log_dir' to the corresponding replica created above.
        _log_dir = _replica->dir();
        ASSERT_TRUE(utils::filesystem::path_exists(_log_dir)) << _log_dir;

        // Copy the log file to '_log_dir'
        auto s = dsn::utils::copy_file(log_files[i], _log_dir + "/log.1.0");
        ASSERT_TRUE(s.ok()) << s.ToString();

        // Start to verify.
        load_and_wait_all_entries_loaded(tests[i].puts, tests[i].total, tests[i].id, 1, 0);
    }
}

TEST_P(load_from_private_log_test, restart_duplication) { test_restart_duplication(); }

TEST_P(load_from_private_log_test, ignore_useless)
{
    utils::filesystem::remove_path(_log_dir);

    mutation_log_ptr mlog = create_private_log();

    int num_entries = 100;
    for (int i = 1; i <= num_entries; i++) {
        auto mu = create_test_mutation(i, "hello!");
        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }

    // commit the last entry
    auto mu = create_test_mutation(1 + num_entries, "hello!");
    mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    mlog->close();

    // starts from 51
    mutation_tuple_set result =
        load_and_wait_all_entries_loaded(50, 100, _replica->get_gpid(), 51, 0);
    ASSERT_EQ(result.size(), 50);

    // starts from 100
    result = load_and_wait_all_entries_loaded(1, 100, _replica->get_gpid(), 100, 0);
    ASSERT_EQ(result.size(), 1);

    // a new duplication's confirmed_decree is invalid_decree,
    // so start_decree is 0.
    // In this case duplication will starts from last_commit(100) + 1,
    // no mutation will be loaded.
    result = load_and_wait_all_entries_loaded(0, 100, _replica->get_gpid(), 101, -1);
    ASSERT_EQ(result.size(), 0);
}

class load_fail_mode_test : public load_from_private_log_test
{
public:
    void SetUp() override
    {
        const int num_entries = generate_multiple_log_files();

        // prepare loading pipeline
        mlog = create_private_log();
        _replica->init_private_log(mlog);
        duplicator = create_test_duplicator(1);
        load = std::make_unique<load_from_private_log>(_replica.get(), duplicator.get());
        load->TEST_set_repeat_delay(0_ms); // no delay
        load->set_start_decree(duplicator->progress().last_decree + 1);
        load->METRIC_VAR_NAME(dup_log_file_load_failed_count)->reset();
        load->METRIC_VAR_NAME(dup_log_file_load_skipped_bytes)->reset();
        end_stage = std::make_unique<end_stage_t>(
            [this, num_entries](decree &&d, mutation_tuple_set &&mutations) {
                load->set_start_decree(d + 1);
                if (d < num_entries - 1) {
                    load->run();
                }
            });
        duplicator->from(*load).link(*end_stage);
    }

    mutation_log_ptr mlog;
    std::unique_ptr<load_from_private_log> load;

    using end_stage_t = pipeline::do_when<decree, mutation_tuple_set>;
    std::unique_ptr<end_stage_t> end_stage;
};

INSTANTIATE_TEST_SUITE_P(, load_fail_mode_test, ::testing::Values(false, true));

TEST_P(load_fail_mode_test, fail_skip)
{
    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_skipped_bytes), 0);

    // will trigger fail-skip and read the subsequent file, some mutations will be lost.
    auto repeats = load->MAX_ALLOWED_BLOCK_REPEATS * load->MAX_ALLOWED_FILE_REPEATS;
    fail::setup();
    fail::cfg("mutation_log_replay_block", fmt::format("100%{}*return()", repeats));
    duplicator->run_pipeline();
    duplicator->wait_all();
    fail::teardown();

    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_failed_count),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_GT(METRIC_VALUE(*load, dup_log_file_load_skipped_bytes), 0);
}

TEST_P(load_fail_mode_test, fail_slow)
{
    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SLOW);
    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_skipped_bytes), 0);
    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_failed_count), 0);

    // will trigger fail-slow and retry infinitely
    auto repeats = load->MAX_ALLOWED_BLOCK_REPEATS * load->MAX_ALLOWED_FILE_REPEATS;
    fail::setup();
    fail::cfg("mutation_log_replay_block", fmt::format("100%{}*return()", repeats));
    duplicator->run_pipeline();
    duplicator->wait_all();
    fail::teardown();

    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_failed_count),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_skipped_bytes), 0);
}

TEST_P(load_fail_mode_test, fail_skip_real_corrupted_file)
{
    { // inject some bad data in the middle of the first file
        std::string log_path = _log_dir + "/log.1.0";
        int64_t file_size;
        ASSERT_TRUE(utils::filesystem::file_size(
            log_path, dsn::utils::FileDataType::kSensitive, file_size));
        auto wfile = file::open(log_path, file::FileOpenType::kWriteOnly);
        ASSERT_NE(wfile, nullptr);

        const char buf[] = "xxxxxx";
        auto buff_len = sizeof(buf);
        auto t = ::dsn::file::write(wfile,
                                    buf,
                                    buff_len,
                                    file_size / 2,
                                    LPC_AIO_IMMEDIATE_CALLBACK,
                                    nullptr,
                                    [=](::dsn::error_code err, size_t n) {
                                        CHECK_EQ(ERR_OK, err);
                                        CHECK_EQ(buff_len, n);
                                    });
        t->wait();
        ASSERT_EQ(ERR_OK, ::dsn::file::flush(wfile));
        ASSERT_EQ(ERR_OK, ::dsn::file::close(wfile));
    }

    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SKIP);
    duplicator->run_pipeline();
    duplicator->wait_all();

    // ensure the bad file will be skipped
    ASSERT_EQ(METRIC_VALUE(*load, dup_log_file_load_failed_count),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_GT(METRIC_VALUE(*load, dup_log_file_load_skipped_bytes), 0);
}

} // namespace replication
} // namespace dsn
