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

#include "utils/fmt_logging.h"
#include "utils/defer.h"
#include "utils/fail_point.h"

#define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem/operations.hpp>
#undef BOOST_NO_CXX11_SCOPED_ENUMS

#include "replica/mutation_log_utils.h"
#include "replica/duplication/load_from_private_log.h"
#include "duplication_test_base.h"

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
                std::string msg = "hello!";
                mutation_ptr mu = create_test_mutation(10 * f + i, msg);
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
            DSN_DECLARE_bool(plog_force_flush);
            auto reserved_plog_force_flush = FLAGS_plog_force_flush;
            FLAGS_plog_force_flush = true;
            for (int i = decree_start; i <= num_entries + decree_start; i++) {
                std::string msg = "hello!";
                //  decree - last_commit_decree  = 1 by default
                mutation_ptr mu = create_test_mutation(i, msg);
                // mock the last_commit_decree of first mu equal with `last_commit_decree_start`
                if (i == decree_start) {
                    mu->data.header.last_committed_decree = last_commit_decree_start;
                }
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }

            // commit the last entry
            mutation_ptr mu = create_test_mutation(decree_start + num_entries + 1, "hello!");
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

TEST_F(load_from_private_log_test, find_log_file_to_start) { test_find_log_file_to_start(); }

TEST_F(load_from_private_log_test, start_duplication_10000_4MB)
{
    test_start_duplication(10000, 4);
}

TEST_F(load_from_private_log_test, start_duplication_50000_4MB)
{
    test_start_duplication(50000, 4);
}

TEST_F(load_from_private_log_test, start_duplication_10000_1MB)
{
    test_start_duplication(10000, 1);
}

TEST_F(load_from_private_log_test, start_duplication_50000_1MB)
{
    test_start_duplication(50000, 1);
}

TEST_F(load_from_private_log_test, start_duplication_100000_4MB)
{
    test_start_duplication(100000, 4);
}

// Ensure replica_duplicator can correctly handle real-world log file
TEST_F(load_from_private_log_test, handle_real_private_log)
{
    struct test_data
    {
        std::string fname;
        int puts;
        int total;
        gpid id;
    } tests[] = {
        // PUT, PUT, PUT, EMPTY, PUT, EMPTY, EMPTY
        {"log.1.0.handle_real_private_log", 4, 6, gpid(1, 4)},

        // EMPTY, PUT, EMPTY
        {"log.1.0.handle_real_private_log2", 1, 2, gpid(1, 4)},

        // EMPTY, EMPTY, EMPTY
        {"log.1.0.all_loaded_are_write_empties", 0, 2, gpid(1, 5)},
    };

    for (auto tt : tests) {
        boost::filesystem::path file(tt.fname);
        boost::filesystem::copy_file(
            file, _log_dir + "/log.1.0", boost::filesystem::copy_option::overwrite_if_exists);

        // reset replica to specified gpid
        duplicator.reset(nullptr);
        _replica = create_mock_replica(
            stub.get(), tt.id.get_app_id(), tt.id.get_partition_index(), _log_dir.c_str());

        load_and_wait_all_entries_loaded(tt.puts, tt.total, tt.id, 1, 0);
    }
}

TEST_F(load_from_private_log_test, restart_duplication) { test_restart_duplication(); }

TEST_F(load_from_private_log_test, ignore_useless)
{
    utils::filesystem::remove_path(_log_dir);

    mutation_log_ptr mlog = create_private_log();

    int num_entries = 100;
    for (int i = 1; i <= num_entries; i++) {
        std::string msg = "hello!";
        mutation_ptr mu = create_test_mutation(i, msg);
        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }

    // commit the last entry
    mutation_ptr mu = create_test_mutation(1 + num_entries, "hello!");
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
        load = make_unique<load_from_private_log>(_replica.get(), duplicator.get());
        load->TEST_set_repeat_delay(0_ms); // no delay
        load->set_start_decree(duplicator->progress().last_decree + 1);
        end_stage = make_unique<end_stage_t>(
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

TEST_F(load_fail_mode_test, fail_skip)
{
    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(load->_counter_dup_load_skipped_bytes_count->get_integer_value(), 0);

    // will trigger fail-skip and read the subsequent file, some mutations will be lost.
    auto repeats = load->MAX_ALLOWED_BLOCK_REPEATS * load->MAX_ALLOWED_FILE_REPEATS;
    fail::setup();
    fail::cfg("mutation_log_replay_block", fmt::format("100%{}*return()", repeats));
    duplicator->run_pipeline();
    duplicator->wait_all();
    fail::teardown();

    ASSERT_EQ(load->_counter_dup_load_file_failed_count->get_integer_value(),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_GT(load->_counter_dup_load_skipped_bytes_count->get_integer_value(), 0);
}

TEST_F(load_fail_mode_test, fail_slow)
{
    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SLOW);
    ASSERT_EQ(load->_counter_dup_load_skipped_bytes_count->get_integer_value(), 0);
    ASSERT_EQ(load->_counter_dup_load_file_failed_count->get_integer_value(), 0);

    // will trigger fail-slow and retry infinitely
    auto repeats = load->MAX_ALLOWED_BLOCK_REPEATS * load->MAX_ALLOWED_FILE_REPEATS;
    fail::setup();
    fail::cfg("mutation_log_replay_block", fmt::format("100%{}*return()", repeats));
    duplicator->run_pipeline();
    duplicator->wait_all();
    fail::teardown();

    ASSERT_EQ(load->_counter_dup_load_file_failed_count->get_integer_value(),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_EQ(load->_counter_dup_load_skipped_bytes_count->get_integer_value(), 0);
}

TEST_F(load_fail_mode_test, fail_skip_real_corrupted_file)
{
    { // inject some bad data in the middle of the first file
        std::string log_path = _log_dir + "/log.1.0";
        auto file_size = boost::filesystem::file_size(log_path);
        int fd = open(log_path.c_str(), O_WRONLY);
        const char buf[] = "xxxxxx";
        auto written_size = pwrite(fd, buf, sizeof(buf), file_size / 2);
        ASSERT_EQ(written_size, sizeof(buf));
        close(fd);
    }

    duplicator->update_fail_mode(duplication_fail_mode::FAIL_SKIP);
    duplicator->run_pipeline();
    duplicator->wait_all();

    // ensure the bad file will be skipped
    ASSERT_EQ(load->_counter_dup_load_file_failed_count->get_integer_value(),
              load_from_private_log::MAX_ALLOWED_FILE_REPEATS);
    ASSERT_GT(load->_counter_dup_load_skipped_bytes_count->get_integer_value(), 0);
}

} // namespace replication
} // namespace dsn
