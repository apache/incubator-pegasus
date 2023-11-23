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

#include "replica/mutation_log.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <sys/types.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <tuple>
#include <unordered_map>

#include "aio/aio_task.h"
#include "aio/file_io.h"
#include "backup_types.h"
#include "common/replication.codes.h"
#include "consensus_types.h"
#include "gtest/gtest.h"
#include "replica/log_block.h"
#include "replica/log_file.h"
#include "replica/mutation.h"
#include "replica/replica_stub.h"
#include "replica/test/mock_utils.h"
#include "replica_test_base.h"
#include "rrdb/rrdb.code.definition.h"
#include "test_util/test_util.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace dsn {
class message_ex;
} // namespace dsn

using namespace ::dsn;
using namespace ::dsn::replication;

static void overwrite_file(const char *file, int offset, const void *buf, int size)
{
    auto wfile = file::open(file, file::FileOpenType::kWriteOnly);
    ASSERT_NE(wfile, nullptr);
    auto t = ::dsn::file::write(wfile,
                                (const char *)buf,
                                size,
                                offset,
                                LPC_AIO_IMMEDIATE_CALLBACK,
                                nullptr,
                                [=](::dsn::error_code err, size_t n) {
                                    CHECK_EQ(ERR_OK, err);
                                    CHECK_EQ(size, n);
                                });
    t->wait();
    ASSERT_EQ(ERR_OK, file::flush(wfile));
    ASSERT_EQ(ERR_OK, file::close(wfile));
}

class replication_test : public pegasus::encrypt_data_test_base
{
};

INSTANTIATE_TEST_CASE_P(, replication_test, ::testing::Values(false, true));

TEST_P(replication_test, log_file)
{
    replica_log_info_map mdecrees;
    gpid gpid(1, 0);

    mdecrees[gpid] = replica_log_info(3, 0);
    std::string fpath = "./log.1.100";
    int index = 1;
    int64_t offset = 100;
    std::string str = "hello, world!";
    error_code err;
    log_file_ptr lf = nullptr;

    // write log
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists(fpath));
    lf = log_file::create_write(".", index, offset);
    ASSERT_TRUE(lf != nullptr);
    ASSERT_EQ(fpath, lf->path());
    ASSERT_EQ(index, lf->index());
    ASSERT_EQ(offset, lf->start_offset());
    ASSERT_EQ(offset, lf->end_offset());
    for (int i = 0; i < 100; i++) {
        auto writer = new log_block();

        if (i == 0) {
            binary_writer temp_writer;
            lf->write_file_header(temp_writer, mdecrees);
            writer->add(temp_writer.get_buffer());
            ASSERT_EQ(mdecrees, lf->previous_log_max_decrees());
            const auto &h = lf->header();
            ASSERT_EQ(100, h.start_global_offset);
        }

        binary_writer temp_writer;
        temp_writer.write(str);
        writer->add(temp_writer.get_buffer());

        aio_task_ptr task =
            lf->commit_log_block(*writer, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        task->wait();
        ASSERT_EQ(ERR_OK, task->error());
        ASSERT_EQ(writer->size(), task->get_transferred_size());

        lf->flush();
        offset += writer->size();

        delete writer;
    }
    lf->close();
    lf = nullptr;
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(fpath));

    // file already exist
    offset = 100;
    lf = log_file::create_write(".", index, offset);
    ASSERT_TRUE(lf == nullptr);

    // invalid file name
    lf = log_file::open_read("", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("a", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("aaaaa", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1.2.aaa", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1.2.removed", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1.", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log..2", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1a.2", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);
    lf = log_file::open_read("log.1.2a", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, err);

    // file not exist
    lf = log_file::open_read("log.0.0", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);

    // bad file data: empty file
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.0"));
    dsn::utils::copy_file_by_size(fpath, "log.1.0", 0);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.0"));
    lf = log_file::open_read("log.1.0", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_HANDLE_EOF, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.0"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.0.removed"));

    // bad file data: incomplete log_block_header
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.1"));
    dsn::utils::copy_file_by_size(fpath, "log.1.1", sizeof(log_block_header) - 1);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.1"));
    lf = log_file::open_read("log.1.1", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INCOMPLETE_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.1"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.1.removed"));

    // bad file data: bad log_block_header (magic = 0xfeadbeef)
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.2"));
    dsn::utils::copy_file_by_size(fpath, "log.1.2");
    int32_t bad_magic = 0xfeadbeef;
    overwrite_file("log.1.2", FIELD_OFFSET(log_block_header, magic), &bad_magic, sizeof(bad_magic));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.2"));
    lf = log_file::open_read("log.1.2", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.2"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.2.removed"));

    // bad file data: bad log_block_header (crc check failed)
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.3"));
    dsn::utils::copy_file_by_size(fpath, "log.1.3");
    int32_t bad_crc = 0;
    overwrite_file("log.1.3", FIELD_OFFSET(log_block_header, body_crc), &bad_crc, sizeof(bad_crc));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.3"));
    lf = log_file::open_read("log.1.3", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INVALID_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.3"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.3.removed"));

    // bad file data: incomplete block body
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.4"));
    dsn::utils::copy_file_by_size(fpath, "log.1.4", sizeof(log_block_header) + 1);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.4"));
    lf = log_file::open_read("log.1.4", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INCOMPLETE_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.4"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.4.removed"));

    // read the file for test
    offset = 100;
    lf = log_file::open_read(fpath.c_str(), err);
    ASSERT_NE(nullptr, lf);
    EXPECT_EQ(ERR_OK, err);
    ASSERT_EQ(1, lf->index());
    ASSERT_EQ(100, lf->start_offset());
    int64_t sz;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(fpath, dsn::utils::FileDataType::kSensitive, sz));
    ASSERT_EQ(lf->start_offset() + sz, lf->end_offset());

    // read data
    lf->reset_stream();
    for (int i = 0; i < 100; i++) {
        blob bb;
        auto err2 = lf->read_next_log_block(bb);
        ASSERT_EQ(ERR_OK, err2);

        binary_reader reader(bb);

        if (i == 0) {
            lf->read_file_header(reader);
            ASSERT_TRUE(lf->is_right_header());
            ASSERT_EQ(100, lf->header().start_global_offset);
        }

        std::string ss;
        reader.read(ss);
        ASSERT_TRUE(ss == str);

        offset += bb.length() + sizeof(log_block_header);
    }

    ASSERT_TRUE(offset == lf->end_offset());

    blob bb;
    err = lf->read_next_log_block(bb);
    ASSERT_TRUE(err == ERR_HANDLE_EOF);

    lf = nullptr;

    utils::filesystem::remove_path(fpath);
}

namespace dsn {
namespace replication {

class mutation_log_test : public replica_test_base
{
public:
    mutation_log_test() {}

    void SetUp() override
    {
        utils::filesystem::remove_path(_log_dir);
        utils::filesystem::create_directory(_log_dir);
        utils::filesystem::remove_path(_log_dir + ".test");
    }

    void TearDown() override { utils::filesystem::remove_path(_log_dir); }

    mutation_ptr create_test_mutation(decree d, const std::string &data) override
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = d;
        mu->data.header.pid = get_gpid();
        mu->data.header.last_committed_decree = d - 1;
        mu->data.header.log_offset = 0;

        binary_writer writer;
        for (int j = 0; j < 100; j++) {
            writer.write(data);
        }
        mu->data.updates.emplace_back(mutation_update());
        mu->data.updates.back().code = RPC_REPLICATION_WRITE_EMPTY;
        mu->data.updates.back().data = writer.get_buffer();

        mu->client_requests.push_back(nullptr);

        return mu;
    }

    static void ASSERT_BLOB_EQ(const blob &lhs, const blob &rhs)
    {
        ASSERT_EQ(std::string(lhs.data(), lhs.length()), std::string(rhs.data(), rhs.length()));
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

    mutation_log_ptr create_private_log() { return create_private_log(1); }

    mutation_log_ptr create_private_log(int private_log_size_mb, decree replay_start_decree = 0)
    {
        gpid id = get_gpid();
        std::map<gpid, decree> replay_condition;
        replay_condition[id] = replay_start_decree;
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
        EXPECT_NE(mlog, nullptr);
        return mlog;
    }

    void test_replay_single_file(int num_entries)
    {
        std::vector<mutation_ptr> mutations;

        { // writing logs
            mutation_log_ptr mlog = create_private_log();

            for (int i = 0; i < num_entries; i++) {
                mutation_ptr mu = create_test_mutation(2 + i, "hello!");
                mutations.push_back(mu);
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }
            mlog->tracker()->wait_outstanding_tasks();
        }

        { // replaying logs
            std::string log_file_path = _log_dir + "/log.1.0";

            error_code ec;
            log_file_ptr file = log_file::open_read(log_file_path.c_str(), ec);
            ASSERT_EQ(ec, ERR_OK) << ec.to_string();

            int64_t end_offset;
            int mutation_index = -1;
            ec = mutation_log::replay(
                file,
                [&mutations, &mutation_index](int log_length, mutation_ptr &mu) -> bool {
                    mutation_ptr wmu = mutations[++mutation_index];
                    EXPECT_EQ(wmu->data.header, mu->data.header);
                    EXPECT_EQ(wmu->data.updates.size(), mu->data.updates.size());
                    ASSERT_BLOB_EQ(wmu->data.updates[0].data, mu->data.updates[0].data);
                    EXPECT_EQ(wmu->data.updates[0].code, mu->data.updates[0].code);
                    EXPECT_EQ(wmu->client_requests.size(), mu->client_requests.size());
                    return true;
                },
                end_offset);
            ASSERT_EQ(ec, ERR_HANDLE_EOF) << ec.to_string();
        }
    }

    void test_replay_multiple_files(int num_entries, int private_log_file_size_mb)
    {
        std::vector<mutation_ptr> mutations;

        { // writing logs
            mutation_log_ptr mlog = create_private_log(private_log_file_size_mb);
            for (int i = 0; i < num_entries; i++) {
                mutation_ptr mu = create_test_mutation(2 + i, "hello!");
                mutations.push_back(mu);
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }
        }

        { // reading logs
            mutation_log_ptr mlog = create_private_log(private_log_file_size_mb);

            std::vector<std::string> log_files;
            ASSERT_TRUE(utils::filesystem::get_subfiles(mlog->dir(), log_files, false));

            int64_t end_offset;
            int mutation_index = -1;
            mutation_log::replay(
                log_files,
                [&mutations, &mutation_index](int log_length, mutation_ptr &mu) -> bool {
                    mutation_ptr wmu = mutations[++mutation_index];
                    EXPECT_EQ(wmu->data.header, mu->data.header);
                    EXPECT_EQ(wmu->data.updates.size(), mu->data.updates.size());
                    ASSERT_BLOB_EQ(wmu->data.updates[0].data, mu->data.updates[0].data);
                    EXPECT_EQ(wmu->data.updates[0].code, mu->data.updates[0].code);
                    EXPECT_EQ(wmu->client_requests.size(), mu->client_requests.size());
                    return true;
                },
                end_offset);
            ASSERT_EQ(mutation_index + 1, (int)mutations.size());

            ASSERT_GE(log_files.size(), 1);
        }
    }

    mutation_ptr generate_slog_mutation(const gpid &pid, const decree d, const std::string &data)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = d;
        mu->data.header.pid = pid;
        mu->data.header.last_committed_decree = d - 1;
        mu->data.header.log_offset = 0;
        mu->data.header.timestamp = d;

        mu->data.updates.push_back(mutation_update());
        mu->data.updates.back().code = dsn::apps::RPC_RRDB_RRDB_PUT;
        mu->data.updates.back().data = blob::create_from_bytes(std::string(data));

        mu->client_requests.push_back(nullptr);

        return mu;
    }

    void generate_slog_file(const std::vector<std::pair<gpid, size_t>> &replica_mutations,
                            mutation_log_ptr &mlog,
                            decree &d,
                            std::unordered_map<gpid, int64_t> &valid_start_offsets,
                            std::pair<gpid, int64_t> &slog_file_start_offset)
    {
        for (size_t i = 0; i < replica_mutations.size(); ++i) {
            const auto &pid = replica_mutations[i].first;

            for (size_t j = 0; j < replica_mutations[i].second; ++j) {
                if (i == 0) {
                    // Record the start offset of each slog file.
                    slog_file_start_offset.first = pid;
                    slog_file_start_offset.second = mlog->get_global_offset();
                }

                const auto &it = valid_start_offsets.find(pid);
                if (it == valid_start_offsets.end()) {
                    // Add new partition with its start offset in slog.
                    valid_start_offsets.emplace(pid, mlog->get_global_offset());
                    mlog->set_valid_start_offset_on_open(pid, mlog->get_global_offset());
                }

                // Append a mutation.
                auto mu = generate_slog_mutation(pid, d++, "test data");
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, mlog->tracker(), nullptr, 0);
            }
        }

        // Wait until all mutations are written into this file.
        mlog->tracker()->wait_outstanding_tasks();
    }

    void generate_slog_files(const std::vector<std::vector<std::pair<gpid, size_t>>> &files,
                             mutation_log_ptr &mlog,
                             std::unordered_map<gpid, int64_t> &valid_start_offsets,
                             std::vector<std::pair<gpid, int64_t>> &slog_file_start_offsets)
    {
        valid_start_offsets.clear();
        slog_file_start_offsets.resize(files.size());

        decree d = 1;
        for (size_t i = 0; i < files.size(); ++i) {
            generate_slog_file(files[i], mlog, d, valid_start_offsets, slog_file_start_offsets[i]);
            if (i + 1 < files.size()) {
                // Do not create a new slog file after the last file is generated.
                mlog->create_new_log_file();
                // Wait until file header is written.
                mlog->tracker()->wait_outstanding_tasks();
            }
        }

        // Close and reset `_current_log_file` since slog has been deprecated and would not be
        // used again.
        mlog->_current_log_file->close();
        mlog->_current_log_file = nullptr;
    }
};

INSTANTIATE_TEST_CASE_P(, mutation_log_test, ::testing::Values(false, true));

TEST_P(mutation_log_test, replay_single_file_1000) { test_replay_single_file(1000); }

TEST_P(mutation_log_test, replay_single_file_2000) { test_replay_single_file(2000); }

TEST_P(mutation_log_test, replay_single_file_5000) { test_replay_single_file(5000); }

TEST_P(mutation_log_test, replay_single_file_10000) { test_replay_single_file(10000); }

TEST_P(mutation_log_test, replay_single_file_1) { test_replay_single_file(1); }

TEST_P(mutation_log_test, replay_single_file_10) { test_replay_single_file(10); }

// mutation_log::open
TEST_P(mutation_log_test, open)
{
    std::vector<mutation_ptr> mutations;

    { // writing logs
        mutation_log_ptr mlog = create_private_log(4);

        for (int i = 0; i < 1000; i++) {
            mutation_ptr mu = create_test_mutation(2 + i, "hello!");
            mutations.push_back(mu);
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        }
    }

    { // reading logs
        mutation_log_ptr mlog = new mutation_log_private(_log_dir, 4, get_gpid(), _replica.get());

        int mutation_index = -1;
        mlog->open(
            [&mutations, &mutation_index](int log_length, mutation_ptr &mu) -> bool {
                mutation_ptr wmu = mutations[++mutation_index];
                EXPECT_EQ(wmu->data.header, mu->data.header);
                EXPECT_EQ(wmu->data.updates.size(), mu->data.updates.size());
                ASSERT_BLOB_EQ(wmu->data.updates[0].data, mu->data.updates[0].data);
                EXPECT_EQ(wmu->data.updates[0].code, mu->data.updates[0].code);
                EXPECT_EQ(wmu->client_requests.size(), mu->client_requests.size());
                return true;
            },
            nullptr);
        ASSERT_EQ(mutation_index + 1, (int)mutations.size());
    }
}

TEST_P(mutation_log_test, replay_multiple_files_10000_1mb) { test_replay_multiple_files(10000, 1); }

TEST_P(mutation_log_test, replay_multiple_files_20000_1mb) { test_replay_multiple_files(20000, 1); }

TEST_P(mutation_log_test, replay_multiple_files_50000_1mb) { test_replay_multiple_files(50000, 1); }

TEST_P(mutation_log_test, replay_start_decree)
{
    // decree ranges from [1, 30)
    generate_multiple_log_files(3);

    decree replay_start_decree = 11; // start replay from second file, the first file is ignored.
    mutation_log_ptr mlog = create_private_log(1, replay_start_decree);

    // ensure the first file is not stripped out.
    ASSERT_EQ(mlog->max_gced_decree(get_gpid()), 0);
    ASSERT_EQ(mlog->get_log_file_map().size(), 3);
}

TEST_P(mutation_log_test, reset_from)
{
    std::vector<mutation_ptr> expected;
    { // writing logs
        mutation_log_ptr mlog = new mutation_log_private(_log_dir, 4, get_gpid(), _replica.get());

        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        for (int i = 0; i < 10; i++) {
            mutation_ptr mu = create_test_mutation(2 + i, "hello!");
            expected.push_back(mu);
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        }
        mlog->flush();

        ASSERT_TRUE(utils::filesystem::rename_path(_log_dir, _log_dir + ".tmp"));
    }

    ASSERT_TRUE(utils::filesystem::directory_exists(_log_dir + ".tmp"));
    ASSERT_FALSE(utils::filesystem::directory_exists(_log_dir));

    // create another set of logs
    mutation_log_ptr mlog = new mutation_log_private(_log_dir, 4, get_gpid(), _replica.get());
    EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);
    for (int i = 0; i < 1000; i++) {
        mutation_ptr mu = create_test_mutation(2000 + i, "hello!");
        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }
    mlog->flush();

    // reset from the tmp log dir.
    std::vector<mutation_ptr> actual;
    auto err = mlog->reset_from(_log_dir + ".tmp",
                                [&](int, mutation_ptr &mu) -> bool {
                                    actual.push_back(mu);
                                    return true;
                                },
                                [](error_code err) { ASSERT_EQ(err, ERR_OK); });
    ASSERT_EQ(err, ERR_OK);
    ASSERT_EQ(actual.size(), expected.size());

    // the tmp dir has been removed.
    ASSERT_FALSE(utils::filesystem::directory_exists(_log_dir + ".tmp"));
    ASSERT_TRUE(utils::filesystem::directory_exists(_log_dir));
}

// multi-threaded testing. ensure reset_from will wait until
// all previous writes complete.
TEST_P(mutation_log_test, reset_from_while_writing)
{
    std::vector<mutation_ptr> expected;
    { // writing logs
        mutation_log_ptr mlog = new mutation_log_private(_log_dir, 4, get_gpid(), _replica.get());
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        for (int i = 0; i < 10; i++) {
            mutation_ptr mu = create_test_mutation(2 + i, "hello!");
            expected.push_back(mu);
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        }
        mlog->flush();

        ASSERT_TRUE(utils::filesystem::rename_path(_log_dir, _log_dir + ".test"));
    }

    // create another set of logs
    mutation_log_ptr mlog = new mutation_log_private(_log_dir, 4, get_gpid(), _replica.get());
    EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

    // given with a large number of mutation to ensure
    // plog::reset_from will face many uncompleted writes.
    for (int i = 0; i < 1000 * 100; i++) {
        mutation_ptr mu = create_test_mutation(2000 + i, "hello!");
        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, mlog->tracker(), nullptr, 0);
    }

    // reset from the tmp log dir.
    std::vector<mutation_ptr> actual;
    auto err = mlog->reset_from(_log_dir + ".test",
                                [&](int, mutation_ptr &mu) -> bool {
                                    actual.push_back(mu);
                                    return true;
                                },
                                [](error_code err) { ASSERT_EQ(err, ERR_OK); });
    ASSERT_EQ(err, ERR_OK);

    mlog->flush();
    ASSERT_EQ(actual.size(), expected.size());
}

TEST_P(mutation_log_test, gc_slog)
{
    // Remove the slog dir and create a new one.
    const std::string slog_dir("./slog_test");
    ASSERT_TRUE(dsn::utils::filesystem::remove_path(slog_dir));
    ASSERT_TRUE(dsn::utils::filesystem::create_directory(slog_dir));

    // Create and open slog object, which would be closed at the end of the scope.
    mutation_log_ptr mlog = new mutation_log_shared(slog_dir, 1, false);
    auto cleanup = dsn::defer([mlog]() { mlog->close(); });
    ASSERT_EQ(ERR_OK, mlog->open(nullptr, nullptr));

    // Each line describes a sequence of mutations written to specified replicas by
    // specified numbers.
    //
    // From these sequences the decrees for each partition could be concluded as below:
    // {1, 1}: 9 ~ 15
    // {1, 2}: 16 ~ 22
    // {2, 5}: 1 ~ 8, 23 ~ 38
    // {2, 7}: 39 ~ 46
    // {5, 6}: 47 ~ 73
    const std::vector<std::vector<std::pair<gpid, size_t>>> files = {
        {{{2, 5}, 8}, {{1, 1}, 7}, {{1, 2}, 2}},
        {{{1, 2}, 5}},
        {{{2, 5}, 16}, {{2, 7}, 8}, {{5, 6}, 27}}};

    // Each line describes a progress of durable decrees for all of replicas: decrees are
    // continuously being applied and becoming durable.
    const std::vector<std::unordered_map<gpid, decree>> durable_decrees = {
        {{{1, 1}, 10}, {{1, 2}, 17}, {{2, 5}, 6}, {{2, 7}, 39}, {{5, 6}, 47}},
        {{{1, 1}, 15}, {{1, 2}, 18}, {{2, 5}, 7}, {{2, 7}, 40}, {{5, 6}, 57}},
        {{{1, 1}, 15}, {{1, 2}, 20}, {{2, 5}, 8}, {{2, 7}, 42}, {{5, 6}, 61}},
        {{{1, 1}, 15}, {{1, 2}, 22}, {{2, 5}, 23}, {{2, 7}, 44}, {{5, 6}, 65}},
        {{{1, 1}, 15}, {{1, 2}, 22}, {{2, 5}, 27}, {{2, 7}, 46}, {{5, 6}, 66}},
        {{{1, 1}, 15}, {{1, 2}, 22}, {{2, 5}, 32}, {{2, 7}, 46}, {{5, 6}, 67}},
        {{{1, 1}, 15}, {{1, 2}, 22}, {{2, 5}, 38}, {{2, 7}, 46}, {{5, 6}, 72}},
        {{{1, 1}, 15}, {{1, 2}, 22}, {{2, 5}, 38}, {{2, 7}, 46}, {{5, 6}, 73}},
    };
    const std::vector<size_t> remaining_slog_files = {3, 3, 2, 1, 1, 1, 1, 0};
    const std::vector<std::set<gpid>> expected_prevent_gc_replicas = {
        {{1, 1}, {1, 2}, {2, 5}, {2, 7}, {5, 6}},
        {{1, 2}, {2, 5}, {2, 7}, {5, 6}},
        {{1, 2}, {2, 5}, {2, 7}, {5, 6}},
        {{2, 5}, {2, 7}, {5, 6}},
        {{2, 5}, {5, 6}},
        {{2, 5}, {5, 6}},
        {{5, 6}},
        {},
    };

    // Each line describes an action, that during a round (related to the index of
    // `durable_decrees`), which replica should be reset to the start offset of an
    // slog file (related to the index of `files` and `slog_file_start_offsets`).
    const std::unordered_map<size_t, size_t> set_to_slog_file_start_offsets = {
        {2, 1},
    };

    // Create slog files and write some data into them according to test cases.
    std::unordered_map<gpid, int64_t> valid_start_offsets;
    std::vector<std::pair<gpid, int64_t>> slog_file_start_offsets;
    generate_slog_files(files, mlog, valid_start_offsets, slog_file_start_offsets);

    for (size_t i = 0; i < durable_decrees.size(); ++i) {
        std::cout << "Update No." << i << " group of durable decrees" << std::endl;

        // Update the progress of durable_decrees for each partition.
        replica_log_info_map replica_durable_decrees;
        for (const auto &d : durable_decrees[i]) {
            replica_durable_decrees.emplace(
                d.first, replica_log_info(d.second, valid_start_offsets[d.first]));
        }

        // Test condition for `valid_start_offset`, see `can_gc_replica_slog`.
        const auto &set_to_start = set_to_slog_file_start_offsets.find(i);
        if (set_to_start != set_to_slog_file_start_offsets.end()) {
            const auto &start_offset = slog_file_start_offsets[set_to_start->second];
            replica_durable_decrees[start_offset.first].valid_start_offset = start_offset.second;
        }

        // Run garbage collection for a round.
        std::set<gpid> actual_prevent_gc_replicas;
        mlog->garbage_collection(replica_durable_decrees, actual_prevent_gc_replicas);

        // Check if the number of remaining slog files after garbage collection is desired.
        std::vector<std::string> file_list;
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(slog_dir, file_list, false));
        ASSERT_EQ(remaining_slog_files[i], file_list.size());

        // Check if the replicas that prevent garbage collection (i.e. cannot be removed by
        // garbage collection) is expected.
        ASSERT_EQ(expected_prevent_gc_replicas[i], actual_prevent_gc_replicas);
    }
}

using gc_slog_flush_replicas_case = std::tuple<std::set<gpid>, uint64_t, size_t, size_t, size_t>;

class GcSlogFlushFeplicasTest : public testing::TestWithParam<gc_slog_flush_replicas_case>
{
};

DSN_DECLARE_uint64(log_shared_gc_flush_replicas_limit);

TEST_P(GcSlogFlushFeplicasTest, FlushReplicas)
{
    std::set<gpid> prevent_gc_replicas;
    size_t last_prevent_gc_replica_count;
    uint64_t limit;
    size_t last_limit;
    size_t expected_flush_replicas;
    std::tie(prevent_gc_replicas,
             last_prevent_gc_replica_count,
             limit,
             last_limit,
             expected_flush_replicas) = GetParam();

    replica_stub::replica_gc_info_map replica_gc_map;
    for (const auto &r : prevent_gc_replicas) {
        replica_gc_map.emplace(r, replica_stub::replica_gc_info());
    }

    const auto reserved_log_shared_gc_flush_replicas_limit =
        FLAGS_log_shared_gc_flush_replicas_limit;
    FLAGS_log_shared_gc_flush_replicas_limit = limit;

    dsn::fail::setup();
    dsn::fail::cfg("mock_flush_replicas_for_slog_gc", "void(true)");

    replica_stub stub;
    stub._last_prevent_gc_replica_count = last_prevent_gc_replica_count;
    stub._real_log_shared_gc_flush_replicas_limit = last_limit;

    stub.flush_replicas_for_slog_gc(replica_gc_map, prevent_gc_replicas);
    EXPECT_EQ(expected_flush_replicas, stub._mock_flush_replicas_for_test);

    dsn::fail::teardown();

    FLAGS_log_shared_gc_flush_replicas_limit = reserved_log_shared_gc_flush_replicas_limit;
}

const std::vector<gc_slog_flush_replicas_case> gc_slog_flush_replicas_tests = {
    // Initially, there is no limit on flushed replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 0, 0, 0, 6},
    // Initially, there is no limit on flushed replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 1, 0, 5, 6},
    // Initially, limit is less than the number of replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 0, 1, 0, 1},
    // Initially, limit is less than the number of replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 0, 2, 0, 2},
    // Initially, limit is just equal to the number of replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 0, 6, 0, 6},
    // Initially, limit is more than the number of replicas.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 0, 7, 0, 6},
    // No replica has been flushed during previous round.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 6, 6, 6, 2},
    // No replica has been flushed during previous round.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 6, 1, 2, 1},
    // The previous limit is 0.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 7, 5, 0, 5},
    // The previous limit is infinite.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 7, 5, std::numeric_limits<size_t>::max(), 5},
    // The number of previously flushed replicas is less than the previous limit.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 7, 5, 0, 5},
    // The number of previously flushed replicas reaches the previous limit.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 8, 6, 2, 4},
    // The number of previously flushed replicas reaches the previous limit.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 12, 6, 6, 6},
    // The number of previously flushed replicas is more than the previous limit.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 9, 3, 2, 3},
    // The number of previously flushed replicas is more than the previous limit.
    {{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}}, 9, 5, 2, 4},
};

INSTANTIATE_TEST_CASE_P(MutationLogTest,
                        GcSlogFlushFeplicasTest,
                        testing::ValuesIn(gc_slog_flush_replicas_tests));

} // namespace replication
} // namespace dsn
