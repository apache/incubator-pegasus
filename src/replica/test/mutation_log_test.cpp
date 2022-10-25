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
#include "replica_test_base.h"

#include "utils/filesystem.h"
#include <gtest/gtest.h>

using namespace ::dsn;
using namespace ::dsn::replication;

static void copy_file(const char *from_file, const char *to_file, int64_t to_size = -1)
{
    int64_t from_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(from_file, from_size));
    ASSERT_LE(to_size, from_size);
    FILE *from = fopen(from_file, "rb");
    ASSERT_TRUE(from != nullptr);
    FILE *to = fopen(to_file, "wb");
    ASSERT_TRUE(to != nullptr);
    if (to_size == -1)
        to_size = from_size;
    if (to_size > 0) {
        std::unique_ptr<char[]> buf(new char[to_size]);
        auto n = fread(buf.get(), 1, to_size, from);
        ASSERT_EQ(to_size, n);
        n = fwrite(buf.get(), 1, to_size, to);
        ASSERT_EQ(to_size, n);
    }
    int r = fclose(from);
    ASSERT_EQ(0, r);
    r = fclose(to);
    ASSERT_EQ(0, r);
}

static void overwrite_file(const char *file, int offset, const void *buf, int size)
{
    FILE *f = fopen(file, "r+b");
    ASSERT_TRUE(f != nullptr);
    int r = fseek(f, offset, SEEK_SET);
    ASSERT_EQ(0, r);
    size_t n = fwrite(buf, 1, size, f);
    ASSERT_EQ(size, n);
    r = fclose(f);
    ASSERT_EQ(0, r);
}

TEST(replication, log_file)
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
            log_file_header &h = lf->header();
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
    copy_file(fpath.c_str(), "log.1.0", 0);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.0"));
    lf = log_file::open_read("log.1.0", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_HANDLE_EOF, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.0"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.0.removed"));

    // bad file data: incomplete log_block_header
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.1"));
    copy_file(fpath.c_str(), "log.1.1", sizeof(log_block_header) - 1);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.1"));
    lf = log_file::open_read("log.1.1", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INCOMPLETE_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.1"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.1.removed"));

    // bad file data: bad log_block_header (magic = 0xfeadbeef)
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.2"));
    copy_file(fpath.c_str(), "log.1.2");
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
    copy_file(fpath.c_str(), "log.1.3");
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
    copy_file(fpath.c_str(), "log.1.4", sizeof(log_block_header) + 1);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.4"));
    lf = log_file::open_read("log.1.4", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_INCOMPLETE_DATA, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.4"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.4.removed"));
    ASSERT_TRUE(dsn::utils::filesystem::rename_path("log.1.4.removed", "log.1.4"));

    // read the file for test
    offset = 100;
    lf = log_file::open_read(fpath.c_str(), err);
    ASSERT_NE(nullptr, lf);
    EXPECT_EQ(ERR_OK, err);
    ASSERT_EQ(1, lf->index());
    ASSERT_EQ(100, lf->start_offset());
    int64_t sz;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(fpath, sz));
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
            LOG_ERROR_F("mlog open failed, encountered error: {}", err);
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
};

TEST_F(mutation_log_test, replay_single_file_1000) { test_replay_single_file(1000); }

TEST_F(mutation_log_test, replay_single_file_2000) { test_replay_single_file(2000); }

TEST_F(mutation_log_test, replay_single_file_5000) { test_replay_single_file(5000); }

TEST_F(mutation_log_test, replay_single_file_10000) { test_replay_single_file(10000); }

TEST_F(mutation_log_test, replay_single_file_1) { test_replay_single_file(1); }

TEST_F(mutation_log_test, replay_single_file_10) { test_replay_single_file(10); }

// mutation_log::open
TEST_F(mutation_log_test, open)
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

TEST_F(mutation_log_test, replay_multiple_files_10000_1mb) { test_replay_multiple_files(10000, 1); }

TEST_F(mutation_log_test, replay_multiple_files_20000_1mb) { test_replay_multiple_files(20000, 1); }

TEST_F(mutation_log_test, replay_multiple_files_50000_1mb) { test_replay_multiple_files(50000, 1); }

TEST_F(mutation_log_test, replay_start_decree)
{
    // decree ranges from [1, 30)
    generate_multiple_log_files(3);

    decree replay_start_decree = 11; // start replay from second file, the first file is ignored.
    mutation_log_ptr mlog = create_private_log(1, replay_start_decree);

    // ensure the first file is not stripped out.
    ASSERT_EQ(mlog->max_gced_decree(get_gpid()), 0);
    ASSERT_EQ(mlog->get_log_file_map().size(), 3);
}

TEST_F(mutation_log_test, reset_from)
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
TEST_F(mutation_log_test, reset_from_while_writing)
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
} // namespace replication
} // namespace dsn
