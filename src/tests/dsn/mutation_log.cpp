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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
# include "mutation_log.h"
# include <gtest/gtest.h>
# include <cstdio>

using namespace ::dsn;
using namespace ::dsn::replication;

static void copy_file(const char* from_file, const char* to_file, int64_t to_size = -1)
{
    int64_t from_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(from_file, from_size));
    ASSERT_LE(to_size, from_size);
    FILE* from = fopen(from_file, "rb");
    ASSERT_TRUE(from != nullptr);
    FILE* to = fopen(to_file, "wb");
    ASSERT_TRUE(to != nullptr);
    if (to_size == -1)
        to_size = from_size;
    if (to_size > 0)
    {
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

static void overwrite_file(const char* file, int offset, const void* buf, int size)
{
    FILE* f = fopen(file, "r+b");
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
    lf = log_file::create_write(
        ".",
        index,
        offset
        );
    ASSERT_TRUE(lf != nullptr);
    ASSERT_EQ(fpath, lf->path());
    ASSERT_EQ(index, lf->index());
    ASSERT_EQ(offset, lf->start_offset());
    ASSERT_EQ(offset, lf->end_offset());
    for (int i = 0; i < 100; i++)
    {
        auto writer = lf->prepare_log_block();

        if (i == 0)
        {
            binary_writer temp_writer;
            lf->write_file_header(
                temp_writer,
                mdecrees
                );
            writer->add(temp_writer.get_buffer());
            ASSERT_EQ(mdecrees, lf->previous_log_max_decrees());
            log_file_header& h = lf->header();
            ASSERT_EQ(100, h.start_global_offset);
        }

        binary_writer temp_writer;
        temp_writer.write(str);
        writer->add(temp_writer.get_buffer());

        task_ptr task = lf->commit_log_block(
            *writer, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0
            );
        task->wait();
        ASSERT_EQ(ERR_OK, task->error());
        ASSERT_EQ(writer->size(), task->io_size());

        lf->flush();
        offset += writer->size();

        delete writer;
    }
    lf->close();
    lf = nullptr;
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(fpath));

    // file already exist
    offset = 100;
    lf = log_file::create_write(
        ".",
        index,
        offset
        );
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
    for (int i = 0; i < 100; i++)
    {
        blob bb;
        auto err = lf->read_next_log_block(bb);
        ASSERT_EQ(ERR_OK, err);

        binary_reader reader(bb);

        if (i == 0)
        {
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

TEST(replication, mutation_log)
{
    gpid gpid(1, 0);
    std::string str = "hello, world!";
    std::string logp = "./test-log";
    std::vector<mutation_ptr> mutations;

    // prepare
    utils::filesystem::remove_path(logp);
    utils::filesystem::create_directory(logp);

    // writing logs
    mutation_log_ptr mlog = new mutation_log_private(
        logp,
        4,
        gpid,
        nullptr,
        1024,
        512
        );

    auto err = mlog->open(nullptr, nullptr);
    EXPECT_EQ(err, ERR_OK);

    for (int i = 0; i < 1000; i++)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = 2 + i;
        mu->data.header.pid = gpid;
        mu->data.header.last_committed_decree = i;
        mu->data.header.log_offset = 0;

        binary_writer writer;
        for (int j = 0; j < 100; j++)
        {
            writer.write(str);
        }
        mu->data.updates.push_back(mutation_update());
        mu->data.updates.back().code = RPC_REPLICATION_WRITE_EMPTY;
        mu->data.updates.back().data = writer.get_buffer();

        mu->client_requests.push_back(nullptr);

        mutations.push_back(mu);

        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }

    mlog->close();

    // reading logs
    mlog = new mutation_log_private(
        logp,
        4,
        gpid,
        nullptr,
        1024,
        512
        );

    int mutation_index = -1;
    mlog->open(
        [&mutations, &mutation_index](mutation_ptr& mu)->bool
    {
        mutation_ptr wmu = mutations[++mutation_index];
#ifdef DSN_USE_THRIFT_SERIALIZATION
        EXPECT_TRUE(wmu->data.header == mu->data.header);
#else
        EXPECT_TRUE(memcmp((const void*)&wmu->data.header,
            (const void*)&mu->data.header,
            sizeof(mu->data.header)) == 0
            );
#endif
        EXPECT_TRUE(wmu->data.updates.size() == mu->data.updates.size());
        EXPECT_TRUE(wmu->data.updates[0].data.length() == mu->data.updates[0].data.length());
        EXPECT_TRUE(memcmp((const void*)wmu->data.updates[0].data.data(),
            (const void*)mu->data.updates[0].data.data(),
            mu->data.updates[0].data.length()) == 0
            );
        EXPECT_TRUE(wmu->data.updates[0].code == mu->data.updates[0].code);
        EXPECT_TRUE(wmu->client_requests.size() == mu->client_requests.size());
        return true;
    }, nullptr
    );
    EXPECT_TRUE(mutation_index + 1 == (int)mutations.size());
    mlog->close();

    // clear all
    utils::filesystem::remove_path(logp);
}
