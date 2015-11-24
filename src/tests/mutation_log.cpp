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

using namespace ::dsn;
using namespace ::dsn::replication;

TEST(replication, log_file)
{
    multi_partition_decrees_ex mdecrees;
    global_partition_id gpid = { 1, 0 };
    mdecrees[gpid] = log_replica_info(3, 0);
    std::string fpath = "./log.1.100";
    int index = 1;
    int64_t offset = 100;
    std::string str = "hello, world!";
    error_code err;

    // write log
    log_file_ptr lf = nullptr;

    lf = log_file::create_write(
        ".",
        1,
        0
        );
    ASSERT_TRUE(lf == nullptr); // already exist

    utils::filesystem::remove_path(fpath);
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

    // write data
    for (int i = 0; i < 100; i++)
    {
        auto writer = lf->prepare_log_entry();

        if (i == 0)
        {
            lf->write_header(
                *writer,
                mdecrees,
                1024
                );
            ASSERT_EQ(mdecrees, lf->previous_log_max_decrees());
            log_file_header& h = lf->header();
            ASSERT_EQ(1024, h.log_buffer_size_bytes);
            ASSERT_EQ(100, h.start_global_offset);
        }

        writer->write(str);

        auto bb = writer->get_buffer();
        ASSERT_EQ(writer->total_size(), bb.length());
        auto task = lf->commit_log_entry(
            bb, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0
            );
        task->wait();
        ASSERT_EQ(ERR_OK, task->error());
        ASSERT_EQ(bb.length(), task->io_size());

        lf->flush();
        offset += bb.length();
    }

    lf->close();
    lf = nullptr;

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

    // bad file data: crc checking failed
    lf = log_file::open_read("log.1.0", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.0"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.0.removed"));
    ASSERT_TRUE(dsn::utils::filesystem::rename_path("log.1.0.removed", "log.1.0"));

    // bad file data: bad log_file_header (magic = 0xfeadbeef)
    lf = log_file::open_read("log.1.1", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.1"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.1.removed"));
    ASSERT_TRUE(dsn::utils::filesystem::rename_path("log.1.1.removed", "log.1.1"));

    // bad file data: less than log_block_header
    lf = log_file::open_read("log.1.2", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.2"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.2.removed"));
    ASSERT_TRUE(dsn::utils::filesystem::rename_path("log.1.2.removed", "log.1.2"));

    // bad file data: bad log_block_header
    lf = log_file::open_read("log.1.3", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);
    ASSERT_TRUE(!dsn::utils::filesystem::file_exists("log.1.3"));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists("log.1.3.removed"));
    ASSERT_TRUE(dsn::utils::filesystem::rename_path("log.1.3.removed", "log.1.3"));

    // bad file data: truncated block body
    lf = log_file::open_read("log.1.4", err);
    ASSERT_TRUE(lf == nullptr);
    ASSERT_EQ(ERR_FILE_OPERATION_FAILED, err);
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
    for (int i = 0; i < 100; i++)
    {
        blob bb;
        auto err = lf->read_next_log_entry(offset - lf->start_offset(), bb);
        ASSERT_TRUE(err == ERR_OK);
        
        binary_reader reader(bb);

        if (i == 0)
        {
            lf->read_header(reader);
            ASSERT_TRUE(lf->is_right_header());
            ASSERT_EQ(1024, lf->header().log_buffer_size_bytes);
            ASSERT_EQ(100, lf->header().start_global_offset);
        }

        std::string ss;
        reader.read(ss);
        ASSERT_TRUE(ss == str);

        offset += bb.length() + sizeof(log_block_header);
    }

    ASSERT_TRUE(offset == lf->end_offset());

    blob bb;
    err = lf->read_next_log_entry(offset - lf->start_offset(), bb);
    ASSERT_TRUE(err == ERR_HANDLE_EOF);

    lf = nullptr;

    utils::filesystem::remove_path(fpath);
}

TEST(replication, mutation_log)
{
    global_partition_id gpid = { 1, 0 };
    std::string str = "hello, world!";
    std::string logp = "./test-log";
    std::vector<mutation_ptr> mutations;
    
    // prepare
    utils::filesystem::remove_path(logp);
    utils::filesystem::create_directory(logp);

    // writing logs
    mutation_log_ptr mlog = new mutation_log(
        logp,
        true,
        1,
        4
        );

    auto err = mlog->open(gpid, nullptr);
    EXPECT_TRUE(err == ERR_OK);

    for (int i = 0; i < 1000; i++)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = 2 + i;
        mu->data.header.gpid = gpid;
        mu->data.header.last_committed_decree = i;
        mu->data.header.log_offset = 0;

        binary_writer writer;
        for (int j = 0; j < 100; j++)
        {   
            writer.write(str);
        }
        mu->data.updates.push_back(writer.get_buffer());

        mutation::client_info ci;
        ci.code = 100;
        ci.req = nullptr;
        mu->client_requests.push_back(ci);

        mutations.push_back(mu);

        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }

    mlog->close(); 

    // reading logs
    mlog = new mutation_log(
        logp,
        true,
        1,
        4
        );

    int mutation_index = -1;
    mlog->open(gpid,
        [&mutations, &mutation_index](mutation_ptr& mu)->bool
        {
            mutation_ptr wmu = mutations[++mutation_index];
            EXPECT_TRUE(memcmp((const void*)&wmu->data.header,
                (const void*)&mu->data.header,
                sizeof(mu->data.header)) == 0
                );
            EXPECT_TRUE(wmu->data.updates.size() == mu->data.updates.size());
            EXPECT_TRUE(wmu->data.updates[0].length() == mu->data.updates[0].length());
            EXPECT_TRUE(memcmp((const void*)wmu->data.updates[0].data(),
                (const void*)mu->data.updates[0].data(),
                mu->data.updates[0].length()) == 0
                );
            EXPECT_TRUE(wmu->client_requests.size() == mu->client_requests.size());
            EXPECT_TRUE(wmu->client_requests[0].code == mu->client_requests[0].code);
            return true;
        }
        );
    EXPECT_TRUE(mutation_index + 1 == (int)mutations.size());
    mlog->close();

    // clear all
    utils::filesystem::remove_path(logp);
}
