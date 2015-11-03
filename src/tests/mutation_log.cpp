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
    std::string fpath = ".//log.1.100";
    int64_t offset = 100;
    std::string str = "hello, world!";
    error_code err;

    // prepare
    mdecrees[gpid] = log_replica_info(3, 0);
    utils::filesystem::remove_path(fpath);

    // write log
    log_file_ptr lf = log_file::create_write(
        "./",
        1,
        offset
        );

    // write header + data block
    {
        auto writer = lf->prepare_log_entry();

        lf->write_header(
            *writer,
            mdecrees,
            1024
            );

        writer->write(str);

        auto bb = writer->get_buffer();
        auto task = lf->commit_log_entry(
            bb, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0
            );
        task->wait();

        offset += writer->total_size();
    }
    
    // writer second data block
    {
        auto writer = lf->prepare_log_entry();

        writer->write(str);

        auto bb = writer->get_buffer();
        auto task = lf->commit_log_entry(
            bb, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0
            );
        task->wait();

        offset += writer->total_size();
    }

    lf = nullptr;

    // read the file for test
    offset = 100;
    lf = log_file::open_read(fpath.c_str(), err);
    EXPECT_TRUE(err == ERR_OK);
    EXPECT_TRUE(lf != nullptr);
    EXPECT_TRUE(lf->index() == 1);
    EXPECT_TRUE(lf->start_offset() == 100);
    EXPECT_TRUE(lf->end_offset() > lf->start_offset());

    // read the first block
    {
        blob bb;
        auto err = lf->read_next_log_entry(offset - lf->start_offset(), bb);
        EXPECT_TRUE(err == ERR_OK);
        
        binary_reader reader(bb);
        lf->read_header(reader);

        EXPECT_TRUE(lf->is_right_header());
        EXPECT_TRUE(lf->header().log_buffer_size_bytes == 1024);

        std::string ss;
        reader.read(ss);
        EXPECT_TRUE(ss == str);

        offset += bb.length() + sizeof(log_block_header);
    }

    // read the second block
    {
        blob bb;
        auto err = lf->read_next_log_entry(offset - lf->start_offset(), bb);
        EXPECT_TRUE(err == ERR_OK);

        binary_reader reader(bb);
        std::string ss;
        reader.read(ss);
        EXPECT_TRUE(ss == str);

        offset += bb.length() + sizeof(log_block_header);
    }

    EXPECT_TRUE(offset == lf->end_offset());
    lf = nullptr;

    utils::filesystem::remove_path(fpath);
}

TEST(replication, mutation_log)
{
    multi_partition_decrees mdecrees;
    std::string str = "hello, world!";
    std::string logp = "./test-log";
    std::vector<mutation_ptr> mutations;
    
    // prepare
    utils::filesystem::remove_path(logp);
    utils::filesystem::create_directory(logp);

    // writing logs
    mutation_log_ptr mlog = new mutation_log(
        logp,
        false,
        1,
        4
        );

    auto err = mlog->open(nullptr);
    EXPECT_TRUE(err == ERR_OK);

    for (int i = 0; i < 1000; i++)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = 2 + i;
        mu->data.header.gpid = { 1, 0 };
        mu->data.header.last_committed_decree = i;
        mu->data.header.log_offset = 0;

        binary_writer writer;
        for (int j = 0; j < 100; j++)
        {   
            writer.write(str);
        }
        mu->data.updates.push_back(writer.get_buffer());

        mutations.push_back(mu);

        mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
    }

    mlog->close(); 

    // reading logs
    mlog = new mutation_log(
        logp,
        false,
        1,
        4
        );

    int mutation_index = -1;
    mlog->open(        
        [&mutations, &mutation_index](mutation_ptr mu)
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
            return true;
        }
        );
    EXPECT_TRUE(mutation_index + 1 == (int)mutations.size());
    mlog->close();

    // clear all
    utils::filesystem::remove_path(logp);
}
