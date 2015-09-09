# include "mutation_log.h"
# include <gtest/gtest.h>

using namespace ::dsn::replication;

TEST(replication, log_file)
{
    multi_partition_decrees mdecrees;
    global_partition_id gpid = { 1, 0 };    
    std::string fpath = ".//log.1.100";
    int64_t offset = 100;
    std::string str = "hello, world!";

    // prepare
    mdecrees[gpid] = 3;
    utils::filesystem::remove_path(fpath);

    // write log
    log_file_ptr lf = log_file::create_write(
        "./",
        1,
        offset,
        10
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
    lf = log_file::open_read(fpath.c_str());
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
        EXPECT_TRUE(lf->header().max_staleness_for_commit == 10);
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
    mutation_log* mlog = new mutation_log(
        1,
        50,
        4,
        true
        );

    mlog->initialize(logp.c_str());
    mlog->start_write_service(
        mdecrees,
        10
        );

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
    delete mlog;    

    // reading logs
    mlog = new mutation_log(
        1,
        50,
        4,
        true
        );

    mlog->initialize(logp.c_str());

    int mutation_index = -1;
    mlog->replay(        
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
        }
        );
    EXPECT_TRUE(mutation_index + 1 == (int)mutations.size());
    delete mlog;

    // clear all
    utils::filesystem::remove_path(logp);
}
