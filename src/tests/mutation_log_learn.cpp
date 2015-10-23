# include "mutation_log.h"
# include <gtest/gtest.h>

using namespace ::dsn;
using namespace ::dsn::replication;

TEST(replication, log_learn)
{
    multi_partition_decrees mdecrees, mdecrees2;
    global_partition_id gpid = { 1, 1 };
    std::string str = "hello, world!";
    std::string logp = "./test-log";
    std::vector<mutation_ptr> mutations;

    decree learn_points[] = {584,585,586, 594,595,596,604,605,606 };

    for (auto& lp : learn_points)
    {
        // prepare
        utils::filesystem::remove_path(logp);
        utils::filesystem::create_directory(logp);

        // writing logs
        mutation_log_ptr mlog = new mutation_log(
            1,
            50,
            1,
            false,
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
            mu->data.header.decree = i + 2;
            mu->data.header.gpid = gpid;
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

        decree durable_decree = lp;

        mlog->garbage_collection_when_as_commit_logs(gpid, durable_decree);
        mlog->close();
        
        // reading logs
        mlog = new mutation_log(
            1,
            50,
            1,
            true,
            true
            );

        mlog->initialize(logp.c_str());

        // learning
        learn_state state;
        mlog->get_learn_state_when_as_commit_logs(gpid, durable_decree + 1, state);
        mlog->close();
        mlog = nullptr;

        int64_t offset = 0;
        std::set<decree> learned_decress;
        
        mutation_log::replay(state.files,
            [&mutations, &learned_decress](mutation_ptr mu)
            {
                learned_decress.insert(mu->data.header.decree);

                mutation_ptr wmu = mutations[mu->data.header.decree - 2];
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
            },
            offset
            );

        for (decree s = durable_decree + 1; s < 1000; s++)
        {
            auto it = learned_decress.find(s);
            EXPECT_TRUE(it != learned_decress.end());
        }

        // clear all
        mdecrees.clear();
        mdecrees2.clear();
        mutations.clear();
        utils::filesystem::remove_path(logp);
    }
}
