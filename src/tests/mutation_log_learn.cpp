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
        mutation_log_ptr mlog = new mutation_log(logp, true, 1, 1);
        mlog->open(gpid, nullptr);

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

        mlog->garbage_collection(gpid, durable_decree, 0);
        mlog->close();
        
        // reading logs
        mlog = new mutation_log(logp, true, 1, 1);
        mlog->open(gpid, nullptr);

        // learning
        learn_state state;
        mlog->get_learn_state(gpid, durable_decree + 1, state);
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
                return true;
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
