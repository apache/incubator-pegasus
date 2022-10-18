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

#include <gtest/gtest.h>
#include "misc/misc.h"
#include "meta/meta_data.h"

using namespace dsn::replication;

TEST(meta_data, dropped_cmp)
{
    dsn::rpc_address n;

    dropped_replica d1, d2;
    // time not equal
    {
        d1 = {n, 10, 5, 5, 5};
        d2 = {n, 9, 20, 5, 5};
        ASSERT_TRUE(dropped_cmp(d1, d2) > 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) < 0);
    }
    // ballot not equal
    {
        d1 = {n, 0, 4, 4, 4};
        d2 = {n, 0, 5, 3, 3};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // last_committed_decree not equal
    {
        d1 = {n, 0, 4, 4, 4};
        d2 = {n, 0, 4, 6, 3};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // last_prepared_deree not equal
    {
        d1 = {n, 0, 7, 8, 9};
        d2 = {n, 0, 7, 8, 10};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // the same
    {
        d1 = {n, 0, 6, 6, 7};
        d2 = {n, 0, 6, 6, 7};

        ASSERT_TRUE(dropped_cmp(d1, d2) == 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) == 0);
    }
}

static bool vec_equal(const std::vector<dropped_replica> &vec1,
                      const std::vector<dropped_replica> &vec2)
{
    if (vec1.size() != vec2.size())
        return false;
    for (unsigned int i = 0; i != vec1.size(); ++i) {
        const dropped_replica &ds1 = vec1[i];
        const dropped_replica &ds2 = vec2[i];
        if (ds1.ballot != ds2.ballot)
            return false;
        if (ds1.last_prepared_decree != ds2.last_prepared_decree)
            return false;
        if (ds1.node != ds2.node)
            return false;
        if (ds1.time != ds2.time)
            return false;
    }
    return true;
}

TEST(meta_data, collect_replica)
{
    app_mapper app;
    node_mapper nodes;

    dsn::app_info info;
    info.app_id = 1;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_AVAILABLE;
    info.app_name = "test";
    info.app_type = "test";
    info.max_replica_count = 3;
    info.partition_count = 1024;
    std::shared_ptr<app_state> the_app = app_state::create(info);
    app.emplace(the_app->app_id, the_app);
    meta_view view = {&app, &nodes};

    replica_info rep;
    rep.app_type = "test";
    rep.pid = dsn::gpid(1, 0);

    dsn::partition_configuration &pc = *get_config(app, rep.pid);
    config_context &cc = *get_config_context(app, rep.pid);

    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 10, 10);

#define CLEAR_REPLICA                                                                              \
    do {                                                                                           \
        pc.primary.set_invalid();                                                                  \
        pc.secondaries.clear();                                                                    \
        pc.last_drops.clear();                                                                     \
    } while (false)

#define CLEAR_DROP_LIST                                                                            \
    do {                                                                                           \
        cc.dropped.clear();                                                                        \
    } while (false)

#define CLEAR_ALL                                                                                  \
    CLEAR_REPLICA;                                                                                 \
    CLEAR_DROP_LIST

    {
        // replica is primary of partition
        CLEAR_ALL;
        rep.ballot = 10;
        pc.ballot = 9;
        pc.primary = node_list[0];
        ASSERT_TRUE(collect_replica(view, node_list[0], rep));
    }

    {
        // replica is secondary of partition
        CLEAR_ALL;
        pc.secondaries.push_back(node_list[0]);
        ASSERT_TRUE(collect_replica(view, node_list[0], rep));
    }

    {
        // replica has been in the drop_list
        CLEAR_ALL;
        cc.dropped.push_back({node_list[0], 5, 0, 0});
        ASSERT_TRUE(collect_replica(view, node_list[0], rep));
    }

    {
        // drop_list all have timestamp, full
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], 5, 1, 1, 2},
            dropped_replica{node_list[1], 6, 1, 1, 2},
            dropped_replica{node_list[2], 7, 1, 1, 2},
            dropped_replica{node_list[3], 8, 1, 1, 2},
        };
        rep.ballot = 10;
        rep.last_prepared_decree = 10;
        ASSERT_FALSE(collect_replica(view, node_list[5], rep));
    }

    {
        // drop_list all have timestamp, not full
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], 5, 1, 1, 2},
            dropped_replica{node_list[1], 6, 1, 1, 2},
            dropped_replica{node_list[2], 7, 1, 1, 2},
        };
        rep.ballot = 10;
        rep.last_durable_decree = 6;
        rep.last_committed_decree = 8;
        rep.last_prepared_decree = 10;

        ASSERT_TRUE(collect_replica(view, node_list[4], rep));
        dropped_replica &d = cc.dropped.front();
        ASSERT_EQ(d.ballot, rep.ballot);
        ASSERT_EQ(d.last_prepared_decree, rep.last_prepared_decree);
    }

    {
        // drop_list mixed, full, minimal position
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 3, 5},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 5},
            dropped_replica{node_list[2], 7, 1, 1, 5},
            dropped_replica{node_list[3], 8, 1, 1, 5},
        };

        rep.ballot = 1;
        rep.last_committed_decree = 3;
        rep.last_prepared_decree = 5;
        ASSERT_FALSE(collect_replica(view, node_list[5], rep));
    }

    {
        // drop_list mixed, not full, minimal position
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 3, 5},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 5},
            dropped_replica{node_list[2], 7, 1, 1, 6},
        };

        rep.ballot = 1;
        rep.last_committed_decree = 3;
        rep.last_prepared_decree = 5;
        ASSERT_TRUE(collect_replica(view, node_list[5], rep));
        dropped_replica &d = cc.dropped.front();
        ASSERT_EQ(d.node, node_list[5]);
        ASSERT_EQ(d.ballot, rep.ballot);
        ASSERT_EQ(d.last_prepared_decree, rep.last_prepared_decree);
    }

    {
        // drop_list mixed, full, not minimal position
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 2, 6},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 6},
            dropped_replica{node_list[2], 7, 1, 1, 6},
            dropped_replica{node_list[3], 8, 1, 1, 6},
        };

        rep.ballot = 2;
        rep.last_committed_decree = 3;
        rep.last_prepared_decree = 6;
        ASSERT_TRUE(collect_replica(view, node_list[5], rep));
        dropped_replica &d = cc.dropped.front();
        ASSERT_EQ(rep.ballot, d.ballot);
        ASSERT_EQ(rep.last_committed_decree, rep.last_committed_decree);

        ASSERT_EQ(4, cc.dropped[1].last_committed_decree);
    }

    {
        // drop_list mixed, not full, not minimal position
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 2, 6},
                      dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 6},
                      dropped_replica{node_list[2], 7, 1, 1, 6}};

        rep.ballot = 3;
        rep.last_committed_decree = 1;
        rep.last_prepared_decree = 6;
        ASSERT_TRUE(collect_replica(view, node_list[5], rep));

        std::vector<dropped_replica> result_dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 2, 6},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 6},
            dropped_replica{node_list[5], dropped_replica::INVALID_TIMESTAMP, 3, 1, 6},
            dropped_replica{node_list[2], 7, 1, 1, 6}};

        ASSERT_TRUE(vec_equal(result_dropped, cc.dropped));
    }

    {
        // drop_list no timestamp, full, minimal position
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 2, 8},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 8},
            dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 2, 6, 8},
            dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 4, 2, 8},
        };

        rep.ballot = 1;
        rep.last_committed_decree = 7;
        rep.last_prepared_decree = 10;
        ASSERT_FALSE(collect_replica(view, node_list[5], rep));
    }

    {
        // drop_list no timestamp, full, middle position
        CLEAR_ALL;
        cc.dropped = {
            dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 2, 2, 8},
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 8},
            dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 2, 6, 8},
            dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 4, 2, 8},
        };

        rep.ballot = 3;
        rep.last_committed_decree = 6;
        rep.last_prepared_decree = 8;
        ASSERT_TRUE(collect_replica(view, node_list[5], rep));

        std::vector<dropped_replica> result_dropped = {
            dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 8},
            dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 2, 6, 8},
            dropped_replica{node_list[5], dropped_replica::INVALID_TIMESTAMP, 3, 6, 8},
            dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 4, 2, 8},
        };

        ASSERT_TRUE(vec_equal(result_dropped, cc.dropped));
    }

    {
        // drop_list no timestamp, full, largest position
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 2, 4, 8},
                      dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 2, 6, 8},
                      dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 4, 2, 8},
                      dropped_replica{node_list[4], dropped_replica::INVALID_TIMESTAMP, 4, 6, 8}};

        rep.ballot = 4;
        rep.last_committed_decree = 8;
        rep.last_prepared_decree = 8;
        ASSERT_TRUE(collect_replica(view, node_list[5], rep));

        std::vector<dropped_replica> result_dropped = {
            dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 2, 6, 8},
            dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 4, 2, 8},
            dropped_replica{node_list[4], dropped_replica::INVALID_TIMESTAMP, 4, 6, 8},
            dropped_replica{node_list[5], dropped_replica::INVALID_TIMESTAMP, 4, 8, 8}};

        ASSERT_TRUE(vec_equal(result_dropped, cc.dropped));
    }
#undef CLEAR_ALL
#undef CLEAR_REPLICA
#undef CLEAR_DROP_LIST
}

TEST(meta_data, construct_replica)
{
    app_mapper app;
    node_mapper nodes;

    dsn::app_info info;
    info.app_id = 1;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_AVAILABLE;
    info.app_name = "test";
    info.app_type = "test";
    info.max_replica_count = 3;
    info.partition_count = 1024;
    std::shared_ptr<app_state> the_app = app_state::create(info);
    app.emplace(the_app->app_id, the_app);
    meta_view view = {&app, &nodes};

    replica_info rep;
    rep.app_type = "test";
    rep.pid = dsn::gpid(1, 0);

    dsn::partition_configuration &pc = *get_config(app, rep.pid);
    config_context &cc = *get_config_context(app, rep.pid);

    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 10, 10);

#define CLEAR_REPLICA                                                                              \
    do {                                                                                           \
        pc.primary.set_invalid();                                                                  \
        pc.secondaries.clear();                                                                    \
        pc.last_drops.clear();                                                                     \
    } while (false)

#define CLEAR_DROP_LIST                                                                            \
    do {                                                                                           \
        cc.dropped.clear();                                                                        \
    } while (false)

#define CLEAR_ALL                                                                                  \
    CLEAR_REPLICA;                                                                                 \
    CLEAR_DROP_LIST

    // drop_list is empty, can't construct replica
    {
        CLEAR_ALL;
        ASSERT_FALSE(construct_replica(view, rep.pid, 3));
        ASSERT_EQ(0, replica_count(pc));
    }

    // only have one node in drop_list
    {
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 5, 10, 12}};
        ASSERT_TRUE(construct_replica(view, rep.pid, 3));
        ASSERT_EQ(node_list[0], pc.primary);
        ASSERT_TRUE(pc.secondaries.empty());
        ASSERT_TRUE(cc.dropped.empty());
        ASSERT_EQ(-1, cc.prefered_dropped);
    }

    // have multiple nodes, ballots are not same
    {
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 6, 10, 12},
                      dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 7, 10, 12},
                      dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 8, 10, 12},
                      dropped_replica{node_list[4], dropped_replica::INVALID_TIMESTAMP, 9, 11, 12}};
        ASSERT_TRUE(construct_replica(view, rep.pid, 3));
        ASSERT_EQ(node_list[4], pc.primary);
        ASSERT_TRUE(pc.secondaries.empty());

        std::vector<dsn::rpc_address> nodes = {node_list[2], node_list[3]};
        ASSERT_EQ(nodes, pc.last_drops);
        ASSERT_EQ(3, cc.dropped.size());
        ASSERT_EQ(2, cc.prefered_dropped);
    }

    // have multiple node, two have same ballots
    {
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 5, 10, 12},
                      dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 7, 11, 12},
                      dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 7, 12, 12}};

        ASSERT_TRUE(construct_replica(view, rep.pid, 3));
        ASSERT_EQ(node_list[2], pc.primary);
        ASSERT_TRUE(pc.secondaries.empty());

        std::vector<dsn::rpc_address> nodes = {node_list[0], node_list[1]};
        ASSERT_EQ(nodes, pc.last_drops);
        ASSERT_EQ(2, cc.dropped.size());
        ASSERT_EQ(1, cc.prefered_dropped);
    }

    // have multiple nodes, all have same ballots
    {
        CLEAR_ALL;
        cc.dropped = {dropped_replica{node_list[0], dropped_replica::INVALID_TIMESTAMP, 7, 11, 14},
                      dropped_replica{node_list[1], dropped_replica::INVALID_TIMESTAMP, 7, 12, 14},
                      dropped_replica{node_list[2], dropped_replica::INVALID_TIMESTAMP, 7, 13, 14},
                      dropped_replica{node_list[3], dropped_replica::INVALID_TIMESTAMP, 7, 14, 14}};

        ASSERT_TRUE(construct_replica(view, rep.pid, 3));
        ASSERT_EQ(node_list[3], pc.primary);
        ASSERT_TRUE(pc.secondaries.empty());

        std::vector<dsn::rpc_address> nodes = {node_list[1], node_list[2]};
        ASSERT_EQ(nodes, pc.last_drops);

        ASSERT_EQ(3, cc.dropped.size());
        ASSERT_EQ(2, cc.prefered_dropped);
    }
}
