#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include "meta_data.h"
#include "meta_service_test_app.h"
#include "server_load_balancer.h"
#include "greedy_load_balancer.h"
#include "../misc/misc.h"

using namespace dsn::replication;

#ifdef ASSERT_EQ
#undef ASSERT_EQ
#endif

#define ASSERT_EQ(left, right) dassert( (left)==(right), "")

#ifdef ASSERT_TRUE
#undef ASSERT_TRUE
#endif

#define ASSERT_TRUE(exp) dassert((exp), "")

#ifdef ASSERT_FALSE
#undef ASSERT_FALSE
#endif
#define ASSERT_FALSE(exp) dassert(!(exp), "")

static void generate_apps(/*out*/app_mapper& mapper, const std::vector<dsn::rpc_address>& node_list)
{
    mapper.clear();
    std::shared_ptr<app_state> the_app = app_state::create("test_app", "test", 1);
    the_app->status = dsn::app_status::AS_AVAILABLE;
    generate_app(the_app, node_list, random32(5, 20));
    mapper.emplace(the_app->app_id, the_app);
}

static void check_cure(server_load_balancer* lb, app_mapper& apps, node_mapper& nodes, ::dsn::partition_configuration& pc)
{
    pc_status ps = pc_status::invalid;
    node_state* ns;

    configuration_proposal_action act;
    while (ps != pc_status::healthy)
    {
        ps = lb->cure({&apps, &nodes}, pc.pid, act);
        if (act.type == config_type::CT_INVALID)
            break;
        switch (act.type) {
        case config_type::CT_ASSIGN_PRIMARY:
            ASSERT_TRUE(pc.primary.is_invalid() && pc.secondaries.size()==0);
            ASSERT_EQ(act.node, act.target);
            ASSERT_TRUE(nodes.find(act.node) != nodes.end());
            ASSERT_TRUE(nodes[act.node].primaries.insert(pc.pid).second);
            ASSERT_TRUE(nodes[act.node].partitions.insert(pc.pid).second);
            pc.primary = act.node;
            break;

        case config_type::CT_ADD_SECONDARY:
            ASSERT_FALSE(is_member(pc, act.node));
            ASSERT_EQ(pc.primary, act.target);
            ASSERT_TRUE(nodes.find(act.node)!=nodes.end());
            pc.secondaries.push_back(act.node);
            ns = &nodes[act.node];
            ASSERT_TRUE(ns->partitions.insert(pc.pid).second);
            break;

        default:
            ASSERT_TRUE(false);
            break;
        }
    }

    //test upgrade to primary
    ASSERT_EQ(nodes[pc.primary].primaries.erase(pc.pid), 1);
    pc.primary.set_invalid();

    ps = lb->cure({&apps, &nodes}, pc.pid, act);
    ASSERT_EQ(act.type, config_type::CT_UPGRADE_TO_PRIMARY);
    ASSERT_TRUE(pc.primary.is_invalid());
    ASSERT_EQ(act.node, act.target);
    ASSERT_TRUE( is_secondary(pc, act.node) );
    ASSERT_TRUE(nodes.find(act.node)!=nodes.end());

    ns = &nodes[act.node];
    pc.primary = act.node;
    std::remove(pc.secondaries.begin(), pc.secondaries.end(), pc.primary);

    ASSERT_TRUE(ns->primaries.insert(pc.pid).second);
    ASSERT_FALSE(ns->partitions.insert(pc.pid).second);
}

void meta_service_test_app::balancer_validator()
{
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 5, 100);

    app_mapper apps;
    node_mapper nodes;

    simple_load_balancer slb(nullptr);
    greedy_load_balancer glb(nullptr);
    std::vector<server_load_balancer*> lbs = { &slb, &glb };

    for (int i=0; i<lbs.size(); ++i)
    {
        std::cerr << "the " << i << " balancer" << std::endl;
        server_load_balancer* lb = lbs[i];

        generate_apps(apps, node_list);
        generate_node_mapper(nodes, apps, node_list);
        migration_list ml;

        for (auto& iter: nodes)
        {
            dinfo("node(%s) have %d primaries, %d partitions",
                iter.first.to_string(),
                iter.second.primaries.size(),
                iter.second.partitions.size());
        }

        //iterate 1000 times
        for (int i=0; i<1000 && lb->balance({&apps, &nodes}, ml); ++i) {
            dinfo("the %dth round of balancer", i);
            migration_check_and_apply(apps, nodes, ml);
        }

        for (auto& iter: nodes)
        {
            dinfo("node(%s) have %d primaries, %d partitions",
                iter.first.to_string(),
                iter.second.primaries.size(),
                iter.second.partitions.size());
        }

        std::shared_ptr<app_state>& the_app = apps[1];
        for (::dsn::partition_configuration& pc: the_app->partitions)
        {
            ASSERT_FALSE(pc.primary.is_invalid());
            ASSERT_TRUE(pc.secondaries.size() >= pc.max_replica_count-1);
        }

        //now test the cure
        ::dsn::partition_configuration& pc = the_app->partitions[0];
        nodes[pc.primary].primaries.erase(pc.pid);
        nodes[pc.primary].partitions.erase(pc.pid);
        for (const dsn::rpc_address& addr: pc.secondaries)
            nodes[addr].partitions.erase(pc.pid);
        pc.primary.set_invalid();
        pc.secondaries.clear();

        //cure test
        check_cure(lb, apps, nodes, pc);
    }
}
