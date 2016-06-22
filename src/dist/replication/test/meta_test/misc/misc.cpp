#include <cstdlib>
#include <iostream>

#include <dsn/dist/replication/replication.types.h>
#include "misc.h"
#include "replication_common.h"

using namespace dsn::replication;

#define ASSERT_EQ(left, right) dassert((left)==(right), "")
#define ASSERT_TRUE(exp) dassert((exp), "")
#define ASSERT_FALSE(exp) dassert(!(exp), "")

uint32_t random32(uint32_t min, uint32_t max)
{
    uint32_t res = (uint32_t)(rand()%(max-min+1));
    return res + min;
}

void generate_node_list(std::vector<dsn::rpc_address> &output_list, int min_count, int max_count)
{
    int count = random32(min_count, max_count);
    output_list.resize(count);
    for (int i=0; i<count; ++i)
        output_list[i].assign_ipv4("127.0.0.1", i+1);
}

void generate_node_mapper(
    /*out*/node_mapper& output_nodes,
    const app_mapper& input_apps,
    const std::vector<dsn::rpc_address>& input_node_list)
{
    output_nodes.clear();
    for (auto& addr: input_node_list) {
        output_nodes[addr].is_alive = true;
        output_nodes[addr].address = addr;
    }

    for (auto& kv: input_apps) {
        const std::shared_ptr<app_state>& app = kv.second;
        for (const dsn::partition_configuration& pc: app->partitions) {
            node_state* ns;
            if (!pc.primary.is_invalid()) {
                ns = &output_nodes[pc.primary];
                ns->is_alive = true;
                ns->primaries.emplace(pc.pid);
                ns->partitions.emplace(pc.pid);
            }
            for (const dsn::rpc_address& sec: pc.secondaries) {
                ASSERT_FALSE(sec.is_invalid());
                ns = &output_nodes[sec];
                ns->is_alive = true;
                ns->partitions.emplace(pc.pid);
            }
        }
    }
}

void generate_app(/*out*/std::shared_ptr<app_state>& app, const std::vector<dsn::rpc_address>& node_list)
{
    for (dsn::partition_configuration& pc: app->partitions)
    {
        pc.ballot = random32(1, 10000);
        std::vector<int> indices(3, 0);
        indices[0] = random32(0, node_list.size()-3);
        indices[1] = random32(indices[0]+1, node_list.size()-2);
        indices[2] = random32(indices[1]+1, node_list.size()-1);

        int p = random32(0, 2);
        pc.primary = node_list[indices[p]];
        pc.secondaries.clear();
        for (unsigned int i=0; i!=indices.size(); ++i)
            if (i != p)
                pc.secondaries.push_back(node_list[indices[i]]);

        ASSERT_FALSE(pc.primary.is_invalid());
        ASSERT_FALSE(is_secondary(pc, pc.primary));
        ASSERT_EQ(pc.secondaries.size(), 2);
        ASSERT_TRUE(pc.secondaries[0]!=pc.secondaries[1]);
    }
}

void migration_check_and_apply(app_mapper& apps, node_mapper& nodes, migration_list& ml)
{
    int i=0;
    for (std::shared_ptr<configuration_balancer_request> &proposal: ml)
    {
        dinfo("the %dth round of proposal, gpid(%d.%d)", i++, proposal->gpid.get_app_id(), proposal->gpid.get_partition_index());
        std::shared_ptr<app_state>& the_app = apps.find(proposal->gpid.get_app_id())->second;

        ASSERT_EQ(proposal->gpid.get_app_id(), the_app->app_id);
        ASSERT_TRUE(proposal->gpid.get_partition_index() < the_app->partition_count);
        dsn::partition_configuration& pc = the_app->partitions[proposal->gpid.get_partition_index()];

        ASSERT_FALSE(pc.primary.is_invalid());
        ASSERT_EQ(pc.secondaries.size(), 2);
        for (auto &addr: pc.secondaries)
            ASSERT_FALSE(addr.is_invalid());
        ASSERT_FALSE(is_secondary(pc, pc.primary));

        node_state* ns;
        for (unsigned int j=0; j<proposal->action_list.size(); ++j) {
            ++pc.ballot;
            configuration_proposal_action& act = proposal->action_list[j];
            dinfo("the %dth round of action, type: %s, node: %s, target: %s", j, dsn::enum_to_string(act.type), act.node.to_string(), act.target.to_string());
            ASSERT_TRUE(act.type!=config_type::CT_INVALID);
            ASSERT_FALSE(act.target.is_invalid());
            ASSERT_FALSE(act.node.is_invalid());

            switch (act.type) {
            case config_type::CT_DOWNGRADE_TO_SECONDARY:
                ASSERT_EQ(act.node, act.target);
                ASSERT_EQ(act.node, pc.primary);
                ASSERT_TRUE(nodes.find(act.node) != nodes.end());
                ASSERT_EQ(nodes[act.node].primaries.erase(pc.pid), 1);
                ASSERT_FALSE(is_secondary(pc, pc.primary));
                pc.secondaries.push_back(pc.primary);
                pc.primary.set_invalid();

                break;

            case config_type::CT_UPGRADE_TO_PRIMARY:
                ASSERT_TRUE(pc.primary.is_invalid());
                ASSERT_EQ(act.node, act.target);
                ASSERT_TRUE( is_secondary(pc, act.node) );
                ASSERT_TRUE(nodes.find(act.node)!=nodes.end());

                ns = &nodes[act.node];
                pc.primary = act.node;
                ASSERT_TRUE(replica_helper::remove_node(act.node, pc.secondaries));
                ASSERT_TRUE(ns->primaries.insert(pc.pid).second);
                ASSERT_FALSE(ns->partitions.insert(pc.pid).second);
                break;

            case config_type::CT_ADD_SECONDARY_FOR_LB:
                ASSERT_EQ(act.target, pc.primary);
                ASSERT_FALSE( is_member(pc, act.node) );
                ASSERT_FALSE(act.node.is_invalid());
                pc.secondaries.push_back(act.node);

                ns = &nodes[act.node];
                ASSERT_TRUE(ns->partitions.insert(pc.pid).second);
                ASSERT_TRUE(ns->primaries.find(pc.pid)==ns->primaries.end());
                break;

            //in balancer, remove primary is not allowed
            case config_type::CT_REMOVE:
            case config_type::CT_DOWNGRADE_TO_INACTIVE:
                ASSERT_FALSE(pc.primary.is_invalid());
                ASSERT_EQ(pc.primary, act.target);
                ASSERT_TRUE( is_secondary(pc, act.node) );
                ASSERT_TRUE(nodes.find(act.node)!=nodes.end());
                ASSERT_TRUE(replica_helper::remove_node(act.node, pc.secondaries));

                ns = &nodes[act.node];
                ASSERT_EQ(ns->partitions.erase(pc.pid), 1);
                ASSERT_EQ(ns->primaries.erase(pc.pid), 0);
                break;

            default:
                ASSERT_TRUE(false);
                break;
            }
        }
    }
}

void app_mapper_compare(const app_mapper& mapper1, const app_mapper& mapper2)
{
    ASSERT_EQ(mapper1.size(), mapper2.size());
    for (auto& kv: mapper1)
    {
        const std::shared_ptr<app_state>& app1 = kv.second;
        ASSERT_TRUE(mapper2.find(app1->app_id) != mapper2.end());
        const std::shared_ptr<app_state> app2 = mapper2.find(app1->app_id)->second;

        ASSERT_EQ(app1->app_id, app2->app_id);
        ASSERT_EQ(app1->app_name, app2->app_name);
        ASSERT_EQ(app1->app_type, app2->app_type);
        ASSERT_EQ(app1->status, app2->status);
        ASSERT_TRUE(app1->status==dsn::app_status::AS_AVAILABLE || app1->status==dsn::app_status::AS_DROPPED);
        if (app1->status == dsn::app_status::AS_AVAILABLE)
        {
            ASSERT_EQ(app1->partition_count, app2->partition_count);
            for (unsigned int i=0; i<app1->partition_count; ++i)
            {
                ASSERT_TRUE(is_partition_config_equal(app1->partitions[i], app2->partitions[i]));
            }
        }
    }
}
