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

#include "misc.h"

#include <cstdlib>
#include <iostream>

#include <boost/lexical_cast.hpp>

#include "common/replication_common.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"

using namespace dsn::replication;

uint32_t random32(uint32_t min, uint32_t max)
{
    uint32_t res = (uint32_t)(rand() % (max - min + 1));
    return res + min;
}

void generate_node_list(std::vector<dsn::rpc_address> &output_list, int min_count, int max_count)
{
    int count = random32(min_count, max_count);
    output_list.resize(count);
    for (int i = 0; i < count; ++i)
        output_list[i].assign_ipv4("127.0.0.1", i + 1);
}

void verbose_apps(const app_mapper &input_apps)
{
    std::cout << input_apps.size() << std::endl;
    for (const auto &apps : input_apps) {
        const std::shared_ptr<app_state> &app = apps.second;
        std::cout << apps.first << " " << app->partition_count << std::endl;
        for (int i = 0; i < app->partition_count; ++i) {
            std::cout << app->partitions[i].secondaries.size() + 1 << " "
                      << app->partitions[i].primary.to_string();
            for (int j = 0; j < app->partitions[i].secondaries.size(); ++j) {
                std::cout << " " << app->partitions[i].secondaries[j].to_string();
            }
            std::cout << std::endl;
        }
    }
}

void generate_node_mapper(
    /*out*/ node_mapper &output_nodes,
    const app_mapper &input_apps,
    const std::vector<dsn::rpc_address> &input_node_list)
{
    output_nodes.clear();
    for (auto &addr : input_node_list) {
        get_node_state(output_nodes, addr, true)->set_alive(true);
    }

    for (auto &kv : input_apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        for (const dsn::partition_configuration &pc : app->partitions) {
            node_state *ns;
            if (!pc.primary.is_invalid()) {
                ns = get_node_state(output_nodes, pc.primary, true);
                ns->put_partition(pc.pid, true);
            }
            for (const dsn::rpc_address &sec : pc.secondaries) {
                CHECK(!sec.is_invalid(), "");
                ns = get_node_state(output_nodes, sec, true);
                ns->put_partition(pc.pid, false);
            }
        }
    }
}

void generate_app(/*out*/ std::shared_ptr<app_state> &app,
                  const std::vector<dsn::rpc_address> &node_list)
{
    for (dsn::partition_configuration &pc : app->partitions) {
        pc.ballot = random32(1, 10000);
        std::vector<int> indices(3, 0);
        indices[0] = random32(0, node_list.size() - 3);
        indices[1] = random32(indices[0] + 1, node_list.size() - 2);
        indices[2] = random32(indices[1] + 1, node_list.size() - 1);

        int p = random32(0, 2);
        pc.primary = node_list[indices[p]];
        pc.secondaries.clear();
        for (unsigned int i = 0; i != indices.size(); ++i)
            if (i != p)
                pc.secondaries.push_back(node_list[indices[i]]);

        CHECK(!pc.primary.is_invalid(), "");
        CHECK(!is_secondary(pc, pc.primary), "");
        CHECK_EQ(pc.secondaries.size(), 2);
        CHECK_NE(pc.secondaries[0], pc.secondaries[1]);
    }
}

void generate_app_serving_replica_info(/*out*/ std::shared_ptr<dsn::replication::app_state> &app,
                                       int total_disks)
{
    char buffer[256];
    for (int i = 0; i < app->partition_count; ++i) {
        config_context &cc = app->helpers->contexts[i];
        dsn::partition_configuration &pc = app->partitions[i];
        replica_info ri;

        snprintf(buffer, 256, "disk%u", dsn::rand::next_u32(1, total_disks));
        ri.disk_tag = buffer;
        cc.collect_serving_replica(pc.primary, ri);

        for (const dsn::rpc_address &addr : pc.secondaries) {
            snprintf(buffer, 256, "disk%u", dsn::rand::next_u32(1, total_disks));
            ri.disk_tag = buffer;
            cc.collect_serving_replica(addr, ri);
        }
    }
}

void generate_apps(/*out*/ dsn::replication::app_mapper &mapper,
                   const std::vector<dsn::rpc_address> &node_list,
                   int apps_count,
                   int disks_per_node,
                   std::pair<uint32_t, uint32_t> partitions_range,
                   bool generate_serving_info)
{
    mapper.clear();
    dsn::app_info info;
    for (int i = 1; i <= apps_count; ++i) {
        info.status = dsn::app_status::AS_AVAILABLE;
        info.app_id = i;
        info.is_stateful = true;
        info.app_name = "test_app" + boost::lexical_cast<std::string>(i);
        info.app_type = "test";
        info.max_replica_count = 3;
        info.partition_count = random32(partitions_range.first, partitions_range.second);
        std::shared_ptr<app_state> the_app = app_state::create(info);
        generate_app(the_app, node_list);

        if (generate_serving_info) {
            generate_app_serving_replica_info(the_app, disks_per_node);
        }
        LOG_DEBUG("generated app, partitions({})", info.partition_count);
        mapper.emplace(the_app->app_id, the_app);
    }
}

void generate_node_fs_manager(const app_mapper &apps,
                              const node_mapper &nodes,
                              /*out*/ nodes_fs_manager &nfm,
                              int total_disks)
{
    nfm.clear();
    const char *prefix = "/home/work/";
    char pid_dir[256];
    std::vector<std::string> data_dirs(total_disks);
    std::vector<std::string> tags(total_disks);
    for (int i = 0; i < data_dirs.size(); ++i) {
        snprintf(pid_dir, 256, "%sdisk%d", prefix, i + 1);
        data_dirs[i] = pid_dir;
        snprintf(pid_dir, 256, "disk%d", i + 1);
        tags[i] = pid_dir;
    }

    for (const auto &kv : nodes) {
        const node_state &ns = kv.second;
        if (nfm.find(ns.addr()) == nfm.end()) {
            nfm.emplace(ns.addr(), std::make_shared<fs_manager>(true));
        }
        fs_manager &manager = *(nfm.find(ns.addr())->second);
        manager.initialize(data_dirs, tags, true);
        ns.for_each_partition([&](const dsn::gpid &pid) {
            const config_context &cc = *get_config_context(apps, pid);
            snprintf(pid_dir,
                     256,
                     "%s%s/%d.%d.test",
                     prefix,
                     cc.find_from_serving(ns.addr())->disk_tag.c_str(),
                     pid.get_app_id(),
                     pid.get_partition_index());
            LOG_DEBUG("concat pid_dir({}) of node({})", pid_dir, ns.addr());
            manager.add_replica(pid, pid_dir);
            return true;
        });
    }
}

void track_disk_info_check_and_apply(const dsn::replication::configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     /*in-out*/ dsn::replication::app_mapper &apps,
                                     /*in-out*/ dsn::replication::node_mapper & /*nodes*/,
                                     /*in-out*/ nodes_fs_manager &manager)
{
    config_context *cc = get_config_context(apps, pid);
    CHECK_NOTNULL(cc, "");

    fs_manager *target_manager = get_fs_manager(manager, act.target);
    CHECK_NOTNULL(target_manager, "");
    fs_manager *node_manager = get_fs_manager(manager, act.node);
    CHECK_NOTNULL(node_manager, "");

    std::string dir;
    replica_info ri;
    switch (act.type) {
    case config_type::CT_ASSIGN_PRIMARY:
        target_manager->allocate_dir(pid, "test", dir);
        CHECK_EQ(dsn::ERR_OK, target_manager->get_disk_tag(dir, ri.disk_tag));
        cc->collect_serving_replica(act.target, ri);
        break;

    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        node_manager->allocate_dir(pid, "test", dir);
        CHECK_EQ(dsn::ERR_OK, node_manager->get_disk_tag(dir, ri.disk_tag));
        cc->collect_serving_replica(act.node, ri);
        break;

    case config_type::CT_DOWNGRADE_TO_SECONDARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
        break;

    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
        node_manager->remove_replica(pid);
        cc->remove_from_serving(act.node);
        break;

    default:
        CHECK(false, "");
        break;
    }
}

void proposal_action_check_and_apply(const configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     app_mapper &apps,
                                     node_mapper &nodes,
                                     nodes_fs_manager *manager)
{
    dsn::partition_configuration &pc = *get_config(apps, pid);
    node_state *ns;

    ++pc.ballot;
    CHECK_NE(act.type, config_type::CT_INVALID);
    CHECK(!act.target.is_invalid(), "");
    CHECK(!act.node.is_invalid(), "");

    if (manager) {
        track_disk_info_check_and_apply(act, pid, apps, nodes, *manager);
    }

    switch (act.type) {
    case config_type::CT_ASSIGN_PRIMARY:
        CHECK_EQ(act.node, act.target);
        CHECK(pc.primary.is_invalid(), "");
        CHECK(pc.secondaries.empty(), "");

        pc.primary = act.node;
        ns = &nodes[act.node];
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
        ns->put_partition(pc.pid, true);
        break;

    case config_type::CT_ADD_SECONDARY:
        CHECK_EQ(act.target, pc.primary);
        CHECK(!is_member(pc, act.node), "");

        pc.secondaries.push_back(act.node);
        ns = &nodes[act.node];
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
        ns->put_partition(pc.pid, false);

        break;

    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        CHECK_EQ(act.node, act.target);
        CHECK_EQ(act.node, pc.primary);
        CHECK(nodes.find(act.node) != nodes.end(), "");
        CHECK(!is_secondary(pc, pc.primary), "");
        nodes[act.node].remove_partition(pc.pid, true);
        pc.secondaries.push_back(pc.primary);
        pc.primary.set_invalid();
        break;

    case config_type::CT_UPGRADE_TO_PRIMARY:
        CHECK(pc.primary.is_invalid(), "");
        CHECK_EQ(act.node, act.target);
        CHECK(is_secondary(pc, act.node), "");
        CHECK(nodes.find(act.node) != nodes.end(), "");

        ns = &nodes[act.node];
        pc.primary = act.node;
        CHECK(replica_helper::remove_node(act.node, pc.secondaries), "");
        ns->put_partition(pc.pid, true);
        break;

    case config_type::CT_ADD_SECONDARY_FOR_LB:
        CHECK_EQ(act.target, pc.primary);
        CHECK(!is_member(pc, act.node), "");
        CHECK(!act.node.is_invalid(), "");
        pc.secondaries.push_back(act.node);

        ns = &nodes[act.node];
        ns->put_partition(pc.pid, false);
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
        break;

    // in balancer, remove primary is not allowed
    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
        CHECK(!pc.primary.is_invalid(), "");
        CHECK_EQ(pc.primary, act.target);
        CHECK(is_secondary(pc, act.node), "");
        CHECK(nodes.find(act.node) != nodes.end(), "");
        CHECK(replica_helper::remove_node(act.node, pc.secondaries), "");

        ns = &nodes[act.node];
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
        ns->remove_partition(pc.pid, false);
        break;

    default:
        CHECK(false, "");
        break;
    }
}

void migration_check_and_apply(app_mapper &apps,
                               node_mapper &nodes,
                               migration_list &ml,
                               nodes_fs_manager *manager)
{
    int i = 0;
    for (auto kv = ml.begin(); kv != ml.end(); ++kv) {
        std::shared_ptr<configuration_balancer_request> &proposal = kv->second;
        LOG_DEBUG("the {}th round of proposal, gpid({})", i++, proposal->gpid);
        std::shared_ptr<app_state> &the_app = apps.find(proposal->gpid.get_app_id())->second;

        CHECK_EQ(proposal->gpid.get_app_id(), the_app->app_id);
        CHECK_LT(proposal->gpid.get_partition_index(), the_app->partition_count);
        dsn::partition_configuration &pc =
            the_app->partitions[proposal->gpid.get_partition_index()];

        CHECK(!pc.primary.is_invalid(), "");
        CHECK_EQ(pc.secondaries.size(), 2);
        for (auto &addr : pc.secondaries) {
            CHECK(!addr.is_invalid(), "");
        }
        CHECK(!is_secondary(pc, pc.primary), "");

        for (unsigned int j = 0; j < proposal->action_list.size(); ++j) {
            configuration_proposal_action &act = proposal->action_list[j];
            LOG_DEBUG("the {}th round of action, type: {}, node: {}, target: {}",
                      j,
                      dsn::enum_to_string(act.type),
                      act.node,
                      act.target);
            proposal_action_check_and_apply(act, proposal->gpid, apps, nodes, manager);
        }
    }
}

void app_mapper_compare(const app_mapper &mapper1, const app_mapper &mapper2)
{
    CHECK_EQ(mapper1.size(), mapper2.size());
    for (auto &kv : mapper1) {
        const std::shared_ptr<app_state> &app1 = kv.second;
        CHECK(mapper2.find(app1->app_id) != mapper2.end(), "");
        const std::shared_ptr<app_state> app2 = mapper2.find(app1->app_id)->second;

        CHECK_EQ(app1->app_id, app2->app_id);
        CHECK_EQ(app1->app_name, app2->app_name);
        CHECK_EQ(app1->app_type, app2->app_type);
        CHECK_EQ(app1->status, app2->status);
        CHECK(app1->status == dsn::app_status::AS_AVAILABLE ||
                  app1->status == dsn::app_status::AS_DROPPED,
              "");
        if (app1->status == dsn::app_status::AS_AVAILABLE) {
            CHECK_EQ(app1->partition_count, app2->partition_count);
            for (unsigned int i = 0; i < app1->partition_count; ++i) {
                CHECK(is_partition_config_equal(app1->partitions[i], app2->partitions[i]), "");
            }
        }
    }
}

bool spin_wait_condition(const std::function<bool()> &pred, int seconds)
{
    for (int i = 0; i != seconds; ++i) {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return pred();
}
