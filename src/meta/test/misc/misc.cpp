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

#include <boost/lexical_cast.hpp>
#include <fmt/core.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <stdio.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "duplication_types.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "rpc/dns_resolver.h" // IWYU pragma: keep
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"

using namespace dsn::replication;

uint32_t random32(uint32_t min, uint32_t max)
{
    uint32_t res = (uint32_t)(rand() % (max - min + 1));
    return res + min;
}

void generate_node_list(std::vector<dsn::host_port> &output_list, int min_count, int max_count)
{
    const auto count = random32(min_count, max_count);
    output_list.reserve(count);
    for (int i = 0; i < count; ++i) {
        output_list.emplace_back(dsn::host_port("localhost", i + 1));
    }
}

void verbose_apps(const app_mapper &input_apps)
{
    std::cout << input_apps.size() << std::endl;
    for (const auto &apps : input_apps) {
        const std::shared_ptr<app_state> &app = apps.second;
        std::cout << apps.first << " " << app->partition_count << std::endl;
        CHECK_EQ(app->partition_count, app->pcs.size());
        for (const auto &pc : app->pcs) {
            std::cout << pc.hp_secondaries.size() + 1 << " " << pc.hp_primary;
            for (const auto &secondary : pc.hp_secondaries) {
                std::cout << " " << secondary;
            }
            std::cout << std::endl;
        }
    }
}

void generate_node_mapper(
    /*out*/ node_mapper &output_nodes,
    const app_mapper &input_apps,
    const std::vector<dsn::host_port> &input_node_list)
{
    output_nodes.clear();
    for (const auto &hp : input_node_list) {
        get_node_state(output_nodes, hp, true)->set_alive(true);
    }

    for (auto &kv : input_apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        for (const auto &pc : app->pcs) {
            node_state *ns;
            if (pc.hp_primary) {
                ns = get_node_state(output_nodes, pc.hp_primary, true);
                ns->put_partition(pc.pid, true);
            }
            for (const auto &sec : pc.hp_secondaries) {
                CHECK(sec, "");
                ns = get_node_state(output_nodes, sec, true);
                ns->put_partition(pc.pid, false);
            }
        }
    }
}

void generate_app(/*out*/ std::shared_ptr<app_state> &app,
                  const std::vector<dsn::host_port> &node_list)
{
    for (auto &pc : app->pcs) {
        pc.ballot = random32(1, 10000);
        std::vector<int> indices(3, 0);
        indices[0] = random32(0, node_list.size() - 3);
        indices[1] = random32(indices[0] + 1, node_list.size() - 2);
        indices[2] = random32(indices[1] + 1, node_list.size() - 1);

        int p = random32(0, 2);
        SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, node_list[indices[p]]);
        for (unsigned int i = 0; i != indices.size(); ++i) {
            if (i != p) {
                ADD_IP_AND_HOST_PORT_BY_DNS(pc, secondaries, node_list[indices[i]]);
            }
        }

        CHECK(pc.hp_primary, "");
        CHECK(!is_secondary(pc, pc.hp_primary), "");
        CHECK(!is_secondary(pc, pc.primary), "");
        CHECK_EQ(pc.hp_secondaries.size(), 2);
        CHECK_NE(pc.hp_secondaries[0], pc.hp_secondaries[1]);
    }
}

void generate_app_serving_replica_info(/*out*/ std::shared_ptr<dsn::replication::app_state> &app,
                                       int total_disks)
{
    char buffer[256];
    for (int i = 0; i < app->partition_count; ++i) {
        auto &cc = app->helpers->contexts[i];
        auto &pc = app->pcs[i];
        replica_info ri;

        snprintf(buffer, 256, "disk%u", dsn::rand::next_u32(1, total_disks));
        ri.disk_tag = buffer;
        cc.collect_serving_replica(pc.hp_primary, ri);

        for (const auto &secondary : pc.hp_secondaries) {
            snprintf(buffer, 256, "disk%u", dsn::rand::next_u32(1, total_disks));
            ri.disk_tag = buffer;
            cc.collect_serving_replica(secondary, ri);
        }
    }
}

void generate_apps(/*out*/ dsn::replication::app_mapper &mapper,
                   const std::vector<dsn::host_port> &node_list,
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
        std::shared_ptr<app_state> app = app_state::create(info);
        generate_app(app, node_list);

        if (generate_serving_info) {
            generate_app_serving_replica_info(app, disks_per_node);
        }
        LOG_DEBUG("generated app, partitions({})", info.partition_count);
        mapper.emplace(app->app_id, app);
    }
}

void generate_node_fs_manager(const app_mapper &apps,
                              const node_mapper &nodes,
                              /*out*/ nodes_fs_manager &nfm,
                              int total_disks)
{
    nfm.clear();
    std::string prefix;
    CHECK(dsn::utils::filesystem::get_current_directory(prefix), "");
    std::vector<std::string> data_dirs(total_disks);
    std::vector<std::string> tags(total_disks);
    for (int i = 0; i < data_dirs.size(); ++i) {
        data_dirs[i] = fmt::format("{}disk{}", prefix, i + 1);
        tags[i] = fmt::format("disk{}", i + 1);
    }

    for (const auto &kv : nodes) {
        const node_state &ns = kv.second;
        if (nfm.find(ns.host_port()) == nfm.end()) {
            nfm.emplace(ns.host_port(), std::make_shared<fs_manager>());
        }
        fs_manager &manager = *(nfm.find(ns.host_port())->second);
        manager.initialize(data_dirs, tags);
        ns.for_each_partition([&](const dsn::gpid &pid) {
            const config_context &cc = *get_config_context(apps, pid);
            const auto dir = fmt::format("{}{}/{}.{}.test",
                                         prefix,
                                         cc.find_from_serving(ns.host_port())->disk_tag,
                                         pid.get_app_id(),
                                         pid.get_partition_index());
            LOG_DEBUG("concat {} of node({})", dir, ns.host_port());
            manager.add_replica(pid, dir);
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

    dsn::host_port hp_target, hp_node;
    GET_HOST_PORT(act, target, hp_target);
    GET_HOST_PORT(act, node, hp_node);

    fs_manager *target_manager = get_fs_manager(manager, hp_target);
    CHECK_NOTNULL(target_manager, "");
    fs_manager *node_manager = get_fs_manager(manager, hp_node);
    CHECK_NOTNULL(node_manager, "");

    std::string dir;
    replica_info ri;
    switch (act.type) {
    case config_type::CT_ASSIGN_PRIMARY: {
        auto selected = target_manager->find_best_dir_for_new_replica(pid);
        CHECK_NOTNULL(selected, "");
        selected->holding_replicas[pid.get_app_id()].emplace(pid);
        cc->collect_serving_replica(hp_target, ri);
        break;
    }
    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB: {
        auto selected = node_manager->find_best_dir_for_new_replica(pid);
        CHECK_NOTNULL(selected, "");
        selected->holding_replicas[pid.get_app_id()].emplace(pid);
        cc->collect_serving_replica(hp_node, ri);
        break;
    }
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
        break;

    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
        node_manager->remove_replica(pid);
        cc->remove_from_serving(hp_node);
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
    node_state *ns = nullptr;

    ++pc.ballot;
    CHECK_NE(act.type, config_type::CT_INVALID);
    CHECK(act.target, "");
    CHECK(act.node, "");

    if (manager) {
        track_disk_info_check_and_apply(act, pid, apps, nodes, *manager);
    }

    dsn::host_port hp_target, hp_node;
    GET_HOST_PORT(act, target, hp_target);
    GET_HOST_PORT(act, node, hp_node);

    switch (act.type) {
    case config_type::CT_ASSIGN_PRIMARY: {
        CHECK_EQ(act.hp_node, act.hp_target);
        CHECK_EQ(act.node, act.target);
        CHECK(!pc.hp_primary, "");
        CHECK(!pc.primary, "");
        CHECK(pc.hp_secondaries.empty(), "");
        CHECK(pc.secondaries.empty(), "");
        SET_IP_AND_HOST_PORT(pc, primary, act.node, hp_node);
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
        ns->put_partition(pc.pid, true);
        break;
    }
    case config_type::CT_ADD_SECONDARY: {
        CHECK_EQ(hp_target, pc.hp_primary);
        CHECK_EQ(act.hp_target, pc.hp_primary);
        CHECK_EQ(act.target, pc.primary);
        CHECK(!is_member(pc, hp_node), "");
        CHECK(!is_member(pc, act.node), "");
        ADD_IP_AND_HOST_PORT(pc, secondaries, act.node, hp_node);
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
        ns->put_partition(pc.pid, false);
        break;
    }
    case config_type::CT_DOWNGRADE_TO_SECONDARY: {
        CHECK_EQ(act.hp_node, act.hp_target);
        CHECK_EQ(act.node, act.target);
        CHECK_EQ(hp_node, hp_target);
        CHECK_EQ(act.hp_node, pc.hp_primary);
        CHECK_EQ(act.node, pc.primary);
        CHECK_EQ(hp_node, pc.hp_primary);
        CHECK(!is_secondary(pc, pc.hp_primary), "");
        CHECK(!is_secondary(pc, pc.primary), "");
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        ns->remove_partition(pc.pid, true);
        ADD_IP_AND_HOST_PORT(pc, secondaries, pc.primary, pc.hp_primary);
        RESET_IP_AND_HOST_PORT(pc, primary);
        break;
    }
    case config_type::CT_UPGRADE_TO_PRIMARY: {
        CHECK(!pc.hp_primary, "");
        CHECK(!pc.primary, "");
        CHECK_EQ(hp_node, hp_target);
        CHECK_EQ(act.hp_node, act.hp_target);
        CHECK_EQ(act.node, act.target);
        CHECK(is_secondary(pc, hp_node), "");
        CHECK(is_secondary(pc, act.node), "");
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        SET_IP_AND_HOST_PORT(pc, primary, act.node, hp_node);
        CHECK(REMOVE_IP_AND_HOST_PORT_BY_OBJ(act, node, pc, secondaries), "");
        ns->put_partition(pc.pid, true);
        break;
    }
    case config_type::CT_ADD_SECONDARY_FOR_LB: {
        CHECK_EQ(hp_target, pc.hp_primary);
        CHECK_EQ(act.hp_target, pc.hp_primary);
        CHECK_EQ(act.target, pc.primary);
        CHECK(!is_member(pc, hp_node), "");
        CHECK(!is_member(pc, act.node), "");
        CHECK(act.hp_node, "");
        CHECK(act.node, "");
        ADD_IP_AND_HOST_PORT(pc, secondaries, act.node, hp_node);
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        ns->put_partition(pc.pid, false);
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
        break;
    }
    // in balancer, remove primary is not allowed
    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_INACTIVE: {
        CHECK(pc.hp_primary, "");
        CHECK(pc.primary, "");
        CHECK_EQ(pc.hp_primary, hp_target);
        CHECK_EQ(pc.hp_primary, act.hp_target);
        CHECK_EQ(pc.primary, act.target);
        CHECK(is_secondary(pc, hp_node), "");
        CHECK(is_secondary(pc, act.node), "");
        CHECK(REMOVE_IP_AND_HOST_PORT_BY_OBJ(act, node, pc, secondaries), "");
        const auto node = nodes.find(hp_node);
        CHECK(node != nodes.end(), "");
        ns = &node->second;
        CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
        ns->remove_partition(pc.pid, false);
        break;
    }
    default:
        CHECK(false, "");
        break;
    }
}

void migration_check_and_apply(app_mapper &apps,
                               node_mapper &nodes,
                               const migration_list &ml,
                               nodes_fs_manager *manager)
{
    int i = 0;
    for (const auto &[_, proposal] : ml) {
        LOG_DEBUG("the {}th round of proposal, gpid({})", i++, proposal->gpid);
        const auto &app = apps.find(proposal->gpid.get_app_id())->second;

        CHECK_EQ(proposal->gpid.get_app_id(), app->app_id);
        CHECK_LT(proposal->gpid.get_partition_index(), app->partition_count);
        const auto &pc = app->pcs[proposal->gpid.get_partition_index()];

        CHECK(pc.hp_primary, "");
        CHECK_EQ(pc.hp_secondaries.size(), 2);
        for (const auto &hp : pc.hp_secondaries) {
            CHECK(hp, "");
        }
        CHECK(!is_secondary(pc, pc.hp_primary), "");
        CHECK(!is_secondary(pc, pc.primary), "");

        for (unsigned int j = 0; j < proposal->action_list.size(); ++j) {
            configuration_proposal_action &act = proposal->action_list[j];
            LOG_DEBUG("the {}th round of action, type: {}, node: {}, target: {}",
                      j,
                      dsn::enum_to_string(act.type),
                      FMT_HOST_PORT_AND_IP(act, node),
                      FMT_HOST_PORT_AND_IP(act, target));
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
                CHECK(is_partition_config_equal(app1->pcs[i], app2->pcs[i]), "");
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
