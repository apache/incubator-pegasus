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

#include <fstream>

#include <gtest/gtest.h>

#include "common/gpid.h"
#include "backup_types.h"
#include "bulk_load_types.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include "consensus_types.h"
#include "duplication_types.h"
#include "meta_admin_types.h"
#include "meta_service_test_app.h"
#include "meta/greedy_load_balancer.h"
#include "meta/meta_data.h"
#include "meta/server_load_balancer.h"
#include "meta/test/misc/misc.h"
#include "partition_split_types.h"
#include "replica_admin_types.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/app_model.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/rpc/serialization.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/task/task_code.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/rpc_address.h"
#include "utils/threadpool_code.h"

namespace dsn {
namespace replication {

static void check_cure(app_mapper &apps, node_mapper &nodes, ::dsn::partition_configuration &pc)
{
    meta_service svc;
    partition_guardian guardian(&svc);
    pc_status ps = pc_status::invalid;
    node_state *ns;

    configuration_proposal_action act;
    while (ps != pc_status::healthy) {
        ps = guardian.cure({&apps, &nodes}, pc.pid, act);
        if (act.type == config_type::CT_INVALID)
            break;
        switch (act.type) {
        case config_type::CT_ASSIGN_PRIMARY:
            CHECK(pc.primary.is_invalid(), "");
            CHECK(pc.secondaries.empty(), "");
            CHECK_EQ(act.node, act.target);
            CHECK(nodes.find(act.node) != nodes.end(), "");

            CHECK_EQ(nodes[act.node].served_as(pc.pid), partition_status::PS_INACTIVE);
            nodes[act.node].put_partition(pc.pid, true);
            pc.primary = act.node;
            break;

        case config_type::CT_ADD_SECONDARY:
            CHECK(!is_member(pc, act.node), "");
            CHECK_EQ(pc.primary, act.target);
            CHECK(nodes.find(act.node) != nodes.end(), "");
            pc.secondaries.push_back(act.node);
            ns = &nodes[act.node];
            CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
            ns->put_partition(pc.pid, false);
            break;

        default:
            CHECK(false, "");
            break;
        }
    }

    // test upgrade to primary
    CHECK_EQ(nodes[pc.primary].served_as(pc.pid), partition_status::PS_PRIMARY);
    nodes[pc.primary].remove_partition(pc.pid, true);
    pc.primary.set_invalid();

    ps = guardian.cure({&apps, &nodes}, pc.pid, act);
    CHECK_EQ(act.type, config_type::CT_UPGRADE_TO_PRIMARY);
    CHECK(pc.primary.is_invalid(), "");
    CHECK_EQ(act.node, act.target);
    CHECK(is_secondary(pc, act.node), "");
    CHECK(nodes.find(act.node) != nodes.end(), "");

    ns = &nodes[act.node];
    pc.primary = act.node;
    std::remove(pc.secondaries.begin(), pc.secondaries.end(), pc.primary);

    CHECK_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
    ns->put_partition(pc.pid, true);
}

// static void verbose_nodes(const node_mapper& nodes)
//{
//    std::cout << "------------" << std::endl;
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("node: %s\ntotal_primaries: %d, total_secondaries: %d\n", n.first.to_string(),
//        ns.primary_count(), ns.partition_count());
//        for (int i=1; i<=2; ++i)
//        {
//            printf("app %d primaries: %d, app %d partitions: %d\n", i, ns.primary_count(i), i,
//            ns.partition_count(i));
//        }
//    }
//}
//
// static void verbose_app_node(const node_mapper& nodes)
//{
//    printf("Total_Pri: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.primary_count());
//    }
//    printf("\nTotal_Sec: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.secondary_count());
//    }
//    printf("\nApp01_Pri: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.primary_count(1));
//    }
//    printf("\nApp01_Sec: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.secondary_count(1));
//    }
//    printf("\nApp02_Pri: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.primary_count(2));
//    }
//    printf("\nApp02_Sec: ");
//    for (const auto& n: nodes)
//    {
//        const node_state& ns = n.second;
//        printf("%*d", 3, ns.secondary_count(2));
//    }
//    printf("\n");
//}

// static void verbose_app(const std::shared_ptr<app_state>& app)
//{
//    std::cout << app->app_name << " " << app->app_id << " " << app->partition_count << std::endl;
//    for (int i=0; i<app->partition_count; ++i)
//    {
//        const partition_configuration& pc = app->partitions[i];
//        std::cout << pc.primary.to_string();
//        for (int j=0; j<pc.secondaries.size(); ++j)
//        {
//            std::cout << " " << pc.secondaries[j].to_string();
//        }
//        std::cout << std::endl;
//    }
//}
// static void print_node_fs_manager(const app_mapper &apps,
//                                  const node_mapper &nodes,
//                                  const nodes_fs_manager &manager)
//{
//    int apps_count = apps.size();
//    for (const auto &kv : nodes) {
//        const node_state &ns = kv.second;
//        printf("%s: %d primaries, %d partitions\n",
//               ns.addr().to_string(),
//               ns.primary_count(),
//               ns.partition_count());
//        printf("%8s", "tag");
//        for (int i = 1; i <= apps_count; ++i) {
//            std::string app = std::string("app") + std::to_string(i);
//            printf("%8s", app.c_str());
//        }
//        printf("\n");
//        const fs_manager &m = manager.find(ns.addr())->second;
//        m.for_each_dir_node([apps_count](const dir_node &dn) {
//            printf("%8s", dn.tag.c_str());
//            for (int i = 1; i <= apps_count; ++i) {
//                printf("%8u", dn.replicas_count(i));
//            }
//            printf("%8u\n", dn.replicas_count());
//            return true;
//        });
//    }
//}

void meta_service_test_app::balancer_validator()
{
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 20, 100);

    app_mapper apps;
    node_mapper nodes;
    nodes_fs_manager manager;
    int disk_on_node = 9;

    meta_service svc;
    greedy_load_balancer glb(&svc);

    generate_apps(
        apps, node_list, 5, disk_on_node, std::pair<uint32_t, uint32_t>(1000, 2000), true);
    generate_node_mapper(nodes, apps, node_list);
    generate_node_fs_manager(apps, nodes, manager, disk_on_node);
    migration_list ml;

    for (auto &iter : nodes) {
        LOG_DEBUG("node(%s) have %d primaries, %d partitions",
                  iter.first.to_string(),
                  iter.second.primary_count(),
                  iter.second.partition_count());
    }

    // iterate 1000000 times
    for (int i = 0; i < 1000000 && glb.balance({&apps, &nodes}, ml); ++i) {
        LOG_DEBUG("the %dth round of balancer", i);
        migration_check_and_apply(apps, nodes, ml, &manager);
        glb.check({&apps, &nodes}, ml);
        LOG_DEBUG("balance checker operation count = %d", ml.size());
    }

    for (auto &iter : nodes) {
        LOG_DEBUG("node(%s) have %d primaries, %d partitions",
                  iter.first.to_string(),
                  iter.second.primary_count(),
                  iter.second.partition_count());
    }

    std::shared_ptr<app_state> &the_app = apps[1];
    for (::dsn::partition_configuration &pc : the_app->partitions) {
        CHECK(!pc.primary.is_invalid(), "");
        CHECK_GE(pc.secondaries.size(), pc.max_replica_count - 1);
    }

    // now test the cure
    ::dsn::partition_configuration &pc = the_app->partitions[0];
    nodes[pc.primary].remove_partition(pc.pid, false);
    for (const dsn::rpc_address &addr : pc.secondaries)
        nodes[addr].remove_partition(pc.pid, false);
    pc.primary.set_invalid();
    pc.secondaries.clear();

    // cure test
    check_cure(apps, nodes, pc);
}

dsn::rpc_address get_rpc_address(const std::string &ip_port)
{
    int splitter = ip_port.find_first_of(':');
    return rpc_address(ip_port.substr(0, splitter).c_str(),
                       boost::lexical_cast<int>(ip_port.substr(splitter + 1)));
}

static void load_apps_and_nodes(const char *file, app_mapper &apps, node_mapper &nodes)
{
    apps.clear();
    nodes.clear();

    std::ifstream infile(file, std::ios::in);
    int total_nodes;
    infile >> total_nodes;

    std::string ip_port;
    std::vector<dsn::rpc_address> node_list;
    for (int i = 0; i < total_nodes; ++i) {
        infile >> ip_port;
        node_list.push_back(get_rpc_address(ip_port));
    }

    int total_apps;
    infile >> total_apps;
    for (int i = 0; i < total_apps; ++i) {
        app_info info;
        infile >> info.app_id >> info.partition_count;
        info.app_name = "test_app_" + boost::lexical_cast<std::string>(info.app_id);
        info.app_type = "test";
        info.max_replica_count = 3;
        info.is_stateful = true;
        info.status = app_status::AS_AVAILABLE;

        std::shared_ptr<app_state> app(new app_state(info));
        apps[info.app_id] = app;
        for (int j = 0; j < info.partition_count; ++j) {
            int n;
            infile >> n;
            infile >> ip_port;
            app->partitions[j].primary = get_rpc_address(ip_port);
            for (int k = 1; k < n; ++k) {
                infile >> ip_port;
                app->partitions[j].secondaries.push_back(get_rpc_address(ip_port));
            }
        }
    }

    generate_node_mapper(nodes, apps, node_list);
}

void meta_service_test_app::balance_config_file()
{
    const char *suits[] = {"suite1", "suite2", nullptr};

    app_mapper apps;
    node_mapper nodes;

    for (int i = 0; suits[i]; ++i) {
        load_apps_and_nodes(suits[i], apps, nodes);

        greedy_load_balancer greedy_lb(nullptr);
        server_load_balancer *lb = &greedy_lb;
        migration_list ml;

        // iterate 1000 times
        for (int i = 0; i < 1000 && lb->balance({&apps, &nodes}, ml); ++i) {
            LOG_DEBUG("the %dth round of balancer", i);
            migration_check_and_apply(apps, nodes, ml, nullptr);
            lb->check({&apps, &nodes}, ml);
            LOG_DEBUG("balance checker operation count = %d", ml.size());
        }
    }
}
} // namespace replication
} // namespace dsn
