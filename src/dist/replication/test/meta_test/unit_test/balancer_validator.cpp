#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication_types.h>
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>

#include <fstream>

#include "meta_data.h"
#include "meta_service_test_app.h"
#include "server_load_balancer.h"
#include "greedy_load_balancer.h"
#include "../misc/misc.h"

using namespace dsn;
using namespace dsn::replication;

#ifdef ASSERT_EQ
#undef ASSERT_EQ
#endif

#define ASSERT_EQ(left, right) dassert((left) == (right), "")

#ifdef ASSERT_TRUE
#undef ASSERT_TRUE
#endif

#define ASSERT_TRUE(exp) dassert((exp), "")

#ifdef ASSERT_FALSE
#undef ASSERT_FALSE
#endif
#define ASSERT_FALSE(exp) dassert(!(exp), "")

static void generate_apps(/*out*/ app_mapper &mapper,
                          const std::vector<dsn::rpc_address> &node_list,
                          int apps_count)
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
        info.partition_count = random32(1000, 2000);
        std::shared_ptr<app_state> the_app = app_state::create(info);
        generate_app(the_app, node_list);

        dinfo("generated app, partitions(%d)", info.partition_count);
        mapper.emplace(the_app->app_id, the_app);
    }
}

static void check_cure(server_load_balancer *lb,
                       app_mapper &apps,
                       node_mapper &nodes,
                       ::dsn::partition_configuration &pc)
{
    pc_status ps = pc_status::invalid;
    node_state *ns;

    configuration_proposal_action act;
    while (ps != pc_status::healthy) {
        ps = lb->cure({&apps, &nodes}, pc.pid, act);
        if (act.type == config_type::CT_INVALID)
            break;
        switch (act.type) {
        case config_type::CT_ASSIGN_PRIMARY:
            ASSERT_TRUE(pc.primary.is_invalid() && pc.secondaries.size() == 0);
            ASSERT_EQ(act.node, act.target);
            ASSERT_TRUE(nodes.find(act.node) != nodes.end());

            ASSERT_EQ(nodes[act.node].served_as(pc.pid), partition_status::PS_INACTIVE);
            nodes[act.node].put_partition(pc.pid, true);
            pc.primary = act.node;
            break;

        case config_type::CT_ADD_SECONDARY:
            ASSERT_FALSE(is_member(pc, act.node));
            ASSERT_EQ(pc.primary, act.target);
            ASSERT_TRUE(nodes.find(act.node) != nodes.end());
            pc.secondaries.push_back(act.node);
            ns = &nodes[act.node];
            ASSERT_EQ(ns->served_as(pc.pid), partition_status::PS_INACTIVE);
            ns->put_partition(pc.pid, false);
            break;

        default:
            ASSERT_TRUE(false);
            break;
        }
    }

    // test upgrade to primary
    ASSERT_EQ(nodes[pc.primary].served_as(pc.pid), partition_status::PS_PRIMARY);
    nodes[pc.primary].remove_partition(pc.pid, true);
    pc.primary.set_invalid();

    ps = lb->cure({&apps, &nodes}, pc.pid, act);
    ASSERT_EQ(act.type, config_type::CT_UPGRADE_TO_PRIMARY);
    ASSERT_TRUE(pc.primary.is_invalid());
    ASSERT_EQ(act.node, act.target);
    ASSERT_TRUE(is_secondary(pc, act.node));
    ASSERT_TRUE(nodes.find(act.node) != nodes.end());

    ns = &nodes[act.node];
    pc.primary = act.node;
    std::remove(pc.secondaries.begin(), pc.secondaries.end(), pc.primary);

    ASSERT_EQ(ns->served_as(pc.pid), partition_status::PS_SECONDARY);
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

void meta_service_test_app::balancer_validator()
{
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 20, 100);

    app_mapper apps;
    node_mapper nodes;

    meta_service svc;
    simple_load_balancer slb(&svc);
    greedy_load_balancer glb(&svc);
    std::vector<server_load_balancer *> lbs = {&slb, &glb};

    for (int i = 0; i < lbs.size(); ++i) {
        std::cerr << "the " << i << "th balancer" << std::endl;
        server_load_balancer *lb = lbs[i];

        generate_apps(apps, node_list, 5);
        generate_node_mapper(nodes, apps, node_list);
        migration_list ml;

        for (auto &iter : nodes) {
            dinfo("node(%s) have %d primaries, %d partitions",
                  iter.first.to_string(),
                  iter.second.primary_count(),
                  iter.second.partition_count());
        }

        // iterate 1000 times
        for (int i = 0; i < 1000000 && lb->balance({&apps, &nodes}, ml); ++i) {
            dinfo("the %dth round of balancer", i);
            migration_check_and_apply(apps, nodes, ml);
        }

        for (auto &iter : nodes) {
            dinfo("node(%s) have %d primaries, %d partitions",
                  iter.first.to_string(),
                  iter.second.primary_count(),
                  iter.second.partition_count());
        }

        std::shared_ptr<app_state> &the_app = apps[1];
        for (::dsn::partition_configuration &pc : the_app->partitions) {
            ASSERT_FALSE(pc.primary.is_invalid());
            ASSERT_TRUE(pc.secondaries.size() >= pc.max_replica_count - 1);
        }

        // now test the cure
        ::dsn::partition_configuration &pc = the_app->partitions[0];
        nodes[pc.primary].remove_partition(pc.pid, false);
        for (const dsn::rpc_address &addr : pc.secondaries)
            nodes[addr].remove_partition(pc.pid, false);
        pc.primary.set_invalid();
        pc.secondaries.clear();

        // cure test
        check_cure(lb, apps, nodes, pc);
    }
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
            dinfo("the %dth round of balancer", i);
            migration_check_and_apply(apps, nodes, ml);
        }
    }
}
