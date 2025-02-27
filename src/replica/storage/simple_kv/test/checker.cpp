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

#include <boost/lexical_cast.hpp>
#include <algorithm>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>

#include "case.h"
#include "checker.h"
#include "common/replication_common.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "meta/meta_server_failure_detector.h"
#include "meta/meta_service.h"
#include "meta/meta_service_app.h"
#include "meta/partition_guardian.h"
#include "meta/server_load_balancer.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica/replication_service_app.h"
#include "replica/storage/simple_kv/test/common.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "runtime/service_app.h"
#include "runtime/service_engine.h"
#include "runtime/tool_api.h"
#include "utils/autoref_ptr.h"
#include "utils/factory_store.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DECLARE_string(partition_guardian_type);

namespace dsn {
class gpid;

namespace replication {
namespace test {

class checker_partition_guardian : public partition_guardian
{
public:
    static bool s_disable_balancer;

public:
    checker_partition_guardian(meta_service *svc) : partition_guardian(svc), _svc(svc) {}
    pc_status
    cure(meta_view view, const dsn::gpid &gpid, configuration_proposal_action &action) override
    {
        const partition_configuration &pc = *get_config(*view.apps, gpid);
        action.type = config_type::CT_INVALID;
        if (s_disable_balancer)
            return pc_status::healthy;

        pc_status result;
        if (!pc.hp_primary) {
            if (pc.hp_secondaries.size() > 0) {
                SET_OBJ_IP_AND_HOST_PORT(action, node, pc, secondaries[0]);
                for (unsigned int i = 1; i < pc.hp_secondaries.size(); ++i)
                    if (pc.hp_secondaries[i] < action.hp_node) {
                        SET_OBJ_IP_AND_HOST_PORT(action, node, pc, secondaries[i]);
                    }
                action.type = config_type::CT_UPGRADE_TO_PRIMARY;
                result = pc_status::ill;
            }

            else if (pc.hp_last_drops.size() == 0) {
                std::vector<host_port> sort_result;
                sort_alive_nodes(*view.nodes,
                                 server_load_balancer::primary_comparator(*view.nodes),
                                 sort_result);
                SET_IP_AND_HOST_PORT_BY_DNS(action, node, sort_result[0]);
                action.type = config_type::CT_ASSIGN_PRIMARY;
                result = pc_status::ill;
            }

            // DDD
            else {
                SET_IP_AND_HOST_PORT(
                    action, node, *pc.last_drops.rbegin(), *pc.hp_last_drops.rbegin());
                action.type = config_type::CT_ASSIGN_PRIMARY;
                LOG_ERROR("{} enters DDD state, we are waiting for its last primary node {} to "
                          "come back ...",
                          pc.pid,
                          FMT_HOST_PORT_AND_IP(action, node));
                result = pc_status::dead;
            }
            SET_OBJ_IP_AND_HOST_PORT(action, target, action, node);
        }

        else if (static_cast<int>(pc.hp_secondaries.size()) + 1 < pc.max_replica_count) {
            std::vector<host_port> sort_result;
            sort_alive_nodes(
                *view.nodes, server_load_balancer::partition_comparator(*view.nodes), sort_result);

            for (auto &node : sort_result) {
                if (!is_member(pc, node)) {
                    SET_IP_AND_HOST_PORT_BY_DNS(action, node, node);
                    break;
                }
            }
            SET_OBJ_IP_AND_HOST_PORT(action, target, pc, primary);
            action.type = config_type::CT_ADD_SECONDARY;
            result = pc_status::ill;
        } else {
            result = pc_status::healthy;
        }
        return result;
    }

    typedef std::function<bool(const host_port &addr1, const host_port &addr2)> node_comparator;
    static void sort_alive_nodes(const node_mapper &nodes,
                                 const node_comparator &cmp,
                                 std::vector<host_port> &sorted_node)
    {
        sorted_node.clear();
        sorted_node.reserve(nodes.size());
        for (auto &iter : nodes) {
            if (iter.first && iter.second.alive()) {
                sorted_node.push_back(iter.first);
            }
        }
        std::sort(sorted_node.begin(), sorted_node.end(), cmp);
    }

    meta_service *_svc;
};

bool test_checker::s_inited = false;
bool checker_partition_guardian::s_disable_balancer = false;

test_checker::test_checker() {}

void test_checker::control_balancer(bool disable_it)
{
    checker_partition_guardian::s_disable_balancer = disable_it;
    if (disable_it && meta_leader()) {
        server_state *ss = meta_leader()->_service->_state.get();
        for (auto &kv : ss->_exist_apps) {
            std::shared_ptr<app_state> &app = kv.second;
            app->helpers->clear_proposals();
        }
    }
}

bool test_checker::init(const std::string &name, const std::vector<service_app *> apps)
{
    if (s_inited)
        return false;

    _apps = apps;
    utils::factory_store<replication::partition_guardian>::register_factory(
        "checker_partition_guardian",
        replication::partition_guardian::create<checker_partition_guardian>,
        PROVIDER_TYPE_MAIN);

    for (auto &app : _apps) {
        if (app->info().type == "meta") {
            meta_service_app *meta_app = (meta_service_app *)app;
            meta_app->_service->_state->set_config_change_subscriber_for_test(
                std::bind(&test_checker::on_config_change, this, std::placeholders::_1));
            FLAGS_partition_guardian_type = "checker_partition_guardian";
            _meta_servers.push_back(meta_app);
        } else if (app->info().type ==
                   dsn::replication::replication_options::kReplicaAppType.c_str()) {
            replication_service_app *replica_app = (replication_service_app *)app;
            replica_app->_stub->set_replica_state_subscriber_for_test(
                std::bind(&test_checker::on_replica_state_change,
                          this,
                          std::placeholders::_1,
                          std::placeholders::_2,
                          std::placeholders::_3),
                false);
            _replica_servers.push_back(replica_app);
        }
    }

    const auto &nodes = dsn::service_engine::instance().get_all_nodes();
    for (const auto &node : nodes) {
        int id = node.second->id();
        std::string addr = node.second->full_name();
        const auto &hp = node.second->rpc()->primary_host_port();
        int port = hp.port();
        _node_to_host_port[addr] = hp;
        LOG_INFO("=== node_to_address[{}]={}", addr, hp);
        _address_to_node[port] = addr;
        LOG_INFO("=== address_to_node[{}]={}", port, addr);
        if (id != port) {
            _address_to_node[id] = addr;
            LOG_INFO("=== address_to_node[{}]={}", id, addr);
        }
    }

    s_inited = true;

    if (!test_case::instance().init(g_case_input)) {
        std::cerr << "init test_case failed" << std::endl;
        s_inited = false;
        return false;
    }

    return true;
}

void test_checker::exit()
{
    if (!s_inited)
        return;

    for (meta_service_app *app : _meta_servers) {
        app->_service->_started.store(false);
    }

    if (test_case::s_close_replica_stub_on_exit) {
        dsn::tools::tool_app *app = dsn::tools::get_current_tool();
        app->stop_all_apps(true);
    }
}

void test_checker::check()
{
    test_case::instance().on_check();
    if (g_done)
        return;

    // 'config_change' and 'replica_state_change' are detected in two ways:
    //   - each time this check() is called, checking will be applied
    //   - register subscribers on meta_server and replica_server to be notified

    parti_config cur_config;
    if (get_current_config(cur_config) && cur_config != _last_config) {
        test_case::instance().on_config_change(_last_config, cur_config);
        _last_config = cur_config;
    }

    state_snapshot cur_states;
    get_current_states(cur_states);
    if (cur_states != _last_states) {
        test_case::instance().on_state_change(_last_states, cur_states);
        _last_states = cur_states;
    }
}

void test_checker::on_replica_state_change(const host_port &from,
                                           const replica_configuration &new_config,
                                           bool is_closing)
{
    state_snapshot cur_states;
    get_current_states(cur_states);
    if (cur_states != _last_states) {
        test_case::instance().on_state_change(_last_states, cur_states);
        _last_states = cur_states;
    }
}

void test_checker::on_config_change(const app_mapper &new_config)
{
    const partition_configuration *pc = get_config(new_config, g_default_gpid);
    CHECK_NOTNULL(pc, "drop table is not allowed in test");

    parti_config cur_config;
    cur_config.convert_from(*pc);
    if (cur_config != _last_config) {
        test_case::instance().on_config_change(_last_config, cur_config);
        _last_config = cur_config;
    }
}

void test_checker::get_current_states(state_snapshot &states)
{
    states.state_map.clear();
    for (auto &app : _replica_servers) {
        if (!app->is_started())
            continue;

        for (auto &kv : app->_stub->_replicas) {
            replica_ptr r = kv.second;
            CHECK_EQ(kv.first, r->get_gpid());
            replica_id id(r->get_gpid(), app->info().full_name);
            replica_state &rs = states.state_map[id];
            rs.id = id;
            rs.status = r->status();
            rs.ballot = r->get_ballot();
            rs.last_committed_decree = r->last_committed_decree();
            rs.last_durable_decree = r->last_durable_decree();
        }
    }
}

bool test_checker::get_current_config(parti_config &config)
{
    meta_service_app *meta = meta_leader();
    if (meta == nullptr)
        return false;
    partition_configuration pc;

    // we should never try to acquire lock when we are in checker. Because we are the only
    // thread that is running.
    // The app and simulator have lots in common with the OS's userspace and kernel space.
    // In normal case, "apps" runs in "userspace". You can "trap into kernel(i.e. the simulator)" by
    // the rDSN's
    //"enqueue,dequeue and lock..."

    const meta_view view = meta->_service->_state->get_meta_view();
    config.convert_from(*get_config(*(view.apps), g_default_gpid));
    return true;
}

meta_service_app *test_checker::meta_leader()
{
    for (auto &meta : _meta_servers) {
        if (!meta->is_started())
            return nullptr;

        if (meta->_service->_failure_detector->get_leader(nullptr))
            return meta;
    }
    return nullptr;
}

bool test_checker::is_server_normal()
{
    auto meta = meta_leader();
    if (!meta)
        return false;
    return check_replica_state(1, 2, 0);
}

bool test_checker::check_replica_state(int primary_count, int secondary_count, int inactive_count)
{
    int p = 0;
    int s = 0;
    int i = 0;
    for (auto &rs : _replica_servers) {
        if (!rs->is_started())
            return false;
        for (auto &replica : rs->_stub->_replicas) {
            auto status = replica.second->status();
            if (status == partition_status::PS_PRIMARY)
                p++;
            else if (status == partition_status::PS_SECONDARY)
                s++;
            else if (status == partition_status::PS_INACTIVE)
                i++;
        }
    }
    return p == primary_count && s == secondary_count && i == inactive_count;
}

std::string test_checker::address_to_node_name(host_port addr)
{
    auto find = _address_to_node.find(addr.port());
    if (find != _address_to_node.end())
        return find->second;
    return "node@" + boost::lexical_cast<std::string>(addr.port());
}

host_port test_checker::node_name_to_address(const std::string &name)
{
    auto find = _node_to_host_port.find(name);
    if (find != _node_to_host_port.end())
        return find->second;
    return host_port();
}

void install_checkers()
{
    dsn::tools::simulator::register_checker("simple_kv.checker",
                                            dsn::tools::checker::create<wrap_checker>);
}
} // namespace test
} // namespace replication
} // namespace dsn
