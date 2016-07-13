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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "checker.h"
# include "case.h"
# include "dsn/utility/factory_store.h"

# include "../../lib/replica.h"
# include "../../lib/replica_stub.h"
# include "../../lib/mutation_log.h"
# include "../../meta_server/meta_service.h"
# include "../../meta_server/meta_server_failure_detector.h"
# include "../../meta_server/server_state.h"
# include "../../meta_server/server_load_balancer.h"
# include "../../client_lib/replication_ds.h"
# include "../../../../core/core/service_engine.h"
# include "../../../../core/core/rpc_engine.h"

# include <sstream>
# include <boost/lexical_cast.hpp>

# include <dsn/tool_api.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "simple_kv.checker"

namespace dsn { namespace replication { namespace test {

class checker_load_balancer: public simple_load_balancer
{
public:
    static bool s_disable_balancer;
public:
    checker_load_balancer(meta_service* svc): simple_load_balancer(svc) {}
    pc_status cure(const meta_view &view, const dsn::gpid& gpid, configuration_proposal_action &action) override
    {
        const partition_configuration& pc = *get_config(*view.apps, gpid);
        action.type = config_type::CT_INVALID;
        if (s_disable_balancer)
            return pc_status::healthy;

        pc_status result;
        if (pc.primary.is_invalid())
        {
            if (pc.secondaries.size() > 0)
            {
                action.node = pc.secondaries[0];
                for (unsigned int i=1; i<pc.secondaries.size(); ++i)
                    if (pc.secondaries[i] < action.node)
                        action.node = pc.secondaries[i];
                action.type = config_type::CT_UPGRADE_TO_PRIMARY;
                result = pc_status::ill;
            }

            else if (pc.last_drops.size() == 0)
            {
                std::vector<rpc_address> sort_result;
                sort_alive_nodes(*view.nodes, primary_comparator(*view.nodes), sort_result);
                action.node = sort_result[0];
                action.type = config_type::CT_ASSIGN_PRIMARY;
                result = pc_status::ill;
            }

            // DDD
            else
            {
                action.node = *pc.last_drops.rbegin();
                action.type = config_type::CT_ASSIGN_PRIMARY;
                derror("%d.%d enters DDD state, we are waiting for its last primary node %s to come back ...",
                    pc.pid.get_app_id(),
                    pc.pid.get_partition_index(),
                    action.node.to_string()
                    );
                result = pc_status::dead;
            }
            action.target = action.node;
        }

        else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
        {
            std::vector<rpc_address> sort_result;
            sort_alive_nodes(*view.nodes, partition_comparator(*view.nodes), sort_result);

            for (auto& node: sort_result) {
                if (!is_member(pc, node)) {
                    action.node = node;
                    break;
                }
            }
            action.target = pc.primary;
            action.type = config_type::CT_ADD_SECONDARY;
            result = pc_status::ill;
        }
        else
        {
            result = pc_status::healthy;
        }
        return result;
    }
};

bool test_checker::s_inited = false;
bool checker_load_balancer::s_disable_balancer = false;

test_checker::test_checker()
{
}

void test_checker::control_balancer(bool disable_it)
{
    checker_load_balancer::s_disable_balancer = disable_it;
    if (disable_it && meta_leader()) {
        server_state* ss = meta_leader()->_service->_state.get();
        for (auto& kv: ss->_exist_apps) {
            std::shared_ptr<app_state>& app = kv.second;
            app->helpers->clear_proposals();
        }
    }
}

bool test_checker::init(const char* name, dsn_app_info* info, int count)
{
    if (s_inited)
        return false;

    _apps.resize(count);
    for (int i = 0; i < count; i++)
    {
        _apps[i] = info[i];
    }

    utils::factory_store<replication::server_load_balancer>::register_factory(
        "checker_load_balancer",
        replication::server_load_balancer::create<checker_load_balancer>,
        PROVIDER_TYPE_MAIN);

    for (auto& app : _apps)
    {
        if (0 == strcmp(app.type, "meta"))
        {
            meta_service_app* meta_app = (meta_service_app*)app.app.app_context_ptr;
            meta_app->_service->_state->set_config_change_subscriber_for_test(
                        std::bind(&test_checker::on_config_change, this, std::placeholders::_1));
            meta_app->_service->_meta_opts.server_load_balancer_type = "checker_load_balancer";
            _meta_servers.push_back(meta_app);
        }
        else if (0 == strcmp(app.type, "replica"))
        {
            replication_service_app* replica_app = (replication_service_app*)app.app.app_context_ptr;
            replica_app->_stub->set_replica_state_subscriber_for_test(
                        std::bind(&test_checker::on_replica_state_change, this,
                                  std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                        false);
            _replica_servers.push_back(replica_app);
        }
    }

    auto nodes = ::dsn::service_engine::fast_instance().get_all_nodes();
    for (auto& node : nodes)
    {
        int id = node.second->id();
        std::string name = node.second->name();
        rpc_address paddr = node.second->rpc(nullptr)->primary_address();
        int port = paddr.port();
        _node_to_address[name] = paddr;
        ddebug("=== node_to_address[%s]=%s", name.c_str(), paddr.to_string());
        _address_to_node[port] = name;
        ddebug("=== address_to_node[%u]=%s", port, name.c_str());
        if (id != port)
        {
            _address_to_node[id] = name;
            ddebug("=== address_to_node[%u]=%s", id, name.c_str());
        }
    }

    s_inited = true;

    if (!test_case::instance().init(g_case_input))
    {
        std::cerr << "init test_case failed" << std::endl;
        s_inited = false;
        return false;
    }

    return true;
}

void test_checker::exit()
{
    if (!s_inited) return;

    for (meta_service_app* app : _meta_servers)
    {
        app->_service->_started = false;
    }

    if (test_case::s_close_replica_stub_on_exit)
    {
        dsn::tools::tool_app* app = dsn::tools::get_current_tool();
        app->stop_all_apps(true);
    }
}

void test_checker::check()
{
    test_case::instance().on_check();
    if (g_done) return;

    // 'config_change' and 'replica_state_change' are detected in two ways:
    //   - each time this check() is called, checking will be applied
    //   - register subscribers on meta_server and replica_server to be notified

    parti_config cur_config;
    if (get_current_config(cur_config) && cur_config != _last_config)
    {
        test_case::instance().on_config_change(_last_config, cur_config);
        _last_config = cur_config;
    }

    state_snapshot cur_states;
    get_current_states(cur_states);
    if (cur_states != _last_states)
    {
        test_case::instance().on_state_change(_last_states, cur_states);
        _last_states = cur_states;
    }
}

void test_checker::on_replica_state_change(::dsn::rpc_address from, const replica_configuration& new_config, bool is_closing)
{
    state_snapshot cur_states;
    get_current_states(cur_states);
    if (cur_states != _last_states)
    {
        test_case::instance().on_state_change(_last_states, cur_states);
        _last_states = cur_states;
    }
}

void test_checker::on_config_change(const app_mapper& new_config)
{
    const partition_configuration* pc = get_config(new_config, g_default_gpid);
    dassert(pc != nullptr, "drop table is not allowed in test");

    parti_config cur_config;
    cur_config.convert_from(*pc);
    if (cur_config != _last_config)
    {
        test_case::instance().on_config_change(_last_config, cur_config);
        _last_config = cur_config;
    }
}

void test_checker::get_current_states(state_snapshot& states)
{
    states.state_map.clear();
    for (auto& app : _replica_servers)
    {
        if (!app->is_started())
            continue;

        for (auto& kv : app->_stub->_replicas)
        {
            replica_ptr r = kv.second;
            dassert(kv.first == r->get_gpid(), "");
            replica_id id(r->get_gpid(), app->name());
            replica_state& rs = states.state_map[id];
            rs.id = id;
            rs.status = r->status();
            rs.ballot = r->get_ballot();
            rs.last_committed_decree = r->last_committed_decree();
            rs.last_durable_decree = r->last_durable_decree();
        }
    }
}

bool test_checker::get_current_config(parti_config& config)
{
    meta_service_app* meta = meta_leader();
    if (meta == nullptr)
        return false;
    partition_configuration c;

    //we should never try to acquire lock when we are in checker. Because we are the only
    //thread that is running.
    //The app and simulator have lots in common with the OS's userspace and kernel space.
    //In normal case, "apps" runs in "userspace". You can "trap into kernel(i.e. the simulator)" by the rDSN's
    //"enqueue,dequeue and lock..."

    //meta->_service->_state->query_configuration_by_gpid(g_default_gpid, c);
    const meta_view view = meta->_service->_state->get_meta_view();
    const partition_configuration* pc = get_config(*(view.apps), g_default_gpid);
    c = *pc;
    config.convert_from(c);
    return true;
}

meta_service_app* test_checker::meta_leader()
{
    for (auto& meta : _meta_servers)
    {
        if (!meta->is_started())
            return nullptr;

        if (meta->_service->_failure_detector->is_primary())
            return meta;
    }
    return nullptr;
}

bool test_checker::is_server_normal()
{
    auto meta = meta_leader();
    if (!meta) return false;
    return check_replica_state(1, 2, 0);
}

bool test_checker::check_replica_state(int primary_count, int secondary_count, int inactive_count)
{
    int p = 0;
    int s = 0;
    int i = 0;
    for (auto& rs : _replica_servers)
    {
        if (!rs->is_started())
            return false;
        for (auto& replica : rs->_stub->_replicas)
        {
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

std::string test_checker::address_to_node_name(rpc_address addr)
{
    auto find = _address_to_node.find(addr.port());
    if (find != _address_to_node.end())
        return find->second;
    return "node@" + boost::lexical_cast<std::string>(addr.port());
}

rpc_address test_checker::node_name_to_address(const std::string& name)
{
    auto find = _node_to_address.find(name);
    if (find != _node_to_address.end())
        return find->second;
    return rpc_address();
}

void install_checkers()
{
    dsn_register_app_checker(
            "simple_kv.checker",
            ::dsn::tools::checker::create<wrap_checker>,
            ::dsn::tools::checker::apply
            );
}

}}}

