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

# include "../../lib/replica.h"
# include "../../lib/replica_stub.h"
# include "../../lib/mutation_log.h"
# include "../../meta_server/meta_service.h"
# include "../../meta_server/meta_server_failure_detector.h"
# include "../../meta_server/server_state.h"
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

bool test_checker::s_inited = false;

test_checker::test_checker()
{
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

    for (auto& app : _apps)
    {
        if (0 == strcmp(app.type, "meta"))
        {
            meta_service_app* meta_app = (meta_service_app*)app.app.app_context_ptr;
            meta_app->_service->_state->set_config_change_subscriber_for_test(
                        std::bind(&test_checker::on_config_change, this, std::placeholders::_1));
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

void test_checker::on_config_change(const std::vector<app_state>& new_config)
{
    partition_configuration c = new_config[g_default_gpid.app_id - 1].partitions[g_default_gpid.pidx];
    parti_config cur_config;
    cur_config.convert_from(c);
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
    meta->_service->_state->query_configuration_by_gpid(g_default_gpid, c);
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
            if (status == PS_PRIMARY)
                p++;
            else if (status == PS_SECONDARY)
                s++;
            else if (status == PS_INACTIVE)
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

