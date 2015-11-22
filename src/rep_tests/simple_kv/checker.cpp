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

# include "checker.h"
# include "case.h"


# include "../../apps/replication/lib/replica.h"
# include "../../apps/replication/lib/replica_stub.h"
# include "../../apps/replication/lib/replication_failure_detector.h"
# include "../../apps/replication/lib/mutation_log.h"
# include "../../apps/replication/meta_server/meta_service.h"
# include "../../apps/replication/meta_server/meta_server_failure_detector.h"
# include "../../apps/replication/meta_server/server_state.h"
# include "../../apps/replication/client_lib/replication_ds.h"
# include "../../core/core/service_engine.h"
# include "../../core/core/rpc_engine.h"

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
            _meta_servers.push_back((meta_service_app*)app.app_context_ptr);
        }
        else if (0 == strcmp(app.type, "replica"))
        {
            _replica_servers.push_back((replication_service_app*)app.app_context_ptr);
        }
    }

    auto nodes = ::dsn::service_engine::fast_instance().get_all_nodes();
    for (auto& node : nodes)
    {
        int id = node.second->id();
        std::string name = node.second->name();
        int port = node.second->rpc(nullptr)->primary_address().port();
        _address_to_node[port] = name;
        ddebug("=== address_to_node[%u]=%s", port, name.c_str());
        if (id != port)
        {
            _address_to_node[id] = name;
            ddebug("=== address_to_node[%u]=%s", id, name.c_str());
        }
    }

    if (!test_case::instance().init(g_case_input))
    {
        std::cerr << "init test_case failed" << std::endl;
        return false;
    }

    //ddebug("=== test_checker created");
    s_inited = true;
    return true;
}

void test_checker::exit()
{
    if (!s_inited) return;

    for (meta_service_app* app : _meta_servers)
    {
        app->_service->_started = false;
    }
    if ( !test_case::s_close_replica_stub_on_exit ) return;

    dsn::tools::tool_app* app = dsn::tools::get_current_tool();
    app->stop_all_apps(true);
}

void test_checker::check()
{
    test_case::instance().on_check();
    if (g_done) return;

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
        }
    }
}


bool test_checker::get_current_config(parti_config& config)
{
    meta_service_app* meta = meta_leader();
    if (meta == nullptr)
        return false;
    partition_configuration c;
    meta->_state->query_configuration_by_gpid(g_default_gpid, c);
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

void install_checkers()
{
    dsn_register_app_checker(
            "simple_kv.checker",
            ::dsn::tools::checker::create<wrap_checker>,
            ::dsn::tools::checker::apply
            );
}

}}}

