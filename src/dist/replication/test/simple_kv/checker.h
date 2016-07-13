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

# pragma once

# include "common.h"

# include <dsn/utility/singleton.h>
# include <dsn/tool/global_checker.h>
# include "../../meta_server/server_state.h"
# include <dsn/dist/replication/meta_service_app.h>
# include <dsn/dist/replication/replication_service_app.h>

namespace dsn { namespace replication { namespace test {

using ::dsn::service::meta_service_app;

class test_checker : public dsn::utils::singleton<test_checker>
{
public:
    static bool s_inited;

public:
    test_checker();

    bool init(const char* name, dsn_app_info* info, int count);

    void exit();

    void check();

    meta_service_app* meta_leader();

    void control_balancer(bool disable_it);

    bool is_server_normal();

    bool check_replica_state(int primary_count, int secondary_count, int inactive_count);

    std::string address_to_node_name(rpc_address addr);
    rpc_address node_name_to_address(const std::string& name);

    void on_replica_state_change(::dsn::rpc_address from, const replica_configuration& new_config, bool is_closing);
    void on_config_change(const app_mapper &new_config);

    void get_current_states(state_snapshot& states);
    bool get_current_config(parti_config& config);
private:
    std::vector<dsn_app_info>             _apps;
    std::vector<meta_service_app*>        _meta_servers;
    std::vector<replication_service_app*> _replica_servers;

    parti_config                          _last_config;
    state_snapshot                        _last_states;

    std::map<std::string, dsn::rpc_address> _node_to_address; // address is primary_address()
    std::map<int, std::string>              _address_to_node; // port is enough for key
};

class wrap_checker : public dsn::tools::checker
{
public:
    wrap_checker(const char* name, dsn_app_info* info, int count)
        : dsn::tools::checker(name, info, count)
    {
        _checker = &test_checker::instance();
        if (!_checker->init(name, info, count))
        {
            g_done = true;
            g_fail = true;
        }
    }

    virtual void check() override
    {
        _checker->check();
    }

private:
    test_checker* _checker;
};

void install_checkers();

}}}

