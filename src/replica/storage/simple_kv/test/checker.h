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

#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "common.h"
#include "meta/meta_data.h"
#include "runtime/rpc/rpc_host_port.h"
#include "runtime/simulator.h"
#include "utils/singleton.h"

namespace dsn {
class service_app;
namespace service {
class meta_service_app;
} // namespace service

namespace replication {
class replica_configuration;
class replication_service_app;

namespace test {

using ::dsn::service::meta_service_app;

class test_checker : public dsn::utils::singleton<test_checker>
{
public:
    static bool s_inited;

public:
    test_checker();

    bool init(const std::string &name, const std::vector<service_app *> apps);

    void exit();

    void check();

    meta_service_app *meta_leader();

    void control_balancer(bool disable_it);

    bool is_server_normal();

    bool check_replica_state(int primary_count, int secondary_count, int inactive_count);

    std::string address_to_node_name(host_port addr);
    host_port node_name_to_address(const std::string &name);

    void on_replica_state_change(::dsn::host_port from,
                                 const replica_configuration &new_config,
                                 bool is_closing);
    void on_config_change(const app_mapper &new_config);

    void get_current_states(state_snapshot &states);
    bool get_current_config(parti_config &config);

private:
    std::vector<service_app *> _apps;
    std::vector<meta_service_app *> _meta_servers;
    std::vector<replication_service_app *> _replica_servers;

    parti_config _last_config;
    state_snapshot _last_states;

    std::map<std::string, dsn::host_port> _node_to_host_port; // host_port is primary_host_port()
    std::map<int, std::string> _address_to_node;              // port is enough for key
};

class wrap_checker : public dsn::tools::checker
{
public:
    wrap_checker() : dsn::tools::checker() {}

    virtual void initialize(const std::string &name, const std::vector<service_app *> &apps)
    {
        _checker = &test_checker::instance();
        if (!_checker->init(name, apps)) {
            g_done = true;
            g_fail = true;
        }
    }
    virtual void check() override { _checker->check(); }

private:
    test_checker *_checker;
};

void install_checkers();
}
}
}
