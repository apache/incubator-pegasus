// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gtest/gtest_prod.h>
#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "meta_admin_types.h"
#include "meta_data.h"
#include "rpc/rpc_host_port.h"
#include "utils/enum_helper.h"
#include "utils/zlocks.h"

namespace dsn {
class command_deregister;

class partition_configuration;

namespace replication {
class meta_service;

// disk_tag->primary_count/total_count_on_this_disk
typedef std::map<std::string, int> disk_load;

enum class balance_type
{
    COPY_PRIMARY = 0,
    COPY_SECONDARY,
    MOVE_PRIMARY,
    INVALID,
};
ENUM_BEGIN(balance_type, balance_type::INVALID)
ENUM_REG(balance_type::COPY_PRIMARY)
ENUM_REG(balance_type::COPY_SECONDARY)
ENUM_REG(balance_type::MOVE_PRIMARY)
ENUM_END(balance_type)

bool calc_disk_load(node_mapper &nodes,
                    const app_mapper &apps,
                    app_id id,
                    const host_port &node,
                    bool only_primary,
                    /*out*/ disk_load &load);
const std::string &get_disk_tag(const app_mapper &apps, const host_port &node, const gpid &pid);
std::shared_ptr<configuration_balancer_request>
generate_balancer_request(const app_mapper &apps,
                          const partition_configuration &pc,
                          const balance_type &type,
                          const host_port &from,
                          const host_port &to);

struct flow_path;

class load_balance_policy
{
public:
    load_balance_policy(meta_service *svc);
    virtual ~load_balance_policy();

    virtual void balance(bool checker, const meta_view *global_view, migration_list *list) = 0;

protected:
    void init(const meta_view *global_view, migration_list *list);
    bool is_ignored_app(app_id app_id);

    bool execute_balance(
        const app_mapper &apps,
        bool balance_checker,
        bool balance_in_turn,
        bool only_move_primary,
        const std::function<bool(const std::shared_ptr<app_state> &, bool)> &balance_operation);
    bool primary_balance(const std::shared_ptr<app_state> &app, bool only_move_primary);
    bool move_primary(std::unique_ptr<flow_path> path);
    bool copy_primary(const std::shared_ptr<app_state> &app, bool still_have_less_than_average);

    meta_service *_svc;
    const meta_view *_global_view;
    migration_list *_migration_result;
    int _alive_nodes;
    // this is used to assign an integer id for every node
    // and these are generated from the above data, which are tempory too
    std::unordered_map<dsn::host_port, int> host_port_id;
    std::vector<dsn::host_port> host_port_vec;

    // the app set which won't be re-balanced
    dsn::zrwlock_nr _balancer_ignored_apps_lock; // {
    std::set<app_id> _balancer_ignored_apps;
    // }
    std::unique_ptr<command_deregister> _ctrl_balancer_ignored_apps;

private:
    void start_moving_primary(const std::shared_ptr<app_state> &app,
                              const host_port &from,
                              const host_port &to,
                              int plan_moving,
                              disk_load *prev_load,
                              disk_load *current_load);
    std::list<dsn::gpid> calc_potential_moving(const std::shared_ptr<app_state> &app,
                                               const host_port &from,
                                               const host_port &to);
    dsn::gpid select_moving(std::list<dsn::gpid> &potential_moving,
                            disk_load *prev_load,
                            disk_load *current_load,
                            const host_port &from,
                            const host_port &to);
    void number_nodes(const node_mapper &nodes);

    std::string remote_command_balancer_ignored_app_ids(const std::vector<std::string> &args);
    std::string set_balancer_ignored_app_ids(const std::vector<std::string> &args);
    std::string get_balancer_ignored_app_ids();
    std::string clear_balancer_ignored_app_ids();

    FRIEND_TEST(cluster_balance_policy, calc_potential_moving);
};

struct flow_path
{
    flow_path(const std::shared_ptr<app_state> &app,
              std::vector<int> &&flow,
              std::vector<int> &&prev)
        : _app(app), _flow(std::move(flow)), _prev(std::move(prev))
    {
    }

    const std::shared_ptr<app_state> &_app;
    std::vector<int> _flow, _prev;
};

// Ford Fulkerson is used for primary balance.
// For more details: https://levy5307.github.io/blog/pegasus-balancer/
class ford_fulkerson
{
public:
    ford_fulkerson() = delete;
    ford_fulkerson(const std::shared_ptr<app_state> &app,
                   const node_mapper &nodes,
                   const std::unordered_map<dsn::host_port, int> &host_port_id,
                   uint32_t higher_count,
                   uint32_t lower_count,
                   int replicas_low);

    // using dijstra to find shortest path
    std::unique_ptr<flow_path> find_shortest_path();
    bool have_less_than_average() const { return _lower_count != 0; }

    class builder
    {
    public:
        builder(const std::shared_ptr<app_state> &app,
                const node_mapper &nodes,
                const std::unordered_map<dsn::host_port, int> &host_port_id)
            : _app(app), _nodes(nodes), _host_port_id(host_port_id)
        {
        }

        std::unique_ptr<ford_fulkerson> build()
        {
            auto nodes_count = _nodes.size();
            int replicas_low = _app->partition_count / nodes_count;
            int replicas_high = (_app->partition_count + nodes_count - 1) / nodes_count;

            uint32_t higher_count = 0, lower_count = 0;
            for (const auto &node : _nodes) {
                int primary_count = node.second.primary_count(_app->app_id);
                if (primary_count > replicas_high) {
                    higher_count++;
                } else if (primary_count < replicas_low) {
                    lower_count++;
                }
            }

            if (0 == higher_count && 0 == lower_count) {
                return nullptr;
            }
            return std::make_unique<ford_fulkerson>(
                _app, _nodes, _host_port_id, higher_count, lower_count, replicas_low);
        }

    private:
        const std::shared_ptr<app_state> &_app;
        const node_mapper &_nodes;
        const std::unordered_map<dsn::host_port, int> &_host_port_id;
    };

private:
    void make_graph();
    void add_edge(int node_id, const node_state &ns);
    void update_decree(int node_id, const node_state &ns);
    void handle_corner_case();

    int select_node(std::vector<bool> &visit, const std::vector<int> &flow);
    int max_value_pos(const std::vector<bool> &visit, const std::vector<int> &flow);
    void update_flow(int pos,
                     const std::vector<bool> &visit,
                     const std::vector<std::vector<int>> &network,
                     std::vector<int> &flow,
                     std::vector<int> &prev);

    const std::shared_ptr<app_state> &_app;
    const node_mapper &_nodes;
    const std::unordered_map<dsn::host_port, int> &_host_port_id;
    uint32_t _higher_count;
    uint32_t _lower_count;
    int _replicas_low;
    size_t _graph_nodes;
    std::vector<std::vector<int>> _network;

    FRIEND_TEST(ford_fulkerson, add_edge);
    FRIEND_TEST(ford_fulkerson, update_decree);
    FRIEND_TEST(ford_fulkerson, find_shortest_path);
    FRIEND_TEST(ford_fulkerson, max_value_pos);
    FRIEND_TEST(ford_fulkerson, select_node);
};

class copy_replica_operation
{
public:
    copy_replica_operation(const std::shared_ptr<app_state> app,
                           const app_mapper &apps,
                           node_mapper &nodes,
                           const std::vector<dsn::host_port> &host_port_vec,
                           const std::unordered_map<dsn::host_port, int> &host_port_id);
    virtual ~copy_replica_operation() = default;

    bool start(migration_list *result);

protected:
    void init_ordered_host_port_ids();
    virtual int get_partition_count(const node_state &ns) const = 0;

    gpid select_partition(migration_list *result);
    const partition_set *get_all_partitions();
    gpid select_max_load_gpid(const partition_set *partitions, migration_list *result);
    void copy_once(gpid selected_pid, migration_list *result);
    void update_ordered_host_port_ids();
    virtual bool only_copy_primary() = 0;
    virtual bool can_select(gpid pid, migration_list *result) = 0;
    virtual bool can_continue() = 0;
    virtual balance_type get_balance_type() = 0;

    std::set<int, std::function<bool(int left, int right)>> _ordered_host_port_ids;
    const std::shared_ptr<app_state> _app;
    const app_mapper &_apps;
    node_mapper &_nodes;
    const std::vector<dsn::host_port> &_host_port_vec;
    const std::unordered_map<dsn::host_port, int> &_host_port_id;
    std::unordered_map<dsn::host_port, disk_load> _node_loads;
    std::vector<int> _partition_counts;

    FRIEND_TEST(copy_primary_operation, misc);
    FRIEND_TEST(copy_replica_operation, get_all_partitions);
};

class copy_primary_operation : public copy_replica_operation
{
public:
    copy_primary_operation(const std::shared_ptr<app_state> app,
                           const app_mapper &apps,
                           node_mapper &nodes,
                           const std::vector<dsn::host_port> &host_port_vec,
                           const std::unordered_map<dsn::host_port, int> &host_port_id,
                           bool have_lower_than_average,
                           int replicas_low);
    ~copy_primary_operation() = default;

private:
    int get_partition_count(const node_state &ns) const;

    bool only_copy_primary() { return true; }
    bool can_select(gpid pid, migration_list *result);
    bool can_continue();
    enum balance_type get_balance_type();

    bool _have_lower_than_average;
    int _replicas_low;

    FRIEND_TEST(copy_primary_operation, misc);
    FRIEND_TEST(copy_primary_operation, can_select);
    FRIEND_TEST(copy_primary_operation, only_copy_primary);
};

configuration_proposal_action
new_proposal_action(const host_port &target, const host_port &node, config_type::type type);

} // namespace replication
} // namespace dsn
