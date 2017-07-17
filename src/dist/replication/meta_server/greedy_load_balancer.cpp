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
 *     a greedy load balancer implementation
 *
 * Revision history:
 *     2016-02-3, Weijie Sun(sunweijie@xiaomi.com), first version
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), refactor, now the balancer only have
 * strategy
 */

#include <algorithm>
#include <iostream>
#include <queue>
#include "greedy_load_balancer.h"
#include "meta_data.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "lb.greedy"

namespace dsn {
namespace replication {

std::shared_ptr<configuration_balancer_request>
greedy_load_balancer::generate_balancer_request(const partition_configuration &pc,
                                                const balance_type &type,
                                                const rpc_address &from,
                                                const rpc_address &to)
{
    configuration_balancer_request result;
    result.gpid = pc.pid;

    std::string ans;
    switch (type) {
    case balance_type::move_primary:
        ans = "move_primary";
        result.action_list.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
        break;
    case balance_type::copy_primary:
        ans = "copy_primary";
        result.action_list.emplace_back(
            configuration_proposal_action{from, to, config_type::CT_ADD_SECONDARY_FOR_LB});
        result.action_list.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, from, config_type::CT_REMOVE});
        break;
    case balance_type::copy_secondary:
        ans = "copy_secondary";
        result.action_list.emplace_back(
            configuration_proposal_action{pc.primary, to, config_type::CT_ADD_SECONDARY_FOR_LB});
        result.action_list.emplace_back(
            configuration_proposal_action{pc.primary, from, config_type::CT_REMOVE});
        break;
    default:
        dassert(false, "");
    }
    dinfo("balancer req: %d.%d %s from %s to %s",
          pc.pid.get_app_id(),
          pc.pid.get_partition_index(),
          ans.c_str(),
          from.to_string(),
          to.to_string());
    return std::make_shared<configuration_balancer_request>(std::move(result));
}

// assume all nodes are alive
void greedy_load_balancer::copy_primary_per_app(const std::shared_ptr<app_state> &app,
                                                bool still_have_less_than_average,
                                                int replicas_low)
{
    const node_mapper &nodes = *(t_global_view->nodes);
    std::vector<int> future_primaries(address_vec.size(), 0);
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        future_primaries[address_id[iter->first]] = iter->second.primary_count(app->app_id);
    }
    std::set<int, std::function<bool(int a, int b)>> pri_queue([&future_primaries](int a, int b) {
        return future_primaries[a] != future_primaries[b]
                   ? future_primaries[a] < future_primaries[b]
                   : a < b;
    });
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        pri_queue.insert(address_id[iter->first]);
    }

    while (true) {
        int id_min = *pri_queue.begin();
        int id_max = *pri_queue.rbegin();

        if (still_have_less_than_average && future_primaries[id_min] >= replicas_low)
            break;
        if (!still_have_less_than_average &&
            future_primaries[id_max] - future_primaries[id_min] <= 1)
            break;

        dinfo("server with min/max load: %s have %d/%s have %d",
              address_vec[id_min].to_string(),
              future_primaries[id_min],
              address_vec[id_max].to_string(),
              future_primaries[id_max]);

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        dassert(id_min != id_max, "min load and max load machines shouldn't the same");

        const node_state &ns = nodes.find(address_vec[id_max])->second;
        const partition_set *pri = ns.partitions(app->app_id, true);
        dassert(pri != nullptr && !pri->empty(), "max load shouldn't empty");

        int original_size = t_migration_result->size();
        for (const gpid &pid : *pri) {
            if (t_migration_result->find(pid) == t_migration_result->end()) {
                const partition_configuration &pc = app->partitions[pid.get_partition_index()];
                dassert(!is_member(pc, address_vec[id_min]),
                        "gpid(%d.%d) shouldn't be copy from %s to %s",
                        pc.pid.get_app_id(),
                        pc.pid.get_partition_index(),
                        address_vec[id_max].to_string(),
                        address_vec[id_min].to_string());
                t_migration_result->emplace(
                    pid,
                    generate_balancer_request(
                        pc, balance_type::copy_primary, address_vec[id_max], address_vec[id_min]));

                future_primaries[id_max]--;
                future_primaries[id_min]++;

                break;
            }
        }

        dassert(t_migration_result->size() > original_size,
                "can't find primry to copy from(%s) to(%s)",
                address_vec[id_max].to_string(),
                address_vec[id_min].to_string());
        pri_queue.insert(id_max);
        pri_queue.insert(id_min);
    }
}

void greedy_load_balancer::copy_secondary_per_app(const std::shared_ptr<app_state> &app)
{
    ddebug("greedy copy secondary for app(name:%s, id:%d)", app->app_name.c_str(), app->app_id);

    std::vector<int> future_partitions(address_vec.size(), 0);
    int total_partitions = 0;
    for (const auto &pair : *(t_global_view->nodes)) {
        const node_state &ns = pair.second;
        future_partitions[address_id[ns.addr()]] = ns.partition_count(app->app_id);
        total_partitions += ns.partition_count(app->app_id);
    }
    std::set<int, std::function<bool(int, int)>> pri_queue([&future_partitions](int id1, int id2) {
        if (future_partitions[id1] != future_partitions[id2])
            return future_partitions[id1] < future_partitions[id2];
        return id1 < id2;
    });
    for (const auto &kv : address_id) {
        pri_queue.emplace(kv.second);
    }

    int replicas_low = total_partitions / t_alive_nodes;
    while (true) {
        int min_id = *pri_queue.begin(), max_id = *pri_queue.rbegin();
        dassert(future_partitions[min_id] <= replicas_low,
                "%d VS %d",
                future_partitions[min_id],
                replicas_low);
        if (future_partitions[max_id] - future_partitions[min_id] <= 1)
            break;

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        const node_state &min_ns = (*(t_global_view->nodes))[address_vec[min_id]];
        const node_state &max_ns = (*(t_global_view->nodes))[address_vec[max_id]];
        const partition_set *all_partitions_max_load = max_ns.partitions(app->app_id, false);

        // TODO: a better secondary balancer policy
        dinfo("server with min/max load: %s have %d/%s have %d",
              address_vec[min_id].to_string(),
              future_partitions[min_id],
              address_vec[max_id].to_string(),
              future_partitions[max_id]);

        bool found_partition_to_move = false;
        for (const gpid &pid : *all_partitions_max_load) {
            if (max_ns.served_as(pid) == partition_status::PS_PRIMARY)
                continue;
            // if the pid have been used
            if (t_migration_result->find(pid) != t_migration_result->end())
                continue;
            if (min_ns.served_as(pid) == partition_status::PS_INACTIVE) {
                found_partition_to_move = true;
                const partition_configuration &pc = app->partitions[pid.get_partition_index()];
                t_migration_result->emplace(
                    pid,
                    generate_balancer_request(
                        pc, balance_type::copy_secondary, max_ns.addr(), min_ns.addr()));
                dinfo("cp secondary(%d.%d) from %s to %s",
                      pid.get_app_id(),
                      pid.get_partition_index(),
                      max_ns.addr().to_string(),
                      min_ns.addr().to_string());

                ++future_partitions[min_id];
                --future_partitions[max_id];
                break;
            }
        }

        if (!found_partition_to_move) {
            dinfo("can't find partition to move from %s to %s",
                  max_ns.addr().to_string(),
                  min_ns.addr().to_string());
            break;
        }
        // we don't put the max back, as we only allow at most one replica to flow out concurrently
        pri_queue.insert(min_id);
        pri_queue.insert(max_id);
    }
}

void greedy_load_balancer::number_nodes(const node_mapper &nodes)
{
    int current_id = 1;

    address_id.clear();
    address_vec.resize(t_alive_nodes + 2);
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        dassert(!iter->first.is_invalid() && !iter->second.addr().is_invalid(), "invalid address");
        dassert(iter->second.alive(), "dead node");

        address_id[iter->first] = current_id;
        address_vec[current_id] = iter->first;
        ++current_id;
    }
}

// shortest path based on dijstra, to find an augmenting path
void greedy_load_balancer::make_balancer_decision_based_on_flow(
    const std::shared_ptr<app_state> &app,
    const std::vector<int> &prev,
    const std::vector<int> &flow)
{
    int graph_nodes = prev.size();
    int current = prev[graph_nodes - 1];
    while (prev[current] != 0) {
        rpc_address from = address_vec[prev[current]];
        rpc_address to = address_vec[current];
        int primaries_need_to_move = flow[graph_nodes - 1];

        const node_state &ns = t_global_view->nodes->find(from)->second;
        ns.for_each_primary(app->app_id, [&, this](const gpid &pid) {
            if (0 == primaries_need_to_move)
                return false;
            const partition_configuration &pc = app->partitions[pid.get_partition_index()];
            for (const rpc_address &addr : pc.secondaries) {
                if (addr == to) {
                    dinfo("plan to move primary, gpid(%d.%d), from(%s), to(%s)",
                          pc.pid.get_app_id(),
                          pc.pid.get_partition_index(),
                          from.to_string(),
                          to.to_string());
                    t_migration_result->emplace(
                        pid, generate_balancer_request(pc, balance_type::move_primary, from, to));
                    --primaries_need_to_move;
                    break;
                }
            }
            return true;
        });
        dassert(primaries_need_to_move == 0, "not enough primary in %s to move", from.to_string());
        current = prev[current];
    }
}

void greedy_load_balancer::shortest_path(std::vector<bool> &visit,
                                         std::vector<int> &flow,
                                         std::vector<int> &prev,
                                         std::vector<std::vector<int>> &network)
{
    int pos, max_value;
    flow[0] = INT_MAX;

    int graph_nodes = network.size();
    while (!visit[graph_nodes - 1]) {
        pos = -1, max_value = 0;
        for (int i = 0; i != graph_nodes; ++i) {
            if (visit[i] == false && flow[i] > max_value) {
                pos = i;
                max_value = flow[i];
            }
        }

        if (pos == -1)
            break;

        visit[pos] = true;
        for (int i = 0; i != graph_nodes; ++i) {
            if (!visit[i] && std::min(flow[pos], network[pos][i]) > flow[i]) {
                flow[i] = std::min(flow[pos], network[pos][i]);
                prev[i] = pos;
            }
        }
    }
}

// load balancer based on ford-fulkerson
void greedy_load_balancer::primary_balancer_per_app(const std::shared_ptr<app_state> &app)
{
    dassert(t_alive_nodes > 2, "too few alive nodes will lead to freeze");
    ddebug("primary balancer for app(%s:%d)", app->app_name.c_str(), app->app_id);

    const node_mapper &nodes = *(t_global_view->nodes);
    int replicas_low = app->partition_count / t_alive_nodes;
    int replicas_high = (app->partition_count + t_alive_nodes - 1) / t_alive_nodes;

    int lower_count = 0, higher_count = 0;
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        int c = iter->second.primary_count(app->app_id);
        if (c > replicas_high)
            higher_count++;
        else if (c < replicas_low)
            lower_count++;
    }
    if (higher_count == 0 && lower_count == 0) {
        dinfo("the primaries are balanced for app(%s:%d)", app->app_name.c_str(), app->app_id);
        return;
    }

    size_t graph_nodes = t_alive_nodes + 2;
    std::vector<std::vector<int>> network(graph_nodes, std::vector<int>(graph_nodes, 0));

    // make graph
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        int from = address_id[iter->first];
        const node_state &ns = iter->second;
        int c = ns.primary_count(app->app_id);
        if (c > replicas_low)
            network[0][from] = c - replicas_low;
        else
            network[from][graph_nodes - 1] = replicas_low - c;

        ns.for_each_primary(app->app_id, [&, this](const gpid &pid) {
            const partition_configuration &pc = app->partitions[pid.get_partition_index()];
            for (auto &target : pc.secondaries) {
                auto i = address_id.find(target);
                dassert(i != address_id.end(),
                        "invalid secondary address, address = %s",
                        target.to_string());
                network[from][i->second]++;
            }
            return true;
        });
    }

    if (higher_count > 0 && lower_count == 0) {
        for (int i = 0; i != graph_nodes; ++i) {
            if (network[0][i] > 0)
                --network[0][i];
            else
                ++network[i][graph_nodes - 1];
        }
    }
    dinfo("start to move primary");
    std::vector<bool> visit(graph_nodes, false);
    std::vector<int> flow(graph_nodes, 0);
    std::vector<int> prev(graph_nodes, -1);

    shortest_path(visit, flow, prev, network);
    // we can't make the server load more balanced
    // by moving primaries to secondaries
    if (!visit[graph_nodes - 1] || flow[graph_nodes - 1] == 0) {
        copy_primary_per_app(app, lower_count != 0, replicas_low);
        return;
    }

    dinfo("%u primaries are flew", flow[graph_nodes - 1]);
    make_balancer_decision_based_on_flow(app, prev, flow);
}

void greedy_load_balancer::primary_balancer_globally()
{
    ddebug("greedy primary balancer globally");
    std::vector<int> primary_count(address_vec.size(), 0);
    dsn::replication::for_each_available_app(
        *t_global_view->apps, [&, this](const std::shared_ptr<app_state> &app) {
            int max_value = -1, min_value = std::numeric_limits<int>::max();
            for (const auto &np : *(t_global_view->nodes)) {
                int id = address_id[np.first];
                primary_count[id] += np.second.primary_count(app->app_id);
                max_value = std::max(max_value, primary_count[id]);
                min_value = std::min(min_value, primary_count[id]);
            }
            dassert(max_value - min_value <= 2,
                    "invalid max(%d), min(%d), current_app(%d)",
                    max_value,
                    min_value,
                    app->app_id);
            if (max_value - min_value <= 1)
                return true;

            // BFS to find a path from the max_value set to the min_value_set
            std::vector<std::set<dsn::gpid>> used_primaries(address_vec.size());
            for (int i = 1; i <= t_alive_nodes; ++i) {
                if (primary_count[i] != max_value)
                    continue;

                std::vector<dsn::gpid> bfs_prev(address_vec.size(), dsn::gpid(-1, -1));
                std::queue<int> bfs_q;
                bfs_q.push(i);
                bfs_prev[i] = {0, 0};

                while (!bfs_q.empty()) {
                    int id = bfs_q.front();
                    if (primary_count[id] == min_value)
                        break;

                    bfs_q.pop();
                    const node_state &ns = (*t_global_view->nodes)[address_vec[id]];
                    const partition_set *pri_set = ns.partitions(app->app_id, true);
                    if (pri_set == nullptr)
                        continue;
                    for (const gpid &pid : *pri_set) {
                        if (used_primaries[id].find(pid) != used_primaries[id].end())
                            continue;
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        dassert(pc.primary == address_vec[id],
                                "invalid primary address, %s VS %s",
                                pc.primary.to_string(),
                                address_vec[id].to_string());
                        for (const dsn::rpc_address &addr : pc.secondaries) {
                            int id2 = address_id[addr];
                            if (bfs_prev[id2].get_app_id() == -1) {
                                bfs_prev[id2] = pid;
                                bfs_q.push(id2);
                            }
                        }
                    }
                }

                if (!bfs_q.empty()) {
                    int id = bfs_q.front();
                    primary_count[id]++;
                    primary_count[i]--;

                    while (bfs_prev[id].get_app_id() != 0) {
                        dsn::gpid pid = bfs_prev[id];
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        t_migration_result->emplace(
                            pid,
                            generate_balancer_request(
                                pc, balance_type::move_primary, pc.primary, address_vec[id]));
                        // jump to prev node
                        id = address_id[pc.primary];
                        used_primaries[id].emplace(pid);
                    }

                    dassert(id == i, "expect first node(%d), actual (%d)", i, id);
                }
            }

            if (t_migration_result->empty()) {
                std::vector<int> node_id(t_alive_nodes);
                for (int i = 0; i < t_alive_nodes; ++i)
                    node_id[i] = i + 1;
                std::sort(node_id.begin(), node_id.end(), [&primary_count](int id1, int id2) {
                    return primary_count[id1] != primary_count[id2]
                               ? primary_count[id1] < primary_count[id2]
                               : id1 < id2;
                });
                int i = 0, j = t_alive_nodes - 1;
                while (i < j && primary_count[node_id[j]] - primary_count[node_id[i]] == 2) {
                    const node_state &ns_max = (*t_global_view->nodes)[address_vec[node_id[j]]];
                    const node_state &ns_min = (*t_global_view->nodes)[address_vec[node_id[i]]];
                    ns_max.for_each_primary(app->app_id, [&](const dsn::gpid &pid) {
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        dassert(!is_member(pc, ns_min.addr()),
                                "should be moved from(%s) to(%s) gpid(%d.%d)",
                                ns_max.addr().to_string(),
                                ns_min.addr().to_string(),
                                pid.get_app_id(),
                                pid.get_partition_index());
                        t_migration_result->emplace(
                            pid,
                            generate_balancer_request(
                                pc, balance_type::copy_primary, ns_max.addr(), ns_min.addr()));
                        return false;
                    });
                    ++i;
                    --j;
                }
            }
            return false;
        });
}

void greedy_load_balancer::greedy_balancer()
{
    const app_mapper &apps = *t_global_view->apps;

    dassert(t_alive_nodes > 2, "too few nodes will be freezed");
    number_nodes(*t_global_view->nodes);

    dsn::replication::for_each_available_app(apps, [this](const std::shared_ptr<app_state> &app) {
        this->primary_balancer_per_app(app);
        return true;
    });
    if (!t_migration_result->empty())
        return;

    primary_balancer_globally();
    if (!t_migration_result->empty())
        return;

    dsn::replication::for_each_available_app(apps, [this](const std::shared_ptr<app_state> &app) {
        this->copy_secondary_per_app(app);
        return true;
    });
}

bool greedy_load_balancer::balance(meta_view view, migration_list &list)
{
    ddebug("balancer round");
    list.clear();

    t_total_partitions = count_partitions(*(view.apps));
    t_alive_nodes = view.nodes->size();
    t_global_view = &view;
    t_migration_result = &list;
    t_migration_result->clear();

    greedy_balancer();
    return !t_migration_result->empty();
}
}
}
