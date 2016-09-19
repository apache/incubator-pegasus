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
 *     the meta server's date structure
 *
 * Revision history:
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#pragma once

#include <memory>
#include <set>
#include <deque>
#include <vector>
#include <map>
#include <unordered_map>
#include <functional>
#include <dsn/utility/utils.h>
#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/cpp/json_helper.h>

namespace dsn { namespace replication {

enum class config_status
{
    not_pending,
    pending_proposal,
    pending_remote_sync,
    invalid_status
};

ENUM_BEGIN(config_status, config_status::invalid_status)
    ENUM_REG(config_status::not_pending)
    ENUM_REG(config_status::pending_proposal)
    ENUM_REG(config_status::pending_remote_sync)
ENUM_END(config_status)

enum class pc_status
{
    healthy,
    ill,
    dead,
    invalid
};

ENUM_BEGIN(pc_status, pc_status::invalid)
    ENUM_REG(pc_status::healthy)
    ENUM_REG(pc_status::ill)
    ENUM_REG(pc_status::dead)
ENUM_END(pc_status)

struct dropped_server
{
    rpc_address node;
    uint64_t time;
};

struct config_context
{
public:
    config_status stage;
    //for server state's update config management
    //[
    task_ptr pending_sync_task;
    std::shared_ptr<configuration_update_request> pending_sync_request;
    dsn_message_t msg;
    //]

    //for load balancer's decision
    //[
    std::shared_ptr<configuration_balancer_request> balancer_proposal;
    std::deque<dropped_server> history;
    //]
public:
    bool empty_balancer_proposals() const { return balancer_proposal==nullptr || balancer_proposal->action_list.empty(); }
    void cancel_sync();
    void clear_proposal();
};

struct partition_configuration_stateless
{
    partition_configuration& config;
    partition_configuration_stateless(partition_configuration& pc): config(pc) {}
    std::vector<dsn::rpc_address>& workers() { return config.last_drops; }
    std::vector<dsn::rpc_address>& hosts() { return config.secondaries; }
    bool is_host(const rpc_address& node) const
    {
        return std::find(config.secondaries.begin(), config.secondaries.end(), node)!=config.secondaries.end();
    }
    bool is_worker(const rpc_address& node) const
    {
        return std::find(config.last_drops.begin(), config.last_drops.end(), node)!=config.last_drops.end();
    }
    bool is_member(const rpc_address& node) const
    {
        return is_host(node) || is_worker(node);
    }
};

class app_state;
class app_state_helper
{
public:
    app_state* owner;
    std::atomic_int available_partitions;
    std::vector<config_context> contexts;
public:
    app_state_helper(): owner(nullptr), available_partitions(0)
    {
        contexts.clear();
    }
    void on_init_partitions();
    void clear_proposals()
    {
        for (config_context& cc: contexts)
            cc.clear_proposal();
    }
};

class app_state: public app_info
{
public:
    app_state(const app_info& info);
public:
    std::shared_ptr<app_state_helper>    helpers;
    std::vector<partition_configuration> partitions;
    static std::shared_ptr<app_state> create(const app_info& info);
    dsn::blob encode_json_with_status(app_status::type temp_status)
    {
        std::swap(status, temp_status);
        //we use the app_info's encode
        dsn::blob result = dsn::json::json_forwarder<app_state>::encode(*this);
        std::swap(status, temp_status);
        return result;
    }
};

typedef std::set<dsn::gpid> partition_set;
typedef std::map<app_id, std::shared_ptr<app_state>> app_mapper;

class node_state
{
private:
    //partitions
    std::map<int32_t, partition_set > app_primaries;
    std::map<int32_t, partition_set > app_partitions;
    unsigned total_primaries;
    unsigned total_partitions;

    //status
    bool is_alive;
    dsn::rpc_address address;

    const partition_set* get_partitions(app_id id, bool only_primary) const;
    partition_set* get_partitions(app_id id, bool only_primary, bool create_new);

public:
    const partition_set* partitions(app_id id, bool only_primary) const;
    partition_set* partitions(app_id id, bool only_primary);

    unsigned primary_count(app_id id) const;
    unsigned secondary_count(app_id id) const { return partition_count(id) - primary_count(id); }
    unsigned partition_count(app_id id) const;

    unsigned primary_count() const { return total_primaries; }
    unsigned secondary_count() const { return total_partitions - total_primaries; }
    unsigned partition_count() const { return total_partitions; }

    partition_status::type served_as(const gpid& pid) const;

    bool alive() const { return is_alive; }
    void set_alive(bool alive) { is_alive = alive; }
    dsn::rpc_address addr() const { return address; }
    void set_addr(const dsn::rpc_address& addr) { address = addr; }

    void put_partition(const dsn::gpid& pid, bool is_primary);
    void remove_partition(const dsn::gpid& pid, bool only_primary);

    bool for_each_partition(const std::function<bool (const dsn::gpid& pid)>& f) const;
    bool for_each_primary(app_id id, const std::function<bool (const dsn::gpid& pid)>& f) const;

    //this is partition count with newly add replicas
    unsigned newly_primary_count(app_id id) const;
    unsigned newly_secondary_count(app_id id) const;
    unsigned newly_partition_count(app_id id) const;
};

typedef std::unordered_map<rpc_address, node_state> node_mapper;
typedef std::map<dsn::gpid, std::shared_ptr<configuration_balancer_request> > migration_list;

struct meta_view
{
    app_mapper* apps;
    node_mapper* nodes;
};

inline node_state* get_node_state(node_mapper& nodes, rpc_address addr, bool create_new)
{
    node_state* ns;
    if (nodes.find(addr) == nodes.end())
    {
        if (!create_new)
            return nullptr;
        ns = &nodes[addr];
        ns->set_addr(addr);
    }
    ns = &nodes[addr];
    return ns;
}

inline bool is_node_alive(const node_mapper& nodes, rpc_address addr)
{
    auto iter = nodes.find(addr);
    if (iter == nodes.end())
        return false;
    return iter->second.alive();
}

inline const partition_configuration* get_config(const app_mapper& apps, const dsn::gpid& gpid)
{
    auto iter = apps.find(gpid.get_app_id());
    if (iter==apps.end() || iter->second->status==app_status::AS_DROPPED)
        return nullptr;
    return &(iter->second->partitions[gpid.get_partition_index()]);
}

inline const config_context* get_config_context(const app_mapper& apps, const dsn::gpid& gpid)
{
    auto iter = apps.find(gpid.get_app_id());
    if (iter == apps.end() || iter->second->status==app_status::AS_DROPPED)
        return nullptr;
    return &(iter->second->helpers->contexts[gpid.get_partition_index()]);
}

inline void for_each_available_app(const app_mapper& apps, const std::function<bool (const std::shared_ptr<app_state>&)>& action)
{
    for (const auto& p: apps) {
        if (p.second->status == app_status::AS_AVAILABLE) {
            if (!action(p.second))
                break;
        }
    }
}

inline int count_partitions(const app_mapper& apps)
{
    int result = 0;
    for (auto iter: apps)
        if (iter.second->status == app_status::AS_AVAILABLE)
            result += iter.second->partition_count;
    return result;
}

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add);

}}

namespace dsn { namespace json {

inline void json_encode(std::stringstream &out, const replication::app_state& state)
{
    json_forwarder<dsn::app_info>::encode(out, (const dsn::app_info&)state);
}

inline void json_decode(dsn::json::string_tokenizer &in, replication::app_state& state)
{
    json_forwarder<dsn::app_info>::decode(in, (dsn::app_info&)state);
}

}}
