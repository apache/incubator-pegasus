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
 *     the meta server's date structure, impl file
 *
 * Revision history:
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#include <dsn/service_api_cpp.h>
#include "meta_data.h"

namespace dsn { namespace replication {

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add)
{
    auto it = std::find(drops.begin(), drops.end(), node);
    if (is_add)
    {
        if (it != drops.end())
            drops.erase(it);
    }
    else
    {
        if (it == drops.end())
        {
            drops.push_back(node);
            if (drops.size() > 3)
            {
                drops.erase(drops.begin());
            }
        }
        else
        {
            dassert(false, "the node %s cannot be in drops set before this update", node.to_string());
        }
    }
}

void config_context::cancel_sync()
{
    if (config_status::pending_remote_sync == stage)
    {
        pending_sync_task->cancel(false);
        pending_sync_task = nullptr;
        pending_sync_request.reset();
    }
    if (msg)
    {
        dsn_msg_release_ref(msg);
    }
    msg = nullptr;
    stage = config_status::not_pending;
}

void config_context::clear_proposal()
{
    if (config_status::pending_remote_sync != stage)
        stage = config_status::not_pending;
    balancer_proposal->action_list.clear();
}

void app_state_helper::on_init_partitions()
{
    config_context context;
    context.stage = config_status::not_pending;
    context.pending_sync_task = nullptr;
    context.msg = nullptr;

    context.is_cure_proposal = false;
    contexts.assign(owner->partition_count, context);
    for (unsigned int i=0; i!=owner->partition_count; ++i)
    {
        contexts[i].balancer_proposal = std::make_shared<configuration_balancer_request>();
    }
}

app_state::app_state(const app_info &info): app_info(info), helpers(new app_state_helper())
{
    helpers->owner = this;

    partition_configuration config;
    config.ballot = 0;
    config.pid.set_app_id(app_id);
    config.last_committed_decree = 0;
    config.last_drops.clear();
    config.max_replica_count = app_info::max_replica_count;
    config.primary.set_invalid();
    config.secondaries.clear();
    partitions.assign(app_info::partition_count, config);
    for (int i=0; i!=app_info::partition_count; ++i)
        partitions[i].pid.set_partition_index(i);

    helpers->on_init_partitions();
}

std::shared_ptr<app_state> app_state::create(const app_info& info)
{
    std::shared_ptr<app_state> result = std::make_shared<app_state>(info);
    result->helpers->owner = result.get();
    return result;
}

node_state::node_state():
    total_primaries(0),
    total_partitions(0),
    is_alive(false)
{
}

bool node_state::for_each_partition(const std::function<bool (const gpid &)> &f) const
{
    for (const auto& pair: app_partitions)
    {
        const partition_set& ps = pair.second;
        for (const auto& gpid: ps)
        {
            if (!f(gpid))
                return false;
        }
    }
    return true;
}

const partition_set* node_state::get_partitions(int app_id, bool only_primary) const
{
    const std::map<int32_t, partition_set>* all_partitions;
    if (only_primary)
        all_partitions = &app_primaries;
    else
        all_partitions = &app_partitions;

    auto iter = all_partitions->find(app_id);
    if (iter == all_partitions->end())
        return nullptr;
    else
        return &(iter->second);
}

partition_set* node_state::get_partitions(app_id id, bool only_primary, bool create_new)
{
    std::map<int32_t, partition_set>* all_partitions;
    if (only_primary)
        all_partitions = &app_primaries;
    else
        all_partitions = &app_partitions;

    if (create_new)
    {
        return &((*all_partitions)[id]);
    }
    else
    {
        auto iter = all_partitions->find(id);
        if (iter == all_partitions->end())
            return nullptr;
        else
            return &(iter->second);
    }
}

partition_set* node_state::partitions(app_id id, bool only_primary)
{
    return const_cast<partition_set*>(get_partitions(id, only_primary));
}

const partition_set* node_state::partitions(app_id id, bool only_primary) const
{
    return get_partitions(id, only_primary);
}

void node_state::put_partition(const gpid &pid, bool is_primary)
{
    partition_set* all = get_partitions(pid.get_app_id(), false, true);
    if ((all->insert(pid)).second)
        total_partitions++;
    if (is_primary)
    {
        partition_set* pri = get_partitions(pid.get_app_id(), true, true);
        if ((pri->insert(pid)).second)
            total_primaries++;
    }
}

void node_state::remove_partition(const gpid &pid, bool only_primary)
{
    partition_set* pri = get_partitions(pid.get_app_id(), true, true);
    total_primaries -= pri->erase(pid);
    if (!only_primary)
    {
        partition_set* all = get_partitions(pid.get_app_id(), false, true);
        total_partitions -= all->erase(pid);
    }
}

bool node_state::for_each_primary(app_id id, const std::function<bool (const gpid &)> &f) const
{
    const partition_set* pri = partitions(id, true);
    if (pri == nullptr)
        return false;
    for (const gpid& pid: *pri)
    {
        dassert(id == pid.get_app_id(), "");
        if (!f(pid))
            return false;
    }
    return true;
}

unsigned node_state::primary_count(app_id id) const
{
    const partition_set* pri = partitions(id, true);
    if (pri == nullptr)
        return 0;
    return pri->size();
}

unsigned node_state::partition_count(app_id id) const
{
    const partition_set* pri = partitions(id, false);
    if (pri == nullptr)
        return 0;
    return pri->size();
}

partition_status::type node_state::served_as(const gpid &pid) const
{
    const partition_set* ps1 = partitions(pid.get_app_id(), true);
    if (ps1 != nullptr && ps1->find(pid) != ps1->end())
        return partition_status::PS_PRIMARY;
    const partition_set* ps2 = partitions(pid.get_app_id(), false);
    if (ps2 != nullptr && ps2->find(pid) != ps2->end())
        return partition_status::PS_SECONDARY;
    return partition_status::PS_INACTIVE;
}

}}
