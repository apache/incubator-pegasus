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
#include <boost/lexical_cast.hpp>
#include <dsn/service_api_cpp.h>
#include "dist/replication/meta_server/meta_data.h"

namespace dsn {
namespace replication {

void when_update_replicas(config_type::type t, const std::function<void(bool)> &func)
{
    switch (t) {
    case config_type::CT_ASSIGN_PRIMARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
    case config_type::CT_UPGRADE_TO_SECONDARY:
        func(true);
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
    case config_type::CT_DROP_PARTITION:
        func(false);
        break;
    default:
        break;
    }
}

void maintain_drops(std::vector<rpc_address> &drops, const rpc_address &node, config_type::type t)
{
    auto action = [&drops, &node](bool is_adding) {
        auto it = std::find(drops.begin(), drops.end(), node);
        if (is_adding) {
            if (it != drops.end()) {
                drops.erase(it);
            }
        } else {
            dassert(it == drops.end(),
                    "the node(%s) cannot be in drops set before this update",
                    node.to_string());
            drops.push_back(node);
            if (drops.size() > 3) {
                drops.erase(drops.begin());
            }
        }
    };
    when_update_replicas(t, action);
}

proposal_actions::proposal_actions() : from_balancer(false) { reset_tracked_current_learner(); }

void proposal_actions::reset_tracked_current_learner()
{
    learning_progress_abnormal_detected = false;
    current_learner.ballot = invalid_ballot;
    current_learner.last_durable_decree = invalid_decree;
    current_learner.last_committed_decree = invalid_decree;
    current_learner.last_prepared_decree = invalid_decree;
}

void proposal_actions::track_current_learner(const dsn::rpc_address &node, const replica_info &info)
{
    if (empty())
        return;
    configuration_proposal_action &act = acts.front();
    if (act.node != node)
        return;

    // currently we only handle add secondary
    // TODO: adjust other proposals according to replica info collected
    if (act.type == config_type::CT_ADD_SECONDARY ||
        act.type == config_type::CT_ADD_SECONDARY_FOR_LB) {

        if (info.status == partition_status::PS_ERROR ||
            info.status == partition_status::PS_INACTIVE) {
            // if we've collected inforamtions for the learner, then it claims it's down
            // we will treat the learning process failed
            if (current_learner.ballot != invalid_ballot) {
                ddebug("%d.%d: a learner's is down to status(%s), perhaps learn failed",
                       info.pid.get_app_id(),
                       info.pid.get_partition_index(),
                       dsn::enum_to_string(info.status));
                learning_progress_abnormal_detected = true;
            } else {
                dinfo("%d.%d: ignore abnormal status of %s, perhaps learn not start",
                      info.pid.get_app_id(),
                      info.pid.get_partition_index(),
                      node.to_string());
            }
        } else if (info.status == partition_status::PS_POTENTIAL_SECONDARY) {
            if (current_learner.ballot > info.ballot ||
                current_learner.last_committed_decree > info.last_committed_decree ||
                current_learner.last_prepared_decree > info.last_prepared_decree) {

                // TODO: need to add a perf counter here
                dwarn("%d.%d: learner(%s)'s progress step back, please trace this carefully",
                      info.pid.get_app_id(),
                      info.pid.get_partition_index(),
                      node.to_string());
            }

            // NOTICE: the flag may be abormal currently. it's balancer's duty to make use of the
            // abnormal flag and decide whether to cancel the proposal.
            // if the balancer try to give the proposal another chance, or another learning round
            // starts before the balancer notice it, let's just treat it normal again.
            learning_progress_abnormal_detected = false;
            current_learner = info;
        }
    }
}

bool proposal_actions::is_abnormal_learning_proposal() const
{
    if (empty())
        return false;
    if (front()->type != config_type::CT_ADD_SECONDARY &&
        front()->type != config_type::CT_ADD_SECONDARY_FOR_LB)
        return false;
    return learning_progress_abnormal_detected;
}

void proposal_actions::clear()
{
    from_balancer = false;
    acts.clear();
    reset_tracked_current_learner();
}

void proposal_actions::pop_front()
{
    if (!acts.empty()) {
        acts.erase(acts.begin());
        reset_tracked_current_learner();
    }
}

const configuration_proposal_action *proposal_actions::front() const
{
    if (acts.empty())
        return nullptr;
    return &acts.front();
}

void proposal_actions::assign_cure_proposal(const configuration_proposal_action &act)
{
    from_balancer = false;
    acts = {act};
    reset_tracked_current_learner();
}

void proposal_actions::assign_balancer_proposals(
    const std::vector<configuration_proposal_action> &cpa_list)
{
    from_balancer = true;
    acts = cpa_list;
    reset_tracked_current_learner();
}

bool proposal_actions::empty() const { return acts.empty(); }

int config_context::MAX_REPLICA_COUNT_IN_GRROUP = 4;
void config_context::cancel_sync()
{
    if (config_status::pending_remote_sync == stage) {
        pending_sync_task->cancel(false);
        pending_sync_task = nullptr;
        pending_sync_request.reset();
    }
    if (msg) {
        msg->release_ref();
    }
    msg = nullptr;
    stage = config_status::not_pending;
}

void config_context::check_size()
{
    // when add learner, it is possible that replica_count > max_replica_count, so we
    // need to remove things from dropped only when it's not empty.
    while (replica_count(*config_owner) + dropped.size() > MAX_REPLICA_COUNT_IN_GRROUP &&
           !dropped.empty()) {
        dropped.erase(dropped.begin());
        prefered_dropped = (int)dropped.size() - 1;
    }
}

std::vector<dropped_replica>::iterator config_context::find_from_dropped(const rpc_address &node)
{
    return std::find_if(dropped.begin(), dropped.end(), [&node](const dropped_replica &r) {
        return r.node == node;
    });
}

std::vector<dropped_replica>::const_iterator
config_context::find_from_dropped(const rpc_address &node) const
{
    return std::find_if(dropped.begin(), dropped.end(), [&node](const dropped_replica &r) {
        return r.node == node;
    });
}

bool config_context::remove_from_dropped(const rpc_address &node)
{
    auto iter = find_from_dropped(node);
    if (iter != dropped.end()) {
        dropped.erase(iter);
        prefered_dropped = (int)dropped.size() - 1;
        return true;
    }
    return false;
}

bool config_context::record_drop_history(const rpc_address &node)
{
    auto iter = find_from_dropped(node);
    if (iter != dropped.end())
        return false;
    dropped.emplace_back(
        dropped_replica{node, dsn_now_ms(), invalid_ballot, invalid_decree, invalid_decree});
    prefered_dropped = (int)dropped.size() - 1;
    check_size();
    return true;
}

int config_context::collect_drop_replica(const rpc_address &node, const replica_info &info)
{
    bool in_dropped = false;
    auto iter = find_from_dropped(node);
    uint64_t last_drop_time = dropped_replica::INVALID_TIMESTAMP;
    if (iter != dropped.end()) {
        in_dropped = true;
        last_drop_time = iter->time;
        dropped.erase(iter);
        prefered_dropped = (int)dropped.size() - 1;
    }

    dropped_replica current = {
        node, last_drop_time, info.ballot, info.last_committed_decree, info.last_prepared_decree};
    auto cmp = [](const dropped_replica &d1, const dropped_replica &d2) {
        return dropped_cmp(d1, d2) < 0;
    };
    iter = std::lower_bound(dropped.begin(), dropped.end(), current, cmp);

    dropped.emplace(iter, current);
    prefered_dropped = (int)dropped.size() - 1;
    check_size();

    iter = find_from_dropped(node);
    if (iter == dropped.end()) {
        dassert(!in_dropped,
                "adjust position of existing node(%s) failed, this is a bug, partition(%d.%d)",
                node.to_string(),
                config_owner->pid.get_app_id(),
                config_owner->pid.get_partition_index());
        return -1;
    }
    return in_dropped ? 1 : 0;
}

bool config_context::check_order()
{
    if (dropped.empty())
        return true;
    for (unsigned int i = 0; i < dropped.size() - 1; ++i) {
        if (dropped_cmp(dropped[i], dropped[i + 1]) > 0) {
            derror("check dropped order for gpid(%d.%d) failed, [%s,%llu,%lld,%lld,%lld@%d] vs "
                   "[%s,%llu,%lld,%lld,%lld@%d]",
                   config_owner->pid.get_app_id(),
                   config_owner->pid.get_partition_index(),
                   dropped[i].node.to_string(),
                   dropped[i].time,
                   dropped[i].ballot,
                   dropped[i].last_committed_decree,
                   dropped[i].last_prepared_decree,
                   i,
                   dropped[i].node.to_string(),
                   dropped[i].time,
                   dropped[i].ballot,
                   dropped[i].last_committed_decree,
                   dropped[i].last_prepared_decree,
                   i + 1);
            return false;
        }
    }
    return true;
}

std::vector<serving_replica>::iterator config_context::find_from_serving(const rpc_address &node)
{
    return std::find_if(serving.begin(), serving.end(), [&node](const serving_replica &r) {
        return r.node == node;
    });
}

std::vector<serving_replica>::const_iterator
config_context::find_from_serving(const rpc_address &node) const
{
    return std::find_if(serving.begin(), serving.end(), [&node](const serving_replica &r) {
        return r.node == node;
    });
}

bool config_context::remove_from_serving(const rpc_address &node)
{
    auto iter = find_from_serving(node);
    if (iter != serving.end()) {
        serving.erase(iter);
        return true;
    }
    return false;
}

void config_context::collect_serving_replica(const rpc_address &node, const replica_info &info)
{
    auto iter = find_from_serving(node);
    if (iter != serving.end()) {
        iter->disk_tag = info.disk_tag;
        iter->storage_mb = 0;
    } else {
        serving.emplace_back(serving_replica{node, 0, info.disk_tag});
    }
}

void config_context::adjust_proposal(const rpc_address &node, const replica_info &info)
{
    lb_actions.track_current_learner(node, info);
}

void app_state_helper::on_init_partitions()
{
    config_context context;
    context.stage = config_status::not_pending;
    context.pending_sync_task = nullptr;
    context.msg = nullptr;

    context.prefered_dropped = -1;
    contexts.assign(owner->partition_count, context);

    std::vector<partition_configuration> &partitions = owner->partitions;
    for (unsigned int i = 0; i != owner->partition_count; ++i) {
        contexts[i].config_owner = &(partitions[i]);
    }

    partitions_in_progress.store(owner->partition_count);
    restore_states.resize(owner->partition_count);
}

app_state::app_state(const app_info &info) : app_info(info), helpers(new app_state_helper())
{
    log_name = info.app_name + "(" + boost::lexical_cast<std::string>(info.app_id) + ")";
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
    for (int i = 0; i != app_info::partition_count; ++i)
        partitions[i].pid.set_partition_index(i);

    helpers->on_init_partitions();
}

std::shared_ptr<app_state> app_state::create(const app_info &info)
{
    return std::make_shared<app_state>(info);
}

node_state::node_state()
    : total_primaries(0), total_partitions(0), is_alive(false), has_collected_replicas(false)
{
}

const partition_set *node_state::get_partitions(int app_id, bool only_primary) const
{
    const std::map<int32_t, partition_set> *all_partitions;
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

partition_set *node_state::get_partitions(app_id id, bool only_primary, bool create_new)
{
    std::map<int32_t, partition_set> *all_partitions;
    if (only_primary)
        all_partitions = &app_primaries;
    else
        all_partitions = &app_partitions;

    if (create_new) {
        return &((*all_partitions)[id]);
    } else {
        auto iter = all_partitions->find(id);
        if (iter == all_partitions->end())
            return nullptr;
        else
            return &(iter->second);
    }
}

partition_set *node_state::partitions(app_id id, bool only_primary)
{
    return const_cast<partition_set *>(get_partitions(id, only_primary));
}

const partition_set *node_state::partitions(app_id id, bool only_primary) const
{
    return get_partitions(id, only_primary);
}

void node_state::put_partition(const gpid &pid, bool is_primary)
{
    partition_set *all = get_partitions(pid.get_app_id(), false, true);
    if ((all->insert(pid)).second)
        total_partitions++;
    if (is_primary) {
        partition_set *pri = get_partitions(pid.get_app_id(), true, true);
        if ((pri->insert(pid)).second)
            total_primaries++;
    }
}

void node_state::remove_partition(const gpid &pid, bool only_primary)
{
    partition_set *pri = get_partitions(pid.get_app_id(), true, true);
    total_primaries -= pri->erase(pid);
    if (!only_primary) {
        partition_set *all = get_partitions(pid.get_app_id(), false, true);
        total_partitions -= all->erase(pid);
    }
}

bool node_state::for_each_primary(app_id id, const std::function<bool(const gpid &)> &f) const
{
    const partition_set *pri = partitions(id, true);
    if (pri == nullptr) {
        return true;
    }
    for (const gpid &pid : *pri) {
        dassert(id == pid.get_app_id(),
                "invalid gpid(%d.%d), app_id must be %d",
                pid.get_app_id(),
                pid.get_partition_index(),
                id);
        if (!f(pid))
            return false;
    }
    return true;
}

bool node_state::for_each_partition(app_id id, const std::function<bool(const gpid &)> &f) const
{
    const partition_set *par = partitions(id, false);
    if (par == nullptr) {
        return true;
    }
    for (const gpid &pid : *par) {
        dassert(id == pid.get_app_id(),
                "invalid gpid(%d.%d), app_id must be %d",
                pid.get_app_id(),
                pid.get_partition_index(),
                id);
        if (!f(pid))
            return false;
    }
    return true;
}

bool node_state::for_each_partition(const std::function<bool(const gpid &)> &f) const
{
    for (const auto &pair : app_partitions) {
        const partition_set &ps = pair.second;
        for (const auto &gpid : ps) {
            if (!f(gpid))
                return false;
        }
    }
    return true;
}

unsigned node_state::primary_count(app_id id) const
{
    const partition_set *pri = partitions(id, true);
    if (pri == nullptr)
        return 0;
    return pri->size();
}

unsigned node_state::partition_count(app_id id) const
{
    const partition_set *pri = partitions(id, false);
    if (pri == nullptr)
        return 0;
    return pri->size();
}

partition_status::type node_state::served_as(const gpid &pid) const
{
    const partition_set *ps1 = partitions(pid.get_app_id(), true);
    if (ps1 != nullptr && ps1->find(pid) != ps1->end())
        return partition_status::PS_PRIMARY;
    const partition_set *ps2 = partitions(pid.get_app_id(), false);
    if (ps2 != nullptr && ps2->find(pid) != ps2->end())
        return partition_status::PS_SECONDARY;
    return partition_status::PS_INACTIVE;
}
}
}
