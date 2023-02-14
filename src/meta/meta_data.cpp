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

#include "utils/fmt_logging.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/flags.h"

#include "meta_data.h"

namespace dsn {
namespace replication {

// There is an option FLAGS_max_replicas_in_group which restricts the max replica count of the whole
// cluster. It's a cluster-level option. However, now that it's allowed to update the replication
// factor of each table, this cluster-level option should be replaced.
//
// Conceptually FLAGS_max_replicas_in_group is the total number of alive and dropped replicas. Its
// default value is 4. For a table that has replication factor 3, that FLAGS_max_replicas_in_group
// is set to 4 means 3 alive replicas plus a dropped replica.
//
// FLAGS_max_replicas_in_group can also be loaded from configuration file, which means its default
// value will be overridden. The value of FLAGS_max_replicas_in_group will be assigned to another
// static variable `MAX_REPLICA_COUNT_IN_GRROUP`, whose default value is also 4.
//
// For unit tests, `MAX_REPLICA_COUNT_IN_GRROUP` is set to the default value 4; for production
// environments, `MAX_REPLICA_COUNT_IN_GRROUP` is set to 3 since FLAGS_max_replicas_in_group is
// configured as 3 in `.ini` file.
//
// Since the cluster-level option FLAGS_max_replicas_in_group contains the alive and dropped
// replicas, we can use the replication factor of each table as the number of alive replicas, and
// introduce another option FLAGS_max_reserved_dropped_replicas representing the max reserved number
// allowed for dropped replicas.
//
// If FLAGS_max_reserved_dropped_replicas is set to 1, there is at most one dropped replicas
// reserved, which means, once the number of alive replicas reaches max_replica_count, at most one
// dropped replica can be reserved and others will be eliminated; If
// FLAGS_max_reserved_dropped_replicas is set to 0, however, none of dropped replicas can be
// reserved.
//
// To be consistent with FLAGS_max_replicas_in_group, default value of
// FLAGS_max_reserved_dropped_replicas is set to 1 so that the unit tests can be passed. For
// production environments, it should be set to 0.
DSN_DEFINE_uint32(meta_server,
                  max_reserved_dropped_replicas,
                  1,
                  "max reserved number allowed for dropped replicas");
DSN_TAG_VARIABLE(max_reserved_dropped_replicas, FT_MUTABLE);

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
            CHECK(
                it == drops.end(), "the node({}) cannot be in drops set before this update", node);
            drops.push_back(node);
            if (drops.size() > 3) {
                drops.erase(drops.begin());
            }
        }
    };
    when_update_replicas(t, action);
}

bool construct_replica(meta_view view, const gpid &pid, int max_replica_count)
{
    partition_configuration &pc = *get_config(*view.apps, pid);
    config_context &cc = *get_config_context(*view.apps, pid);

    CHECK_EQ_MSG(replica_count(pc), 0, "replica count of gpid({}) must be 0", pid);
    CHECK_GT(max_replica_count, 0);

    std::vector<dropped_replica> &drop_list = cc.dropped;
    if (drop_list.empty()) {
        LOG_WARNING("construct for ({}) failed, coz no replicas collected", pid);
        return false;
    }

    // treat last server in drop_list as the primary
    const dropped_replica &server = drop_list.back();
    CHECK_NE_MSG(server.ballot,
                 invalid_ballot,
                 "the ballot of server must not be invalid_ballot, node = {}",
                 server.node);
    pc.primary = server.node;
    pc.ballot = server.ballot;
    pc.partition_flags = 0;
    pc.max_replica_count = max_replica_count;

    LOG_INFO("construct for ({}), select {} as primary, ballot({}), committed_decree({}), "
             "prepare_decree({})",
             pid,
             server.node,
             server.ballot,
             server.last_committed_decree,
             server.last_prepared_decree);

    drop_list.pop_back();

    // we put max_replica_count-1 recent replicas to last_drops, in case of the DDD-state when the
    // only primary dead
    // when add node to pc.last_drops, we don't remove it from our cc.drop_list
    CHECK(pc.last_drops.empty(),
          "last_drops of partition({}.{}) must be empty",
          pid.get_app_id(),
          pid.get_partition_index());
    for (auto iter = drop_list.rbegin(); iter != drop_list.rend(); ++iter) {
        if (pc.last_drops.size() + 1 >= max_replica_count)
            break;
        // similar to cc.drop_list, pc.last_drop is also a stack structure
        pc.last_drops.insert(pc.last_drops.begin(), iter->node);
        LOG_INFO("construct for ({}), select {} into last_drops, ballot({}), "
                 "committed_decree({}), prepare_decree({})",
                 pid,
                 iter->node,
                 iter->ballot,
                 iter->last_committed_decree,
                 iter->last_prepared_decree);
    }

    cc.prefered_dropped = (int)drop_list.size() - 1;
    return true;
}

bool collect_replica(meta_view view, const rpc_address &node, const replica_info &info)
{
    partition_configuration &pc = *get_config(*view.apps, info.pid);
    // current partition is during partition split
    if (pc.ballot == invalid_ballot)
        return false;
    config_context &cc = *get_config_context(*view.apps, info.pid);
    if (is_member(pc, node)) {
        cc.collect_serving_replica(node, info);
        return true;
    }

    // compare current node's replica information with current proposal,
    // and try to find abnormal situations in send proposal
    cc.adjust_proposal(node, info);

    // adjust the drop list
    int ans = cc.collect_drop_replica(node, info);
    CHECK(cc.check_order(), "");

    return info.status == partition_status::PS_POTENTIAL_SECONDARY || ans != -1;
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
                LOG_INFO("{}: a learner's is down to status({}), perhaps learn failed",
                         info.pid,
                         dsn::enum_to_string(info.status));
                learning_progress_abnormal_detected = true;
            } else {
                LOG_DEBUG(
                    "{}: ignore abnormal status of {}, perhaps learn not start", info.pid, node);
            }
        } else if (info.status == partition_status::PS_POTENTIAL_SECONDARY) {
            if (current_learner.ballot > info.ballot ||
                current_learner.last_committed_decree > info.last_committed_decree ||
                current_learner.last_prepared_decree > info.last_prepared_decree) {

                // TODO: need to add a perf counter here
                LOG_WARNING("{}: learner({})'s progress step back, please trace this carefully",
                            info.pid,
                            node);
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
    while (replica_count(*config_owner) + dropped.size() >
               config_owner->max_replica_count + FLAGS_max_reserved_dropped_replicas &&
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
        CHECK(!in_dropped,
              "adjust position of existing node({}) failed, this is a bug, partition({}.{})",
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
            LOG_ERROR("check dropped order for gpid({}) failed, [{},{},{},{},{}@{}] vs "
                      "[{},{},{},{},{}@{}]",
                      config_owner->pid,
                      dropped[i].node,
                      dropped[i].time,
                      dropped[i].ballot,
                      dropped[i].last_committed_decree,
                      dropped[i].last_prepared_decree,
                      i,
                      dropped[i + 1].node,
                      dropped[i + 1].time,
                      dropped[i + 1].ballot,
                      dropped[i + 1].last_committed_decree,
                      dropped[i + 1].last_prepared_decree,
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
    auto compact_status = info.__isset.manual_compact_status ? info.manual_compact_status
                                                             : manual_compaction_status::IDLE;
    if (iter != serving.end()) {
        iter->disk_tag = info.disk_tag;
        iter->storage_mb = 0;
        iter->compact_status = compact_status;
    } else {
        serving.emplace_back(serving_replica{node, 0, info.disk_tag, compact_status});
    }
}

void config_context::adjust_proposal(const rpc_address &node, const replica_info &info)
{
    lb_actions.track_current_learner(node, info);
}

bool config_context::get_disk_tag(const rpc_address &node, /*out*/ std::string &disk_tag) const
{
    auto iter = find_from_serving(node);
    if (iter == serving.end()) {
        return false;
    }
    disk_tag = iter->disk_tag;
    return true;
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

void app_state_helper::reset_manual_compact_status()
{
    for (auto &cc : contexts) {
        for (auto &r : cc.serving) {
            r.compact_status = manual_compaction_status::IDLE;
        }
    }
}

bool app_state_helper::get_manual_compact_progress(/*out*/ int32_t &progress) const
{
    int32_t total_replica_count = owner->partition_count * owner->max_replica_count;
    CHECK_GT_MSG(total_replica_count,
                 0,
                 "invalid app metadata, app({}), partition_count({}), max_replica_count({})",
                 owner->app_name,
                 owner->partition_count,
                 owner->max_replica_count);
    int32_t finish_count = 0, idle_count = 0;
    for (const auto &cc : contexts) {
        for (const auto &r : cc.serving) {
            if (r.compact_status == manual_compaction_status::IDLE) {
                idle_count++;
            } else if (r.compact_status == manual_compaction_status::FINISHED) {
                finish_count++;
            }
        }
    }
    // all replicas of all partitions are idle
    if (idle_count == total_replica_count) {
        progress = 0;
        return false;
    }
    progress = finish_count * 100 / total_replica_count;
    return true;
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
        CHECK_EQ_MSG(id, pid.get_app_id(), "invalid gpid({}), app_id must be {}", pid, id);
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
        CHECK_EQ_MSG(id, pid.get_app_id(), "invalid gpid({}), app_id must be {}", pid, id);
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
} // namespace replication
} // namespace dsn
