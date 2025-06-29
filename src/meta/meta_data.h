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

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "meta/duplication/duplication_info.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"
#include "task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/enum_helper.h"
#include "utils/error_code.h"
#include "utils/extensible_object.h"
#include "utils/fmt_logging.h"
#include "utils/utils.h"

namespace dsn {
class message_ex;

namespace replication {

enum class config_status
{
    not_pending,
    pending_proposal, // deprecated since pegasus v1.8 or older version
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

class pc_flags
{
public:
    static const int dropped = 1;
};

class proposal_actions
{
private:
    bool from_balancer;

    // used for track the learing process and check if abnormal situation happens
    bool learning_progress_abnormal_detected;
    replica_info current_learner;

    // NOTICE:
    // meta servic use configuration_proposal_action::period_ts
    // to store a expire timestamp, but a rpc_sender use this field
    // to suggest a ttl period
    std::vector<configuration_proposal_action> acts;

public:
    proposal_actions();
    void reset_tracked_current_learner();
    void track_current_learner(const host_port &node, const replica_info &info);
    void clear();

    // return the action in acts & whether the action is from balancer
    bool is_from_balancer() const { return from_balancer; }
    bool is_abnormal_learning_proposal() const;

    void pop_front();
    void assign_cure_proposal(const configuration_proposal_action &act);
    void assign_balancer_proposals(const std::vector<configuration_proposal_action> &cpa_list);

    const configuration_proposal_action *front() const;
    bool empty() const;
};

//
// structure "dropped_replica" represents a replica which was downgraded to inactive.
// there are 2 sources to get the dropped replica:
//   1. by record the meta's update-cfg action
//   2. by collect the inactive replicas reported from the replica servers
// generally, we give a partitial order for the dropped_replica, in which a higher order
// roughly means that the replica has MORE data.
//
// a load balancer may record a list of dropped_replica to track the drop history and use
// it to do the cure decision.
//

// currently dropped_cmp depend on the dropped_replica::INVALID_TIMESTAMP is 0,
// if you modify the dropped_replica::INVALID_TIMESTAMP, please modify the dropped_cmp accordingly.
struct dropped_replica
{
    dsn::host_port node;

    // if a drop-replica is generated by the update-cfg-req, then we can
    // record the drop time (milliseconds)
    uint64_t time;
    // if a drop-replica is got from the replica server's report, then we can
    // record (ballot, commit_decree, prepare_decree)
    //[
    int64_t ballot;
    int64_t last_committed_decree;
    int64_t last_prepared_decree;
    //]
    static const uint64_t INVALID_TIMESTAMP = 0;
};

// the order of dropped_replica
// ret:
//   0 => equal
//   negtive => d1 smaller than d2
//   positive => d1 larger than d2
inline int dropped_cmp(const dropped_replica &d1, const dropped_replica &d2)
{
    if (d1.time != d2.time) {
        return (d1.time < d2.time) ? -1 : 1;
    }
    if (d1.ballot != d2.ballot) {
        return d1.ballot < d2.ballot ? -1 : 1;
    }
    if (d1.last_committed_decree != d2.last_committed_decree) {
        return d1.last_committed_decree < d2.last_committed_decree ? -1 : 1;
    }
    if (d1.last_prepared_decree != d2.last_prepared_decree) {
        return d1.last_prepared_decree < d2.last_prepared_decree ? -1 : 1;
    }
    return 0;
}

// Represent a replica that is serving. Info in this structure can only from config-sync of RS.
// Load balancer may use this to do balance decisions.
struct serving_replica
{
    dsn::host_port node;
    // TODO: report the storage size of replica
    int64_t storage_mb;
    std::string disk_tag;
    manual_compaction_status::type compact_status;
};

class config_context
{
public:
    partition_configuration *pc;
    config_status stage;
    // for server state's update config management
    //[
    task_ptr pending_sync_task;
    std::shared_ptr<configuration_update_request> pending_sync_request;
    dsn::message_ex *msg;
    //]

    // for load balancer's decision
    //[
    proposal_actions lb_actions;
    std::vector<serving_replica> serving;
    std::vector<dropped_replica> dropped;
    // An index value to the vector "dropped".
    // Used in load-balancer's cure to avoid select the same learner as
    // previous unsuccessful proposal.
    // Please refer to partition_guardian::on_missing_secondary.
    //
    // This should always be less than the dropped.size()
    //
    // TODO: a more clear implementation
    int32_t prefered_dropped;
    //]
public:
    void check_size();
    void cancel_sync();

    std::vector<dropped_replica>::iterator find_from_dropped(const dsn::host_port &node);
    std::vector<dropped_replica>::const_iterator find_from_dropped(const host_port &node) const;

    // return true if remove ok, false if node doesn't in dropped
    bool remove_from_dropped(const dsn::host_port &node);

    // put recently downgraded node to dropped
    // return true if put ok, false if the node has been in dropped
    bool record_drop_history(const dsn::host_port &node);

    // Notice: please make sure whether node is actually an inactive or a serving replica
    // ret:
    //   1 => node has been in the dropped
    //   0 => insert the info to the dropped
    //  -1 => info is too staled to insert
    int collect_drop_replica(const dsn::host_port &node, const replica_info &info);

    // check if dropped vector satisfied the order
    bool check_order();

    std::vector<serving_replica>::iterator find_from_serving(const dsn::host_port &node);
    std::vector<serving_replica>::const_iterator find_from_serving(const host_port &node) const;

    // return true if remove ok, false if node doesn't in serving
    bool remove_from_serving(const dsn::host_port &node);

    void collect_serving_replica(const dsn::host_port &node, const replica_info &info);

    void adjust_proposal(const dsn::host_port &node, const replica_info &info);

    bool get_disk_tag(const host_port &node, /*out*/ std::string &disk_tag) const;

public:
    // intialize to 4 statically.
    // and will be set by load-balancer module
    static int MAX_REPLICA_COUNT_IN_GRROUP;
};

struct partition_configuration_stateless
{
    partition_configuration &pc;
    partition_configuration_stateless(partition_configuration &_pc) : pc(_pc) {}
    std::vector<dsn::host_port> &workers() { return pc.hp_last_drops; }
    std::vector<dsn::host_port> &hosts() { return pc.hp_secondaries; }
    bool is_host(const host_port &node) const { return utils::contains(pc.hp_secondaries, node); }
    bool is_worker(const host_port &node) const { return utils::contains(pc.hp_last_drops, node); }
    bool is_member(const host_port &node) const { return is_host(node) || is_worker(node); }
};

struct restore_state
{
    // restore_status:
    //      ERR_OK: restore haven't encounter some error
    //      ERR_CORRUPTION : data on backup media is damaged and we can not skip the damage data,
    //                       so should restore rollback
    //      ERR_IGNORE_DAMAGED_DATA : data on backup media is damaged but we can skip the damage
    //                                data, so skip the damaged partition
    dsn::error_code restore_status;
    int32_t progress;
    std::string reason;
    restore_state() : restore_status(dsn::ERR_OK), progress(0), reason() {}
};

// app partition_split states
// when starting partition split, `splitting_count` will be equal to old_partition_count,
// <parent_partition_index, SPLITTING> will be inserted into `status`.
// if partition[0] finish split, `splitting_count` will decrease and <0, SPLITTING> will be removed
// in `status`.
struct split_state
{
    int32_t splitting_count;
    // partition_index -> split_status
    std::map<int32_t, split_status::type> status;
    split_state() : splitting_count(0) {}
};

class app_state;

class app_state_helper
{
public:
    app_state *owner;
    std::atomic_int partitions_in_progress;
    std::vector<config_context> contexts;
    dsn::message_ex *pending_response;
    std::vector<restore_state> restore_states;
    split_state split_states;

public:
    app_state_helper() : owner(nullptr), partitions_in_progress(0)
    {
        contexts.clear();
        pending_response = nullptr;
    }
    void on_init_partitions();
    void clear_proposals()
    {
        for (config_context &cc : contexts) {
            cc.lb_actions.clear();
        }
    }

    void reset_manual_compact_status();
    // get replica group manual compact progress
    // return false if partition is not executing manual compaction
    bool get_manual_compact_progress(/*out*/ int32_t &progress) const;
};

/*
 * NOTICE: several keys in envs are reserved for recover from cold_backup:
 * envs["block_service_provider"] = <block_service_provider>
 * envs["cluster_name"] = <cluster_name>
 * envs["policy_name"] = <policy_name>
 * envs["app_name"] = <app_name>
 * envs["app_id"] = <app_id>
 * envs["backup_id"] = <backup_id>
 * envs["skip_bad_partition"] = <"true" or "false">
 *
 * after a newly assigned primary get these envs from app_info, it will try to
 * init a replica with data stored on the block_device
 */
class app_state : public app_info
{
protected:
    std::string log_name;

public:
    app_state(const app_info &info);

public:
    const char *get_logname() const { return log_name.c_str(); }
    std::shared_ptr<app_state_helper> helpers;
    std::vector<partition_configuration> pcs;
    std::map<dupid_t, duplication_info_s_ptr> duplications;

    static std::shared_ptr<app_state> create(const app_info &info);
    dsn::blob to_json(app_status::type temp_status)
    {
        app_info another = *this;
        another.status = temp_status;
        // persistent envs to zookeeper
        dsn::blob result = dsn::json::json_forwarder<app_info>::encode(another);
        return result;
    }
    bool splitting() const { return helpers->split_states.splitting_count > 0; }
};

typedef std::set<dsn::gpid> partition_set;
typedef std::map<app_id, std::shared_ptr<app_state>> app_mapper;

class node_state : public extensible_object<node_state, 4>
{
private:
    // partitions
    std::map<int32_t, partition_set> app_primaries;
    std::map<int32_t, partition_set> app_partitions;
    unsigned total_primaries;
    unsigned total_partitions;

    // status
    bool is_alive;
    bool has_collected_replicas;
    dsn::host_port hp;

    const partition_set *get_partitions(app_id id, bool only_primary) const;
    partition_set *get_partitions(app_id id, bool only_primary, bool create_new);

public:
    node_state();
    const partition_set *partitions(app_id id, bool only_primary) const;
    partition_set *partitions(app_id id, bool only_primary);

    unsigned primary_count(app_id id) const;
    unsigned secondary_count(app_id id) const { return partition_count(id) - primary_count(id); }
    unsigned partition_count(app_id id) const;

    unsigned primary_count() const { return total_primaries; }
    unsigned secondary_count() const { return total_partitions - total_primaries; }
    unsigned partition_count() const { return total_partitions; }

    partition_status::type served_as(const gpid &pid) const;

    bool alive() const { return is_alive; }
    void set_alive(bool alive) { is_alive = alive; }
    bool has_collected() { return has_collected_replicas; }
    void set_replicas_collect_flag(bool has_collected) { has_collected_replicas = has_collected; }
    const dsn::host_port &host_port() const { return hp; }
    void set_hp(const dsn::host_port &val) { hp = val; }

    void put_partition(const dsn::gpid &pid, bool is_primary);
    void remove_partition(const dsn::gpid &pid, bool only_primary);

    bool for_each_partition(const std::function<bool(const dsn::gpid &pid)> &f) const;
    bool for_each_partition(app_id id, const std::function<bool(const dsn::gpid &)> &f) const;
    bool for_each_primary(app_id id, const std::function<bool(const dsn::gpid &pid)> &f) const;
};

typedef std::unordered_map<host_port, node_state> node_mapper;
typedef std::map<dsn::gpid, std::shared_ptr<configuration_balancer_request>> migration_list;

struct meta_view
{
    app_mapper *apps;
    node_mapper *nodes;
};

inline node_state *get_node_state(node_mapper &nodes, const host_port &hp, bool create_new)
{
    node_state *ns;
    if (nodes.find(hp) == nodes.end()) {
        if (!create_new)
            return nullptr;
        ns = &nodes[hp];
        ns->set_hp(hp);
    }
    ns = &nodes[hp];
    return ns;
}

inline bool is_node_alive(const node_mapper &nodes, const host_port &hp)
{
    auto iter = nodes.find(hp);
    if (iter == nodes.end())
        return false;
    return iter->second.alive();
}

inline const partition_configuration *get_config(const app_mapper &apps, const dsn::gpid &gpid)
{
    const auto iter = apps.find(gpid.get_app_id());
    if (iter == apps.end() || iter->second->status == app_status::AS_DROPPED) {
        return nullptr;
    }

    return &(iter->second->pcs[gpid.get_partition_index()]);
}

inline partition_configuration *get_config(app_mapper &apps, const dsn::gpid &gpid)
{
    const auto iter = apps.find(gpid.get_app_id());
    if (iter == apps.end() || iter->second->status == app_status::AS_DROPPED) {
        return nullptr;
    }

    return &(iter->second->pcs[gpid.get_partition_index()]);
}

inline const config_context *get_config_context(const app_mapper &apps, const dsn::gpid &gpid)
{
    auto iter = apps.find(gpid.get_app_id());
    if (iter == apps.end() || iter->second->status == app_status::AS_DROPPED)
        return nullptr;
    return &(iter->second->helpers->contexts[gpid.get_partition_index()]);
}

inline config_context *get_config_context(app_mapper &apps, const dsn::gpid &gpid)
{
    auto iter = apps.find(gpid.get_app_id());
    if (iter == apps.end() || iter->second->status == app_status::AS_DROPPED)
        return nullptr;
    return &(iter->second->helpers->contexts[gpid.get_partition_index()]);
}

inline int replica_count(const partition_configuration &pc)
{
    int ans = pc.hp_primary ? 1 : 0;
    return ans + pc.hp_secondaries.size();
}

enum health_status
{
    HS_DEAD = 0,     // (primary = 0 && secondary = 0)
    HS_UNREADABLE,   // (primary = 0 && secondary > 0)
    HS_UNWRITABLE,   // (primary = 1 && primary + secondary < mutation_2pc_min_replica_count)
    HS_WRITABLE_ILL, // (primary = 1 && primary + secondary >= mutation_2pc_min_replica_count
                     //              && primary + secondary < max_replica_count)
    HS_HEALTHY,      // (primary = 1 && primary + secondary >= max_replica_count)
    HS_MAX_VALUE
};

inline health_status partition_health_status(const partition_configuration &pc,
                                             int mutation_2pc_min_replica_count)
{
    if (!pc.hp_primary) {
        if (pc.hp_secondaries.empty()) {
            return HS_DEAD;
        }
        return HS_UNREADABLE;
    }

    const auto replica_count = pc.hp_secondaries.size() + 1;
    if (replica_count < mutation_2pc_min_replica_count) {
        return HS_UNWRITABLE;
    }

    if (replica_count < pc.max_replica_count) {
        return HS_WRITABLE_ILL;
    }
    return HS_HEALTHY;
}

inline void
for_each_available_app(const app_mapper &apps,
                       const std::function<bool(const std::shared_ptr<app_state> &)> &action)
{
    for (const auto &[_, as] : apps) {
        if (as->status == app_status::AS_AVAILABLE && !action(as)) {
            break;
        }
    }
}

inline int count_partitions(const app_mapper &apps)
{
    int result = 0;
    for (auto iter : apps)
        if (iter.second->status == app_status::AS_AVAILABLE)
            result += iter.second->partition_count;
    return result;
}

void when_update_replicas(config_type::type t, const std::function<void(bool)> &func);

// TODO(yingchun): refactor to deal both rpc_address and host_port
template <typename T>
void maintain_drops(/*inout*/ std::vector<T> &drops, const T &node, config_type::type t)
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

// Try to construct a replica-group by current replica-infos of a gpid
// ret:
//   if construct the replica successfully, return true.
//   Notice: as long as we can construct something from current infos, we treat it as a
//   success
bool construct_replica(meta_view view, const gpid &pid, int max_replica_count);

// When replica infos are collected from replica servers, meta-server
// will use this to check if a replica on a server is useful
// params:
//   node: the owner of the replica info
//   info: the replica info on node
// ret:
//   return true if the replica is accepted as an useful replica. Or-else false.
//   WARNING: if false is returned, the replica on node may be garbage-collected
bool collect_replica(meta_view view, const host_port &node, const replica_info &info);

inline bool has_seconds_expired(uint64_t second_ts) { return second_ts * 1000 < dsn_now_ms(); }

inline bool has_milliseconds_expired(uint64_t milliseconds_ts)
{
    return milliseconds_ts < dsn_now_ms();
}
} // namespace replication
} // namespace dsn

namespace dsn {
namespace json {

inline void json_encode(dsn::json::JsonWriter &out, const replication::app_state &state)
{
    json_forwarder<dsn::app_info>::encode(out, (const dsn::app_info &)state);
}

inline bool json_decode(const dsn::json::JsonObject &in, replication::app_state &state)
{
    return json_forwarder<dsn::app_info>::decode(in, (dsn::app_info &)state);
}
} // namespace json
} // namespace dsn
