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

#include "utils/zlocks.h"
#include "block_service/block_service.h"
#include "common/json_helper.h"

#include "mutation.h"

class replication_service_test_app;

namespace dsn {
namespace replication {

class replica;
class replica_stub;

struct remote_learner_state
{
    int64_t signature;
    ::dsn::task_ptr timeout_task;
    decree prepare_start_decree;
    std::string last_learn_log_file;
};

typedef std::unordered_map<::dsn::rpc_address, remote_learner_state> learner_map;

#define CLEANUP_TASK(task_, force)                                                                 \
    {                                                                                              \
        task_ptr t = task_;                                                                        \
        if (t != nullptr) {                                                                        \
            bool finished;                                                                         \
            t->cancel(force, &finished);                                                           \
            if (!finished && !dsn_task_is_running_inside(task_.get()))                             \
                return false;                                                                      \
            task_ = nullptr;                                                                       \
        }                                                                                          \
    }

#define CLEANUP_TASK_ALWAYS(task_)                                                                 \
    {                                                                                              \
        task_ptr t = task_;                                                                        \
        if (t != nullptr) {                                                                        \
            bool finished;                                                                         \
            t->cancel(false, &finished);                                                           \
            CHECK(finished || dsn_task_is_running_inside(task_.get()),                             \
                  "task must be finished at this point");                                          \
            task_ = nullptr;                                                                       \
        }                                                                                          \
    }

class primary_context
{
public:
    primary_context(gpid gpid, int max_concurrent_2pc_count = 1, bool batch_write_disabled = false)
        : next_learning_version(0),
          write_queue(gpid, max_concurrent_2pc_count, batch_write_disabled),
          last_prepare_decree_on_new_primary(0),
          last_prepare_ts_ms(dsn_now_ms())
    {
    }

    void cleanup(bool clean_pending_mutations = true);
    bool is_cleaned();

    void reset_membership(const partition_configuration &config, bool clear_learners);
    void get_replica_config(partition_status::type status,
                            /*out*/ replica_configuration &config,
                            uint64_t learner_signature = invalid_signature);
    bool check_exist(::dsn::rpc_address node, partition_status::type status);
    partition_status::type get_node_status(::dsn::rpc_address addr) const;

    void do_cleanup_pending_mutations(bool clean_pending_mutations = true);

    // reset bulk load states in secondary_bulk_load_states by node address
    void reset_node_bulk_load_states(const rpc_address &node);

    void cleanup_bulk_load_states();

    void cleanup_split_states();

    bool secondary_disk_space_insufficient() const;

public:
    // membership mgr, including learners
    partition_configuration membership;
    node_statuses statuses;
    learner_map learners;
    uint64_t next_learning_version;

    // 2pc batching
    mutation_queue write_queue;

    // group check
    dsn::task_ptr group_check_task; // the repeated group check task of LPC_GROUP_CHECK
    // calls broadcast_group_check() to check all replicas separately
    // created in replica::init_group_check()
    // cancelled in cleanup() when status changed from PRIMARY to others
    node_tasks group_check_pending_replies; // group check response tasks of RPC_GROUP_CHECK for
                                            // each replica

    // reconfiguration task of RPC_CM_UPDATE_PARTITION_CONFIGURATION
    dsn::task_ptr reconfiguration_task;

    // when read lastest update, all prepared decrees must be firstly committed
    // (possibly true on old primary) before opening read service
    decree last_prepare_decree_on_new_primary;

    // copy checkpoint from secondaries ptr
    dsn::task_ptr checkpoint_task;

    uint64_t last_prepare_ts_ms;

    // Used for partition split
    // child addresses who has been caught up with its parent
    std::unordered_set<dsn::rpc_address> caught_up_children;

    // Used for partition split
    // whether parent's write request should be sent to child synchronously
    // if {sync_send_write_request} = true
    // - parent should recevie prepare ack from child synchronously during 2pc
    // if {sync_send_write_request} = false and replica is during partition split
    // - parent should copy mutations to child asynchronously, child is during async-learn
    // whether a replica is during partition split is determined by a variety named `_child_gpid` of
    // replica class
    // if app_id of `_child_gpid` is greater than zero, it means replica is during partition split,
    // otherwise, not during partition split
    bool sync_send_write_request{false};

    // Used for partition split
    // primary parent register child on meta_server task
    dsn::task_ptr register_child_task;

    // Used partition split
    // secondary replica address who has paused or canceled split
    std::unordered_set<rpc_address> split_stopped_secondary;

    // Used for partition split
    // primary parent query child on meta_server task
    // Called by `trigger_primary_parent_split`
    dsn::task_ptr query_child_task;

    // Used for bulk load
    // group bulk_load response tasks of RPC_GROUP_BULK_LOAD for each secondary replica
    node_tasks group_bulk_load_pending_replies;
    // bulk_load_state of secondary replicas
    std::unordered_map<rpc_address, partition_bulk_load_state> secondary_bulk_load_states;
    // if primary send an empty prepare after ingestion succeed to gurantee secondary commit its
    // ingestion request
    bool ingestion_is_empty_prepare_sent{false};

    // secondary rpc_address -> secondary disk_status
    std::unordered_map<rpc_address, disk_status::type> secondary_disk_status;
};

class secondary_context
{
public:
    secondary_context() : checkpoint_is_running(false) {}
    bool cleanup(bool force);
    bool is_cleaned();

public:
    bool checkpoint_is_running;
    ::dsn::task_ptr checkpoint_task;
    ::dsn::task_ptr checkpoint_completed_task;
    ::dsn::task_ptr catchup_with_private_log_task;
};

class potential_secondary_context
{
public:
    explicit potential_secondary_context(replica *r)
        : owner_replica(r),
          learning_version(0),
          learning_start_ts_ns(0),
          learning_copy_file_count(0),
          learning_copy_file_size(0),
          learning_copy_buffer_size(0),
          learning_status(learner_status::LearningInvalid),
          learning_round_is_running(false),
          learn_app_concurrent_count_increased(false),
          learning_start_prepare_decree(invalid_decree)
    {
    }

    bool cleanup(bool force);
    bool is_cleaned();
    uint64_t duration_ms() const
    {
        return learning_start_ts_ns > 0 ? (dsn_now_ns() - learning_start_ts_ns) / 1000000 : 0;
    }

public:
    replica *owner_replica;
    uint64_t learning_version;
    uint64_t learning_start_ts_ns;
    uint64_t learning_copy_file_count;
    uint64_t learning_copy_file_size;
    uint64_t learning_copy_buffer_size;
    learner_status::type learning_status;
    volatile bool learning_round_is_running;
    volatile bool learn_app_concurrent_count_increased;
    decree learning_start_prepare_decree;

    // The start decree in the first round of learn.
    // It indicates the minimum decree under `learn/` dir.
    decree first_learn_start_decree{invalid_decree};

    ::dsn::task_ptr delay_learning_task;
    ::dsn::task_ptr learning_task;
    ::dsn::task_ptr learn_remote_files_task;
    ::dsn::task_ptr learn_remote_files_completed_task;
    ::dsn::task_ptr catchup_with_private_log_task;
    ::dsn::task_ptr completion_notify_task;
};

class partition_split_context
{
public:
    bool cleanup(bool force);
    bool is_cleaned() const;
    uint64_t total_ms() const
    {
        return splitting_start_ts_ns > 0 ? (dsn_now_ns() - splitting_start_ts_ns) / 1000000 : 0;
    }
    uint64_t async_learn_ms() const
    {
        return splitting_start_async_learn_ts_ns > 0
                   ? (dsn_now_ns() - splitting_start_async_learn_ts_ns) / 1000000
                   : 0;
    }

public:
    gpid parent_gpid;
    // whether child has copied parent prepare list
    bool is_prepare_list_copied{false};
    // whether child has catched up with parent during async-learn
    bool is_caught_up{false};

    // child replica async learn parent states
    task_ptr async_learn_task;

    // partition split states checker, start when initialize child replica
    // see more in function `child_check_split_context` and `parent_check_states`
    task_ptr check_state_task;

    // Used for split related perf-counter
    uint64_t splitting_start_ts_ns{0};
    uint64_t splitting_start_async_learn_ts_ns{0};
    uint64_t splitting_copy_file_count{0};
    uint64_t splitting_copy_file_size{0};
    uint64_t splitting_copy_mutation_count{0};
};

//---------------inline impl----------------------------------------------------------------

inline partition_status::type primary_context::get_node_status(::dsn::rpc_address addr) const
{
    auto it = statuses.find(addr);
    return it != statuses.end() ? it->second : partition_status::PS_INACTIVE;
}
} // namespace replication
} // namespace dsn
