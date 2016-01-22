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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include "mutation.h"

namespace dsn { namespace replication {

struct remote_learner_state
{
    int64_t signature;
    ::dsn::task_ptr timeout_task;
    decree   prepare_start_decree;
};

typedef std::unordered_map< ::dsn::rpc_address, remote_learner_state> learner_map;

class primary_context
{
public:
    primary_context(global_partition_id gpid, int max_concurrent_2pc_count = 1, bool batch_write_disabled = false)
        : write_queue(gpid, max_concurrent_2pc_count, batch_write_disabled)
    {}

    void cleanup(bool clean_pending_mutations = true);
    bool is_cleaned();
       
    void reset_membership(const partition_configuration& config, bool clear_learners);
    void get_replica_config(partition_status status, /*out*/ replica_configuration& config, uint64_t learner_signature = invalid_signature);
    bool check_exist(::dsn::rpc_address node, partition_status status);
    partition_status get_node_status(::dsn::rpc_address addr) const;

    void do_cleanup_pending_mutations(bool clean_pending_mutations = true);
    
public:
    // membership mgr, including learners
    partition_configuration membership;
    node_statuses           statuses;
    learner_map             learners;

    // 2pc batching
    mutation_queue          write_queue;

    // group check
    dsn::task_ptr     group_check_task; // the repeated group check task of LPC_GROUP_CHECK
                                        // calls broadcast_group_check() to check all replicas separately
                                        // created in replica::init_group_check()
                                        // cancelled in cleanup() when status changed from PRIMARY to others
    node_tasks        group_check_pending_replies; // group check response tasks of RPC_GROUP_CHECK for each replica

    // reconfiguration task of RPC_CM_UPDATE_PARTITION_CONFIGURATION
    dsn::task_ptr     reconfiguration_task;

    // when read lastest update, all prepared decrees must be firstly committed
    // (possibly true on old primary) before opening read service
    decree       last_prepare_decree_on_new_primary; 

    // copy checkpoint from secondaries ptr
    dsn::task_ptr   checkpoint_task;
};

class secondary_context
{
public:
    secondary_context() : checkpoint_is_running(false) {}
    bool cleanup(bool force);
    bool is_cleaned();

public:
    bool            checkpoint_is_running;
    ::dsn::task_ptr checkpoint_task;
    ::dsn::task_ptr checkpoint_completed_task;
    ::dsn::task_ptr catchup_with_private_log_task;
};

class potential_secondary_context 
{
public:
    potential_secondary_context() :
        learning_signature(0),
        learning_round_is_running(false),
        learning_status(learner_status::Learning_INVALID),
        learning_start_prepare_decree(invalid_decree)
    {}

    bool cleanup(bool force);
    bool is_cleaned();

public:
    uint64_t        learning_signature;
    learner_status  learning_status;
    volatile bool   learning_round_is_running;
    decree          learning_start_prepare_decree;

    ::dsn::task_ptr       delay_learning_task;
    ::dsn::task_ptr       learning_task;
    ::dsn::task_ptr       learn_remote_files_task;
    ::dsn::task_ptr       learn_remote_files_completed_task;
    ::dsn::task_ptr       catchup_with_private_log_task;
};

//---------------inline impl----------------------------------------------------------------

inline partition_status primary_context::get_node_status(::dsn::rpc_address addr) const
{ 
    auto it = statuses.find(addr);
    return it != statuses.end()  ? it->second : PS_INACTIVE;
}

}} // end namespace
