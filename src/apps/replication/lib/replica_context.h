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

#include "mutation.h"

namespace dsn { namespace replication {

struct remote_learner_state
{
    uint64_t       signature;
    task_ptr      timeout_task;
    decree       prepare_start_decree;
};

typedef std::map<end_point, remote_learner_state> learner_map;

class primary_context
{
public:
    void cleanup(bool clean_pending_mutations = true);
       
    void reset_membership(const partition_configuration& config, bool clear_learners);
    bool get_replica_config(const end_point& node, __out_param replica_configuration& config);
    void get_replica_config(partition_status status, __out_param replica_configuration& config);
    bool check_exist(const end_point& node, partition_status status);
    partition_status GetNodeStatus(const end_point& addr) const;

    void do_cleanup_pending_mutations(bool clean_pending_mutations = true);
    void CleanupGroupCheck();
    
public:
    // membership mgr, including learners
    partition_configuration membership;
    NodeStatusMap          statuses;
    learner_map             learners;

    // 2pc batching
    mutation_ptr pending_mutation;
    task_ptr     pending_mutation_task;

    // group check
    task_ptr     group_check_task;
    node_tasks   group_check_pending_replies;

    // reconfig
    task_ptr     reconfiguration_task;
};


class potential_secondary_context 
{
public:
    potential_secondary_context() :
        learning_signature(0),
        learning_round_is_running(false),
        learning_status(learner_status::Learning_INVALID)
    {}
    bool cleanup(bool force);

public:
    uint64_t        learning_signature;
    learner_status  learning_status;
    volatile bool learning_round_is_running;

    task_ptr       learning_task;
    task_ptr       learn_remote_files_task;
    task_ptr       learn_remote_files_completed_task;


};

//---------------inline impl----------------------------------------------------------------

inline partition_status primary_context::GetNodeStatus(const end_point& addr) const
{ 
    auto it = statuses.find(addr);
    return it != statuses.end()  ? it->second : PS_INACTIVE;
}

}} // end namespace
