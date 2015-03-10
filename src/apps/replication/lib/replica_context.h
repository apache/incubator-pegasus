/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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
    task_ptr      timeout_tsk;
    decree       prepareStartDecree;
};

typedef std::map<end_point, remote_learner_state, end_point_comparor> learner_map;

class primary_context
{
public:
    void Cleanup(bool cleanPendingMutations = true);
       
    void ResetMembership(const partition_configuration& config, bool clearLearners);
    bool GetReplicaConfig(const end_point& node, __out_param replica_configuration& config);
    void GetReplicaConfig(partition_status status, __out_param replica_configuration& config);
    bool CheckExist(const end_point& node, partition_status status);
    partition_status GetNodeStatus(const end_point& addr) const;

    void CleanupPendingMutations(bool cleanPendingMutations = true);
    void CleanupGroupCheck();
    
public:
    // membership mgr, including learners
    partition_configuration membership;
    NodeStatusMap          Statuses;
    learner_map             Learners;

    // 2pc batching
    mutation_ptr PendingMutation;
    task_ptr     PendingMutationTask;

    // group check
    task_ptr     GroupCheckTask;
    node_tasks   GroupCheckPendingReplies;

    // reconfig
    task_ptr     ReconfigurationTask;
};


class potential_secondary_context 
{
public:
    potential_secondary_context() : LearningSignature(0), LearningRoundIsRuning(false) {}
    bool Cleanup(bool force);

public:
    uint64_t        LearningSignature;
    LearnerState  LearningState;
    volatile bool LearningRoundIsRuning;

    task_ptr       LearningTask;
    task_ptr       LearnRemoteFilesTask;
    task_ptr       LearnRemoteFilesCompletedTask;


};

//---------------inline impl----------------------------------------------------------------

inline partition_status primary_context::GetNodeStatus(const end_point& addr) const
{ 
    auto it = Statuses.find(addr);
    return it != Statuses.end()  ? it->second : PS_INACTIVE;
}

}} // end namespace
