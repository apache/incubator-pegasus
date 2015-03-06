#pragma once

#include "mutation.h"

namespace rdsn { namespace replication {

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
    bool GetReplicaConfig(const end_point& node, __out replica_configuration& config);
    void GetReplicaConfig(partition_status status, __out replica_configuration& config);
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
