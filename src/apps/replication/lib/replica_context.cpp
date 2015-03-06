#include "replica_context.h"

namespace rdsn { namespace replication {

void primary_context::Cleanup(bool cleanPendingMutations)
{
    CleanupPendingMutations(cleanPendingMutations);

    // clean up group check
    if (nullptr != GroupCheckTask)
    {
        GroupCheckTask->cancel(true);
        GroupCheckTask = nullptr;
    }

    for (auto it = GroupCheckPendingReplies.begin(); it != GroupCheckPendingReplies.end(); it++)
    {
        it->second->cancel(true);
    }
    GroupCheckPendingReplies.clear();

    // clean up reconfiguration
    if (nullptr != ReconfigurationTask)
    {
        ReconfigurationTask->cancel(true);
        ReconfigurationTask = nullptr;
    }
}

void primary_context::CleanupPendingMutations(bool cleanPendingMutations)
{
    if (PendingMutationTask != nullptr) 
    {
        PendingMutationTask->cancel(true);        
        PendingMutationTask = nullptr;
    }

    if (cleanPendingMutations)
    {
        PendingMutation = nullptr;
    }
}

void primary_context::ResetMembership(const partition_configuration& config, bool clearLearners)
{
    Statuses.clear();
    if (clearLearners)
    {
        Learners.clear();
    }

    membership = config;

    if (membership.primary != rdsn::end_point::INVALID)
    {
        Statuses[membership.primary] = PS_PRIMARY;
    }

    for (auto it = config.secondaries.begin(); it != config.secondaries.end(); it++)
    {
        Statuses[*it] = PS_SECONDARY;
        Learners.erase(*it);
    }

    for (auto it = Learners.begin(); it != Learners.end(); it++)
    {
        Statuses[it->first] = PS_POTENTIAL_SECONDARY;
    }

    for (auto it = config.dropOuts.begin(); it != config.dropOuts.end(); it++)
    {
        if (Statuses.find(*it) == Statuses.end())
        {
            Statuses[*it] = PS_INACTIVE;
        }
    }
}

bool primary_context::GetReplicaConfig(const end_point& node, __out replica_configuration& config)
{
    config.gpid = membership.gpid;
    config.primary = membership.primary;  
    config.ballot = membership.ballot;

    auto it = Statuses.find(node);
    if (it != Statuses.end())
    {
        config.status = it->second;
        return true;
    }
    else
    {
        config.status = PS_INACTIVE;
        return false;
    }
}


void primary_context::GetReplicaConfig(partition_status status, __out replica_configuration& config)
{
    config.gpid = membership.gpid;
    config.primary = membership.primary;  
    config.ballot = membership.ballot;
    config.status = status;
}

bool primary_context::CheckExist(const end_point& node, partition_status status)
{
    switch (status)
    {
    case PS_PRIMARY:
        return membership.primary == node;
    case PS_SECONDARY:
        return std::find(membership.secondaries.begin(), membership.secondaries.end(), node) != membership.secondaries.end();
    case PS_POTENTIAL_SECONDARY:
        return Learners.find(node) != Learners.end();
    case PS_INACTIVE:
        return std::find(membership.dropOuts.begin(), membership.dropOuts.end(), node) != membership.dropOuts.end();
    default:
        rassert(false, "");
        return false;
    }
}

bool potential_secondary_context::Cleanup(bool force)
{
    if (LearnRemoteFilesTask != nullptr)
    {
        bool cleanRemoteLearning = LearnRemoteFilesTask->cancel(false);
        if (force)
        {
            LearnRemoteFilesTask->cancel(true);
        }
        else if (!cleanRemoteLearning)
        {
            return false;
        }
    }

    if (LearningTask != nullptr)
    {
        LearningTask->cancel(true);
    }

    if (LearnRemoteFilesCompletedTask != nullptr)
    {
        LearnRemoteFilesCompletedTask->cancel(true);
    }

    LearningSignature = 0;
    LearningRoundIsRuning = false;
    return true;
}

}} // end namespace
