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
 *     context for replica with different roles
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica_context.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.context"

namespace dsn {
    namespace replication {

# define CLEANUP_TASK(task_, force)         \
    {                                       \
        task_ptr t = task_;                 \
        if (t != nullptr)                   \
        {                                   \
            bool finished;                  \
            t->cancel(force, &finished);    \
            if (!finished && !dsn_task_current(task_->native_handle()))   \
                return false;               \
            task_ = nullptr;                \
        }                                   \
    }

# define CLEANUP_TASK_ALWAYS(task_)         \
    {                                       \
        task_ptr t = task_;                 \
        if (t != nullptr)                   \
        {                                   \
            bool finished;                  \
            t->cancel(false, &finished);    \
   dassert (finished || dsn_task_current(task_->native_handle()), "task must be finished at this point"); \
            task_ = nullptr;                \
        }                                   \
    }

void primary_context::cleanup(bool clean_pending_mutations)
{
    do_cleanup_pending_mutations(clean_pending_mutations);

    // clean up group check
    CLEANUP_TASK_ALWAYS(group_check_task)

    for (auto it = group_check_pending_replies.begin(); it != group_check_pending_replies.end(); ++it)
    {
        CLEANUP_TASK_ALWAYS(it->second)
        //it->second->cancel(true);
    }

    group_check_pending_replies.clear();

    // clean up reconfiguration
    CLEANUP_TASK_ALWAYS(reconfiguration_task)

    // clean up checkpoint
    CLEANUP_TASK_ALWAYS(checkpoint_task)
}

bool primary_context::is_clean()
{
    return
        nullptr == group_check_task &&
        nullptr == reconfiguration_task &&
        nullptr == checkpoint_task &&
        group_check_pending_replies.empty()
        ;
}

void primary_context::do_cleanup_pending_mutations(bool clean_pending_mutations)
{
    if (clean_pending_mutations)
    {
        write_queue.clear();
    }
}

void primary_context::reset_membership(const partition_configuration& config, bool clear_learners)
{
    statuses.clear();
    if (clear_learners)
    {
        learners.clear();
    }

    membership = config;

    if (membership.primary.is_invalid() == false)
    {
        statuses[membership.primary] = PS_PRIMARY;
    }

    for (auto it = config.secondaries.begin(); it != config.secondaries.end(); ++it)
    {
        statuses[*it] = PS_SECONDARY;
        learners.erase(*it);
    }

    for (auto it = learners.begin(); it != learners.end(); ++it)
    {
        statuses[it->first] = PS_POTENTIAL_SECONDARY;
    }
}

void primary_context::get_replica_config(partition_status st, /*out*/ replica_configuration& config, uint64_t learner_signature /*= invalid_signature*/)
{
    config.gpid = membership.gpid;
    config.primary = membership.primary;  
    config.ballot = membership.ballot;
    config.status = st;
    config.learner_signature = learner_signature;
}

bool primary_context::check_exist(::dsn::rpc_address node, partition_status st)
{
    switch (st)
    {
    case PS_PRIMARY:
        return membership.primary == node;
    case PS_SECONDARY:
        return std::find(membership.secondaries.begin(), membership.secondaries.end(), node) != membership.secondaries.end();
    case PS_POTENTIAL_SECONDARY:
        return learners.find(node) != learners.end();
    default:
        dassert (false, "");
        return false;
    }
}

bool secondary_context::cleanup(bool force)
{
    CLEANUP_TASK(checkpoint_task, force)
    return true;
}

bool secondary_context::is_clean()
{
    return nullptr == checkpoint_task;
}

bool potential_secondary_context::cleanup(bool force)
{
    task_ptr t = nullptr;

    CLEANUP_TASK(learn_remote_files_task, force)

    CLEANUP_TASK_ALWAYS(learning_task)

    CLEANUP_TASK_ALWAYS(learn_remote_files_completed_task)

    learning_signature = 0;
    learning_round_is_running = false;
    learning_start_prepare_decree = invalid_decree;
    learning_status = Learning_INVALID;
    return true;
}

bool potential_secondary_context::is_clean()
{
    return 
        nullptr == learn_remote_files_completed_task &&
        nullptr == learning_task &&
        nullptr == learn_remote_files_completed_task
        ;
}

}} // end namespace
