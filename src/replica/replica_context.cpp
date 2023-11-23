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

#include <algorithm>
#include <atomic>
#include <vector>

#include "bulk_load_types.h"
#include "common/replication_enums.h"
#include "mutation.h"
#include "replica.h"
#include "replica_context.h"
#include "replica_stub.h"
#include "utils/error_code.h"

namespace dsn {
namespace replication {

void primary_context::cleanup(bool clean_pending_mutations)
{
    do_cleanup_pending_mutations(clean_pending_mutations);

    // clean up group check
    CLEANUP_TASK_ALWAYS(group_check_task)

    for (auto it = group_check_pending_replies.begin(); it != group_check_pending_replies.end();
         ++it) {
        CLEANUP_TASK_ALWAYS(it->second)
        // it->second->cancel(true);
    }

    group_check_pending_replies.clear();

    // clean up reconfiguration
    CLEANUP_TASK_ALWAYS(reconfiguration_task)

    // clean up checkpoint
    CLEANUP_TASK_ALWAYS(checkpoint_task)

    // cleanup group bulk load
    for (auto &kv : group_bulk_load_pending_replies) {
        CLEANUP_TASK_ALWAYS(kv.second);
    }
    group_bulk_load_pending_replies.clear();

    membership.ballot = 0;

    cleanup_bulk_load_states();

    cleanup_split_states();

    secondary_disk_status.clear();
}

bool primary_context::is_cleaned()
{
    return nullptr == group_check_task && nullptr == reconfiguration_task &&
           nullptr == checkpoint_task && group_check_pending_replies.empty() &&
           nullptr == register_child_task && nullptr == query_child_task &&
           group_bulk_load_pending_replies.empty();
}

void primary_context::do_cleanup_pending_mutations(bool clean_pending_mutations)
{
    if (clean_pending_mutations) {
        write_queue.clear();
    }
}

void primary_context::reset_membership(const partition_configuration &config, bool clear_learners)
{
    statuses.clear();
    if (clear_learners) {
        learners.clear();
    }

    if (config.ballot > membership.ballot)
        next_learning_version = (((uint64_t)config.ballot) << 32) + 1;
    else
        ++next_learning_version;

    membership = config;

    if (membership.primary.is_invalid() == false) {
        statuses[membership.primary] = partition_status::PS_PRIMARY;
    }

    for (auto it = config.secondaries.begin(); it != config.secondaries.end(); ++it) {
        statuses[*it] = partition_status::PS_SECONDARY;
        learners.erase(*it);
    }

    for (auto it = learners.begin(); it != learners.end(); ++it) {
        statuses[it->first] = partition_status::PS_POTENTIAL_SECONDARY;
    }
}

void primary_context::get_replica_config(partition_status::type st,
                                         /*out*/ replica_configuration &config,
                                         uint64_t learner_signature /*= invalid_signature*/)
{
    config.pid = membership.pid;
    config.primary = membership.primary;
    config.ballot = membership.ballot;
    config.status = st;
    config.learner_signature = learner_signature;
}

bool primary_context::check_exist(::dsn::rpc_address node, partition_status::type st)
{
    switch (st) {
    case partition_status::PS_PRIMARY:
        return membership.primary == node;
    case partition_status::PS_SECONDARY:
        return std::find(membership.secondaries.begin(), membership.secondaries.end(), node) !=
               membership.secondaries.end();
    case partition_status::PS_POTENTIAL_SECONDARY:
        return learners.find(node) != learners.end();
    default:
        CHECK(false, "invalid partition_status, status = {}", enum_to_string(st));
        return false;
    }
}

void primary_context::reset_node_bulk_load_states(const rpc_address &node)
{
    secondary_bulk_load_states[node].__set_download_progress(0);
    secondary_bulk_load_states[node].__set_download_status(ERR_OK);
    secondary_bulk_load_states[node].__set_ingest_status(ingestion_status::IS_INVALID);
    secondary_bulk_load_states[node].__set_is_cleaned_up(false);
    secondary_bulk_load_states[node].__set_is_paused(false);
}

void primary_context::cleanup_bulk_load_states()
{
    secondary_bulk_load_states.erase(secondary_bulk_load_states.begin(),
                                     secondary_bulk_load_states.end());
    ingestion_is_empty_prepare_sent = false;
}

void primary_context::cleanup_split_states()
{
    CLEANUP_TASK_ALWAYS(register_child_task)
    CLEANUP_TASK_ALWAYS(query_child_task)

    caught_up_children.clear();
    sync_send_write_request = false;
    split_stopped_secondary.clear();
}

bool primary_context::secondary_disk_abnormal() const
{
    for (const auto &kv : secondary_disk_status) {
        if (kv.second != disk_status::NORMAL) {
            LOG_INFO("partition[{}] secondary[{}] disk space is {}",
                     membership.pid,
                     kv.first.to_string(),
                     enum_to_string(kv.second));
            return true;
        }
    }
    return false;
}

bool secondary_context::cleanup(bool force)
{
    CLEANUP_TASK(checkpoint_task, force)

    if (!force) {
        CLEANUP_TASK_ALWAYS(checkpoint_completed_task);
    } else {
        CLEANUP_TASK(checkpoint_completed_task, force)
    }

    CLEANUP_TASK(catchup_with_private_log_task, force)

    checkpoint_is_running = false;
    return true;
}

bool secondary_context::is_cleaned() { return checkpoint_is_running == false; }

bool potential_secondary_context::cleanup(bool force)
{
    task_ptr t = nullptr;

    if (!force) {
        CLEANUP_TASK_ALWAYS(delay_learning_task)

        CLEANUP_TASK_ALWAYS(learning_task)

        CLEANUP_TASK_ALWAYS(learn_remote_files_completed_task)

        CLEANUP_TASK_ALWAYS(completion_notify_task)
    } else {
        CLEANUP_TASK(delay_learning_task, true)

        CLEANUP_TASK(learning_task, true)

        CLEANUP_TASK(learn_remote_files_completed_task, true)

        CLEANUP_TASK(completion_notify_task, true)
    }

    CLEANUP_TASK(learn_remote_files_task, force)

    CLEANUP_TASK(catchup_with_private_log_task, force)

    learning_version = 0;
    learning_start_ts_ns = 0;
    learning_copy_file_count = 0;
    learning_copy_file_size = 0;
    learning_copy_buffer_size = 0;
    learning_round_is_running = false;
    if (learn_app_concurrent_count_increased) {
        --owner_replica->get_replica_stub()->_learn_app_concurrent_count;
        learn_app_concurrent_count_increased = false;
    }
    learning_start_prepare_decree = invalid_decree;
    first_learn_start_decree = invalid_decree;
    learning_status = learner_status::LearningInvalid;
    return true;
}

bool potential_secondary_context::is_cleaned()
{
    return nullptr == delay_learning_task && nullptr == learning_task &&
           nullptr == learn_remote_files_task && nullptr == learn_remote_files_completed_task &&
           nullptr == catchup_with_private_log_task && nullptr == completion_notify_task;
}

bool partition_split_context::cleanup(bool force)
{
    CLEANUP_TASK(async_learn_task, force)
    if (!force) {
        CLEANUP_TASK_ALWAYS(check_state_task)
    } else {
        CLEANUP_TASK(check_state_task, force)
    }

    splitting_start_ts_ns = 0;
    splitting_start_async_learn_ts_ns = 0;
    splitting_copy_file_count = 0;
    splitting_copy_file_size = 0;
    parent_gpid.set_app_id(0);
    is_prepare_list_copied = false;
    is_caught_up = false;
    return true;
}

bool partition_split_context::is_cleaned() const
{
    return async_learn_task == nullptr && check_state_task == nullptr;
}

} // namespace replication
} // namespace dsn
