// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "utils/fmt_logging.h"
#include "runtime/message_utils.h"

#include "replica_duplicator.h"
#include "mutation_batch.h"

namespace dsn {
namespace replication {

/*static*/ constexpr int64_t mutation_batch::PREPARE_LIST_NUM_ENTRIES;

mutation_buffer::mutation_buffer(replica_base *r,
                                 decree init_decree,
                                 int max_count,
                                 mutation_committer committer)
    : prepare_list(r, init_decree, max_count, committer)
{
    auto counter_str = fmt::format("dup_recent_mutation_loss_count@{}", r->get_gpid());
    _counter_dulication_mutation_loss_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());
}

void mutation_buffer::commit(decree d, commit_type ct)
{
    if (d <= last_committed_decree())
        return;

    CHECK_EQ_PREFIX(ct, COMMIT_TO_DECREE_HARD);

    ballot last_bt = 0;
    for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++) {
        mutation_ptr next_committed_mutation = get_mutation_by_decree(d0);
        // The unexpected case as follow: next_committed_decree is out of prepare_list[start~end]
        //
        // last_committed_decree - next_committed_decree
        //                         |                                                  |
        //                        n                                              n+1
        //
        //  [min_decree------max_decree]
        //                |                                |
        //             n+m(m>1)            n+k(k>=m)
        //
        // just LOG_ERROR but not CHECK if mutation loss or other problem, it's different from
        // base class implement. And from the error and perf-counter, we can choose restart
        // duplication
        // or ignore the loss.
        if (next_committed_mutation == nullptr || !next_committed_mutation->is_logged()) {
            LOG_ERROR_PREFIX("mutation[{}] is lost in prepare_list: "
                             "prepare_last_committed_decree={}, prepare_min_decree={}, "
                             "prepare_max_decree={}",
                             d0,
                             last_committed_decree(),
                             min_decree(),
                             max_decree());
            _counter_dulication_mutation_loss_count->set(min_decree() - last_committed_decree());
            // if next_commit_mutation loss, let last_commit_decree catch up  with min_decree, and
            // the next loop will commit from min_decree
            _last_committed_decree = min_decree() - 1;
            return;
        }

        CHECK_GE_PREFIX(next_committed_mutation->data.header.ballot, last_bt);
        _last_committed_decree++;
        last_bt = next_committed_mutation->data.header.ballot;
        _committer(next_committed_mutation);
    }
}

error_s mutation_batch::add(mutation_ptr mu)
{
    if (mu->get_decree() <= _mutation_buffer->last_committed_decree()) {
        // ignore
        return error_s::ok();
    }

    auto old = _mutation_buffer->get_mutation_by_decree(mu->get_decree());
    if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot) {
        // ignore
        return error_s::ok();
    }

    error_code ec = _mutation_buffer->prepare(mu, partition_status::PS_INACTIVE);
    if (ec != ERR_OK) {
        return FMT_ERR(
            ERR_INVALID_DATA,
            "failed to add mutation [err:{}, logged:{}, decree:{}, committed:{}, start_decree:{}]",
            ec.to_string(),
            mu->is_logged(),
            mu->get_decree(),
            mu->data.header.last_committed_decree,
            _start_decree);
    }

    return error_s::ok();
}

decree mutation_batch::last_decree() const { return _mutation_buffer->last_committed_decree(); }

void mutation_batch::set_start_decree(decree d) { _start_decree = d; }

void mutation_batch::reset_mutation_buffer(decree d) { _mutation_buffer->reset(d); }

mutation_tuple_set mutation_batch::move_all_mutations()
{
    // free the internal space
    _mutation_buffer->truncate(last_decree());
    _total_bytes = 0;
    return std::move(_loaded_mutations);
}

mutation_batch::mutation_batch(replica_duplicator *r) : replica_base(r)
{
    // Prepend a special tag identifying this is a mutation_batch,
    // so `dxxx_replica` logging in prepare_list will print along with its real caller.
    // This helps for debugging.
    replica_base base(
        r->get_gpid(), std::string("mutation_batch@") + r->replica_name(), r->app_name());
    _mutation_buffer =
        make_unique<mutation_buffer>(&base, 0, PREPARE_LIST_NUM_ENTRIES, [this](mutation_ptr &mu) {
            // committer
            add_mutation_if_valid(mu, _start_decree);
        });

    // start duplication from confirmed_decree
    _mutation_buffer->reset(r->progress().confirmed_decree);
}

void mutation_batch::add_mutation_if_valid(mutation_ptr &mu, decree start_decree)
{
    if (mu->get_decree() < start_decree) {
        // ignore
        return;
    }
    for (mutation_update &update : mu->data.updates) {
        // ignore WRITE_EMPTY
        if (update.code == RPC_REPLICATION_WRITE_EMPTY) {
            continue;
        }
        // Ignore non-idempotent writes.
        // Normally a duplicating replica will reply non-idempotent writes with
        // ERR_OPERATION_DISABLED, but there could still be a mutation written
        // before the duplication was added.
        // To ignore means this write will be lost, which is acceptable under this rare case.
        if (!task_spec::get(update.code)->rpc_request_is_write_idempotent) {
            continue;
        }
        blob bb;
        if (update.data.buffer() != nullptr) {
            bb = std::move(update.data);
        } else {
            bb = blob::create_from_bytes(update.data.data(), update.data.length());
        }

        _total_bytes += bb.length();
        _loaded_mutations.emplace(
            std::make_tuple(mu->data.header.timestamp, update.code, std::move(bb)));
    }
}

} // namespace replication
} // namespace dsn
