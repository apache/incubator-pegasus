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

#include "prepare_list.h"

#include <memory>
#include <utility>

#include "common/replication_enums.h"
#include "consensus_types.h"
#include "replica/mutation_cache.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"

namespace dsn {
namespace replication {

prepare_list::prepare_list(replica_base *r,
                           decree init_decree,
                           int max_count,
                           mutation_committer committer)
    : mutation_cache(init_decree, max_count), replica_base(r)
{
    _committer = std::move(committer);
    _last_committed_decree = init_decree;
}

prepare_list::prepare_list(replica_base *r, const prepare_list &parent_plist)
    : mutation_cache(parent_plist), replica_base(r)
{
    _committer = parent_plist._committer;
    _last_committed_decree = parent_plist._last_committed_decree;
}

void prepare_list::reset(decree init_decree)
{
    _last_committed_decree = init_decree;
    mutation_cache::reset(init_decree, true);
}

void prepare_list::truncate(decree init_decree)
{
    while (min_decree() <= init_decree && count() > 0) {
        pop_min();
    }

    if (count() == 0) {
        mutation_cache::reset(init_decree, true);
    }

    _last_committed_decree = init_decree;
}

error_code prepare_list::prepare(mutation_ptr &mu,
                                 partition_status::type status,
                                 bool pop_all_committed_mutations,
                                 bool secondary_commit)
{
    decree d = mu->data.header.decree;
    CHECK_GT_PREFIX(d, last_committed_decree());

    ADD_POINT(mu->_tracer);
    switch (status) {
    case partition_status::PS_PRIMARY:
        // pop committed mutations if buffer is full or pop_all_committed_mutations = true
        while ((d - min_decree() >= capacity() || pop_all_committed_mutations) &&
               last_committed_decree() > min_decree()) {
            pop_min();
        }
        return mutation_cache::put(mu);

    case partition_status::PS_SECONDARY:
    case partition_status::PS_POTENTIAL_SECONDARY:
        // all mutations with lower decree must be ready
        if (secondary_commit) {
            commit(mu->data.header.last_committed_decree, COMMIT_TO_DECREE_HARD);
        }
        // pop committed mutations if buffer is full or pop_all_committed_mutations = true
        while ((d - min_decree() >= capacity() || pop_all_committed_mutations) &&
               last_committed_decree() > min_decree()) {
            pop_min();
        }
        CHECK_EQ_PREFIX_MSG(mutation_cache::put(mu), ERR_OK, "mutation_cache::put failed");
        return ERR_OK;

    //// delayed commit - only when capacity is an issue
    // case partition_status::PS_POTENTIAL_SECONDARY:
    //    while (true)
    //    {
    //        error_code err = mutation_cache::put(mu);
    //        if (err == ERR_CAPACITY_EXCEEDED)
    //        {
    //            CHECK_GE(mu->data.header.last_committed_decree, min_decree());
    //            commit (min_decree(), true);
    //            pop_min();
    //        }
    //        else
    //            break;
    //    }
    //    CHECK_EQ(err, ERR_OK);
    //    return ERR_OK;

    case partition_status::PS_INACTIVE: // only possible during init
        if (mu->data.header.last_committed_decree > max_decree()) {
            reset(mu->data.header.last_committed_decree);
        } else if (mu->data.header.last_committed_decree > _last_committed_decree) {
            // all mutations with lower decree must be ready
            commit(mu->data.header.last_committed_decree, COMMIT_TO_DECREE_HARD);
        }
        // pop committed mutations if buffer is full
        while (d - min_decree() >= capacity() && last_committed_decree() > min_decree()) {
            pop_min();
        }
        CHECK_EQ_PREFIX_MSG(mutation_cache::put(mu), ERR_OK, "mutation_cache::put failed");
        return ERR_OK;

    default:
        CHECK(false, "invalid partition_status, status = {}", enum_to_string(status));
        return ERR_OK;
    }
}

//
// ordered commit
//
void prepare_list::commit(decree d, commit_type ct)
{
    if (d <= last_committed_decree())
        return;

    ballot last_bt = 0;
    switch (ct) {
    case COMMIT_TO_DECREE_HARD: {
        for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++) {
            mutation_ptr mu = get_mutation_by_decree(d0);

            CHECK_PREFIX_MSG(
                mu != nullptr && mu->is_logged(), "mutation {} is missing in prepare list", d0);
            CHECK_GE_PREFIX(mu->data.header.ballot, last_bt);

            _last_committed_decree++;
            last_bt = mu->data.header.ballot;
            _committer(mu);
        }

        return;
    }
    case COMMIT_TO_DECREE_SOFT: {
        for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++) {
            mutation_ptr mu = get_mutation_by_decree(d0);
            if (mu != nullptr && mu->is_ready_for_commit() && mu->data.header.ballot >= last_bt) {
                _last_committed_decree++;
                last_bt = mu->data.header.ballot;
                _committer(mu);
            } else
                break;
        }

        return;
    }
    case COMMIT_ALL_READY: {
        if (d != last_committed_decree() + 1)
            return;

        int count = 0;
        mutation_ptr mu = get_mutation_by_decree(last_committed_decree() + 1);

        while (mu != nullptr && mu->is_ready_for_commit() && mu->data.header.ballot >= last_bt) {
            _last_committed_decree++;
            last_bt = mu->data.header.ballot;
            _committer(mu);
            count++;
            mu = mutation_cache::get_mutation_by_decree(_last_committed_decree + 1);
        }

        return;
    }
    default:
        CHECK(false, "invalid commit type {}", ct);
    }

    return;
}
} // namespace replication
} // namespace dsn
