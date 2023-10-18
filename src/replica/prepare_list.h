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

#include <functional>

#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "mutation_cache.h"
#include "replica/mutation.h"
#include "replica/replica_base.h"
#include "utils/error_code.h"
#include "utils/fmt_utils.h"

namespace dsn {
namespace replication {

enum commit_type
{
    COMMIT_TO_DECREE_HARD, // commit (last_committed, ...<mutations must be is_commit_ready..., d]
    COMMIT_TO_DECREE_SOFT, // commit (last_committed, ...<if is_commit_ready mutations>.., d]
    COMMIT_ALL_READY       // commit (last_committed, ...<all is_commit_ready mutations> ...]
    // - only valid when partition_status::PS_SECONDARY or partition_status::PS_PRIMARY
};
USER_DEFINED_ENUM_FORMATTER(commit_type)

// prepare_list origins from the concept of `prepared list` in PacificA.
// It stores an continuous and ordered list of mutations.
// The prefix of the prepared list up to a `committed point` is regarded as committed.
// The prepare_list only stores the most updated part (the uncommitted suffix) of prepared list,
// say, the committed prefix will be truncated automatically.
class prepare_list : public mutation_cache, private replica_base
{
public:
    typedef std::function<void(mutation_ptr &)> mutation_committer;

public:
    prepare_list(replica_base *r, decree init_decree, int max_count, mutation_committer committer);
    prepare_list(replica_base *r, const prepare_list &parent_plist);

    decree last_committed_decree() const { return _last_committed_decree; }
    void reset(decree init_decree);
    void truncate(decree init_decree);
    void set_committer(mutation_committer committer) { _committer = committer; }

    //
    // for two-phase commit
    //
    // if pop_all_committed_mutations = true, pop all committed mutations, will only used during
    // bulk load ingestion
    // if secondary_commit = true, and status is secondary or potential secondary, previous logs
    // will be committed
    // TODO(yingchun): should check return values for all callers by adding WARN_UNUSED_RESULT.
    error_code prepare(mutation_ptr &mu,
                       partition_status::type status,
                       bool pop_all_committed_mutations = false,
                       bool secondary_commit = true);
    virtual void commit(decree decree, commit_type ct); // ordered commit

    virtual ~prepare_list() = default;

private:
    friend class mutation_buffer;
    decree _last_committed_decree;
    mutation_committer _committer;
};

} // namespace replication
} // namespace dsn
