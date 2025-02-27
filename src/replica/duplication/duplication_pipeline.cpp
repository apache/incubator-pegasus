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

#include "duplication_pipeline.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "load_from_private_log.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/mutation_log.h"
#include "replica/replica.h"
#include "utils/autoref_ptr.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DEFINE_uint64(
    replication,
    dup_no_mutation_load_delay_ms,
    100,
    "The duration of the delay until the next execution if there is no mutation to be loaded.");
DSN_TAG_VARIABLE(dup_no_mutation_load_delay_ms, FT_MUTABLE);

METRIC_DEFINE_counter(replica,
                      dup_shipped_bytes,
                      dsn::metric_unit::kBytes,
                      "The shipped size of private log for dup");

namespace dsn {

namespace replication {

//                     //
// mutation_duplicator //
//                     //

/*static*/ std::function<std::unique_ptr<mutation_duplicator>(
    replica_base *, std::string_view /*remote cluster*/, std::string_view /*app*/)>
    mutation_duplicator::creator;

//               //
// load_mutation //
//               //

void load_mutation::run()
{
    decree last_decree = _duplicator->progress().last_decree;
    _start_decree = last_decree + 1;

    // Load the mutations from plog that have been committed recently, if any.
    const auto max_plog_committed_decree =
        std::min(_replica->private_log()->max_decree_on_disk(), _replica->last_applied_decree());
    if (_start_decree > max_plog_committed_decree) {
        // Wait for a while if no mutation was added.
        repeat(std::chrono::milliseconds(FLAGS_dup_no_mutation_load_delay_ms));
        return;
    }

    _log_on_disk->set_start_decree(_start_decree);
    _log_on_disk->async();
}

load_mutation::~load_mutation() = default;

load_mutation::load_mutation(replica_duplicator *duplicator,
                             replica *r,
                             load_from_private_log *load_private)
    : replica_base(r), _log_on_disk(load_private), _replica(r), _duplicator(duplicator)
{
}

//               //
// ship_mutation //
//               //

void ship_mutation::ship(mutation_tuple_set &&in)
{
    _mutation_duplicator->duplicate(std::move(in), [this](size_t total_shipped_size) mutable {
        update_progress();
        METRIC_VAR_INCREMENT_BY(dup_shipped_bytes, total_shipped_size);
        step_down_next_stage();
    });
}

void ship_mutation::run(decree &&last_decree, mutation_tuple_set &&in)
{
    _last_decree = last_decree;

    if (in.empty()) {
        update_progress();
        step_down_next_stage();
        return;
    }

    ship(std::move(in));
}

void ship_mutation::update_progress()
{
    CHECK_EQ_PREFIX(
        _duplicator->update_progress(duplication_progress().set_last_decree(_last_decree)),
        error_s::ok());

    // committed decree never decreases
    decree last_committed_decree = _replica->last_committed_decree();
    CHECK_GE_PREFIX(last_committed_decree, _last_decree);
}

ship_mutation::ship_mutation(replica_duplicator *duplicator)
    : replica_base(duplicator),
      _duplicator(duplicator),
      _replica(duplicator->_replica),
      _stub(duplicator->_replica->get_replica_stub()),
      METRIC_VAR_INIT_replica(dup_shipped_bytes)
{
    _mutation_duplicator = new_mutation_duplicator(
        duplicator, duplicator->remote_cluster_name(), duplicator->remote_app_name());
    _mutation_duplicator->set_task_environment(duplicator);
}

} // namespace replication
} // namespace dsn
