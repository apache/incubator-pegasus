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

#pragma once

#include <gtest/gtest_prod.h>
#include <stddef.h>
#include <stdint.h>
#include <chrono>

#include "common/replication_other_types.h"
#include "mutation_batch.h"
#include "replica/duplication/mutation_duplicator.h"
#include "replica/log_file.h"
#include "replica/mutation_log.h"
#include "replica/replica_base.h"
#include "runtime/pipeline.h"
#include "utils/autoref_ptr.h"
#include "utils/chrono_literals.h"
#include "utils/metrics.h"

namespace dsn {
namespace replication {

class replica;
class replica_duplicator;
class replica_stub;

/// Loads mutations from private log into memory.
/// It works in THREAD_POOL_REPLICATION_LONG (LPC_DUPLICATION_LOAD_MUTATIONS),
/// which permits tasks to be executed in a blocking way.
/// NOTE: The resulted `mutation_tuple_set` may be empty.
class load_from_private_log final : public replica_base,
                                    public pipeline::when<>,
                                    public pipeline::result<decree, mutation_tuple_set>
{
public:
    load_from_private_log(replica *r, replica_duplicator *dup);

    // Start loading block from private log file.
    // The loaded mutations will be passed down to `ship_mutation`.
    void run() override;

    void set_start_decree(decree start_decree);

    /// ==== Implementation ==== ///

    /// Find the log file that contains `_start_decree`.
    void find_log_file_to_start();

    void replay_log_block();

    // Switches to the log file with index = current_log_index + 1.
    // Returns true if succeeds.
    bool switch_to_next_log_file();

    void start_from_log_file(log_file_ptr f);

    bool will_fail_skip() const;
    bool will_fail_fast() const;

    void TEST_set_repeat_delay(std::chrono::milliseconds delay) { _repeat_delay = delay; }

    METRIC_DEFINE_VALUE(dup_log_file_load_failed_count, int64_t)
    METRIC_DEFINE_VALUE(dup_log_file_load_skipped_bytes, int64_t)

    static constexpr int MAX_ALLOWED_BLOCK_REPEATS{3};
    static constexpr int MAX_ALLOWED_FILE_REPEATS{10};

private:
    void find_log_file_to_start(const mutation_log::log_file_map_by_index &log_files);

private:
    friend class load_from_private_log_test;
    friend class load_fail_mode_test;
    FRIEND_TEST(load_fail_mode_test, fail_skip);
    FRIEND_TEST(load_fail_mode_test, fail_slow);
    FRIEND_TEST(load_fail_mode_test, fail_skip_real_corrupted_file);

    mutation_log_ptr _private_log;
    replica_duplicator *_duplicator;
    replica_stub *_stub;

    log_file_ptr _current;

    size_t _start_offset{0};
    int64_t _current_global_end_offset{0};
    mutation_batch _mutation_batch;

    // How many times it repeats reading from current block but failed.
    int _err_block_repeats_num{0};
    // How many times it repeats reading current log file but failed.
    int _err_file_repeats_num{0};

    decree _start_decree{0};

    METRIC_VAR_DECLARE_counter(dup_log_file_load_failed_count);
    METRIC_VAR_DECLARE_counter(dup_log_file_load_skipped_bytes);
    METRIC_VAR_DECLARE_counter(dup_log_read_bytes);
    METRIC_VAR_DECLARE_counter(dup_log_read_mutations);

    std::chrono::milliseconds _repeat_delay{10_s};
};

} // namespace replication
} // namespace dsn
