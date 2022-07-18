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

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/cpp/pipeline.h>
#include <dsn/dist/replication/replica_base.h>
#include <dsn/dist/replication.h>
#include <dsn/tool-api/zlocks.h>

namespace dsn {
namespace replication {

class duplication_progress
{
public:
    // check if checkpoint has catch up with `_start_point_decree`
    bool checkpoint_has_prepared{false};
    // the maximum decree that's been persisted in meta server
    decree confirmed_decree{invalid_decree};

    // the maximum decree that's been duplicated to remote.
    decree last_decree{invalid_decree};

    duplication_progress &set_last_decree(decree d)
    {
        last_decree = d;
        return *this;
    }

    duplication_progress &set_confirmed_decree(decree d)
    {
        confirmed_decree = d;
        return *this;
    }
};

class load_mutation;
class ship_mutation;
class load_from_private_log;
class replica;
class replica_stub;

// Each replica_duplicator is responsible for one duplication.
// It works in THREAD_POOL_REPLICATION (LPC_DUPLICATE_MUTATIONS),
// sharded by gpid, thus all functions are single-threaded,
// no read lock required (of course write lock is necessary when
// reader could be in other thread).
//
// TODO(wutao1): Optimization for multi-duplication
//               Currently we create duplicator for each duplication.
//               They're isolated even if they share the same private log.
class replica_duplicator : public replica_base, public pipeline::base
{
public:
    replica_duplicator(const duplication_entry &ent, replica *r);

    // This is a blocking call.
    // The thread may be seriously blocked under the destruction.
    // Take care when running in THREAD_POOL_REPLICATION, though
    // duplication removal is extremely rare.
    ~replica_duplicator();

    // Updates this duplication to `next_status`.
    // Not thread-safe.
    void update_status_if_needed(duplication_status::type next_status);

    void update_fail_mode(duplication_fail_mode::type fmode)
    {
        _fail_mode.store(fmode, std::memory_order_relaxed);
    }
    duplication_fail_mode::type fail_mode() const
    {
        return _fail_mode.load(std::memory_order_relaxed);
    }

    dupid_t id() const { return _id; }

    const std::string &remote_cluster_name() const { return _remote_cluster_name; }

    // Thread-safe
    duplication_progress progress() const
    {
        zauto_read_lock l(_lock);
        return _progress;
    }

    // Thread-safe
    error_s update_progress(const duplication_progress &p);

    void prepare_dup();

    void start_dup_log();

    // Pausing duplication will clear all the internal volatile states, thus
    // when next time it restarts, the states will be reinitialized like the
    // server being restarted.
    // It is useful when something went wrong internally.
    void pause_dup_log();

    // Holds its own tracker, so that other tasks
    // won't be effected when this duplication is removed.
    dsn::task_tracker *tracker() { return &_tracker; }

    std::string to_string() const;

    // To ensure mutation logs after start_decree is available
    // for duplication. If not, it means the eventual consistency
    // of duplication is no longer guaranteed due to the missing logs.
    // For current implementation the system will fail fast.
    void verify_start_decree(decree start_decree);

    decree get_max_gced_decree() const;

    // For metric "dup.pending_mutations_count"
    uint64_t get_pending_mutations_count() const;

    duplication_status::type status() const { return _status; };

private:
    friend class duplication_test_base;
    friend class replica_duplicator_test;
    friend class duplication_sync_timer_test;
    friend class load_from_private_log_test;
    friend class ship_mutation_test;

    friend class load_mutation;
    friend class ship_mutation;

    const dupid_t _id;
    const std::string _remote_cluster_name;

    replica *_replica;
    replica_stub *_stub;
    dsn::task_tracker _tracker;

    decree _start_point_decree = invalid_decree;
    duplication_status::type _status{duplication_status::DS_INIT};
    std::atomic<duplication_fail_mode::type> _fail_mode{duplication_fail_mode::FAIL_SLOW};

    // protect the access of _progress.
    mutable zrwlock_nr _lock;
    duplication_progress _progress;

    /// === pipeline === ///
    std::unique_ptr<load_mutation> _load;
    std::unique_ptr<ship_mutation> _ship;
    std::unique_ptr<load_from_private_log> _load_private;
};

typedef std::unique_ptr<replica_duplicator> replica_duplicator_u_ptr;

} // namespace replication
} // namespace dsn
