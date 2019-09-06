// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

    dupid_t id() const { return _id; }

    // Thread-safe
    duplication_progress progress() const
    {
        zauto_read_lock l(_lock);
        return _progress;
    }

    // Thread-safe
    error_s update_progress(const duplication_progress &p);

    void start_dup();

    // Pausing duplication will clear all the internal volatile states, thus
    // when next time it restarts, the states will be reinitialized like the
    // server being restarted.
    // It is useful when something went wrong internally.
    void pause_dup();

    // Holds its own tracker, so that other tasks
    // won't be effected when this duplication is removed.
    dsn::task_tracker *tracker() { return &_tracker; }

    std::string to_string() const;

private:
    friend class replica_duplicator_test;
    friend class duplication_sync_timer_test;
    friend class load_from_private_log_test;

    friend class load_mutation;
    friend class ship_mutation;

    const dupid_t _id;
    const std::string _remote_cluster_name;

    replica *_replica;
    replica_stub *_stub;
    dsn::task_tracker _tracker;

    duplication_status::type _status{duplication_status::DS_INIT};

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
