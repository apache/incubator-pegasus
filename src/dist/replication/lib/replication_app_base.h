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
 *     interface for apps to be replicated using rDSN
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

//
// replication_app_base is the base class for all app to be replicated using
// this library
// 

# include <dsn/cpp/serverlet.h>
# include <dsn/dist/replication/replication.types.h>
# include <dsn/dist/replication/replication_other_types.h>
# include <dsn/dist/replication/replication.codes.h>
# include <dsn/cpp/perf_counter_.h>
# include <atomic>

namespace dsn { namespace replication {

class mutation;

class replica_init_info
{
public:
    int32_t magic;
    int32_t crc;
    ballot  init_ballot;
    decree  init_durable_decree;
    int64_t init_offset_in_shared_log;
    int64_t init_offset_in_private_log;

public:
    replica_init_info() { memset((void*)this, 0, sizeof(*this)); }
    error_code load(const char* file);
    error_code store(const char* file);
    std::string to_string();
};

class replica_app_info
{
private:
    dsn::app_info *_app;

public:
    replica_app_info(dsn::app_info* app) { _app = app; }
    error_code load(const char* file);
    error_code store(const char* file);
};

class replication_app_base
{
public:
    explicit replication_app_base(::dsn::replication::replica* replica);
    ~replication_app_base() {}

    //
    // Open the app.
    //
    ::dsn::error_code open();

    //
    // Close the app.
    // If `clear_state' is true, means clear the app state after close it.
    //
    // Must be thread safe.
    //
    ::dsn::error_code close(bool clear_state);

    //
    // synchonously checkpoint, and update last_durable_decree internally.
    // which stops replication writes to the app concurrently.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    ::dsn::error_code sync_checkpoint()
    {
        return _callbacks.calls.sync_checkpoint(_app_context_callbacks, _last_committed_decree.load());
    }

    //
    // asynchonously checkpoint, which will not stall the normal write ops.
    // replication layer will check last_durable_decree() later.
    // 
    // Must be thread safe.
    //
    // It is not always necessary for the apps to implement this method,
    // but if it is implemented, the checkpoint logic in replication will be much simpler.
    //
    ::dsn::error_code async_checkpoint() 
    { 
        return _callbacks.calls.async_checkpoint(_app_context_callbacks, _last_committed_decree.load());
    }
    
    //
    // prepare an app-specific learning request (on learner, to be sent to learneee
    // and used by method get_checkpoint), so that the learning process is more efficient
    //
    ::dsn::error_code prepare_get_checkpoint(/*out*/ ::dsn::blob& learn_req);

    // 
    // Learn [start, infinite) from remote replicas (learner)
    //
    // Must be thread safe.
    //
    // The learned checkpoint can be a complete checkpoint (0, infinite), or a delta checkpoint
    // [start, infinite), depending on the capability of the underlying implementation.
    // 
    // Note the files in learn_state are copied from dir /replica@remote/data to dir /replica@local/learn,
    // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to get the
    // full path of the files.
    //
    ::dsn::error_code get_checkpoint(int64_t learn_start, const ::dsn::blob& learn_request, /*out*/ learn_state& state);

    //
    // [DSN_CHKPT_LEARN]
    // after learn the state from learner, apply the learned state to the local app
    //
    // Or,
    //
    // [DSN_CHKPT_COPY]
    // when an app only implement synchonous checkpoint, the primary replica
    // needs to copy checkpoint from secondaries instead of
    // doing checkpointing by itself, in order to not stall the normal
    // write operations.
    //
    // Postconditions:
    // * if mode == DSN_CHKPT_COPY, after apply_checkpoint() succeed:
    //   last_durable_decree() == state.to_decree_included
    // * if mode == DSN_CHKPT_LEARN, after apply_checkpoint() succeed:
    //   last_committed_decree() == last_durable_decree() == state.to_decree_included
    // 
    ::dsn::error_code apply_checkpoint(dsn_chkpt_apply_mode mode, const learn_state& state);

    //
    // Query methods.
    //    
    ::dsn::replication::decree last_durable_decree() 
    {
        return _app_context_callbacks ? _callbacks.calls.get_last_checkpoint_decree(_app_context_callbacks) : 0;
    }

public:
    //
    // utility functions to be used by app
    //   
    const char* replica_name() const;
    const std::string& data_dir() const { return _dir_data; }
    const std::string& learn_dir() const { return _dir_learn; }
    ::dsn::replication::decree last_committed_decree() const { return _last_committed_decree.load(); }
    void* app_context() { return _app_context; }
    void reset_counters_after_learning();
    
private:
    // routines for replica internal usage
    friend class replica;
    friend class replica_stub;
    
    ::dsn::error_code open_internal(replica* r);
    ::dsn::error_code open_new_internal(replica* r, int64_t shared_log_start, int64_t private_log_start);
    ::dsn::error_code write_internal(mutation_ptr& mu);

    const replica_init_info& init_info() const { return _info; }
    ::dsn::error_code update_init_info(
        replica* r, 
        int64_t shared_log_offset, 
        int64_t private_log_offset, 
        int64_t durable_decree
        );

    void install_perf_counters();

private:
    void* _app_context;
    void* _app_context_callbacks;
    dsn_app_callbacks _callbacks;

private:
    std::string _dir_data; // ${replica_dir}/data
    std::string _dir_learn; // ${replica_dir}/learn
    replica*    _replica;
    std::atomic<int64_t> _last_committed_decree;
    replica_init_info _info;
    
    perf_counter_ _app_commit_throughput;
    perf_counter_ _app_commit_latency;
    perf_counter_ _app_commit_decree;
    perf_counter_ _app_req_throughput;
};

//------------------ inline implementation ---------------------

}} // namespace
