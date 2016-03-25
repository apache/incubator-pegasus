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

namespace dsn { namespace replication {

class mutation;

class replica_init_info
{
public:
    int32_t magic;
    int32_t crc;
    ballot  init_ballot;
    decree  init_decree;
    int64_t init_offset_in_shared_log;
    int64_t init_offset_in_private_log;

public:
    replica_init_info() { memset((void*)this, 0, sizeof(*this)); }
    error_code load(const char* file);
    error_code store(const char* file);
};

class replication_app_base
{
public:
    // requests may be batched by replication and fed to replication
    // app with the same decree, in this case, apps may need to 
    // be aware of the batch state for the current request
    enum batch_state
    {
        BS_NOT_BATCH,  // request is not batched
        BS_BATCH,      // request is batched but not the last in the same batch
        BS_BATCH_LAST  // request is batched and the last in the same batch
    };

public:
    replication_app_base(::dsn::replication::replica* replica);
    ~replication_app_base() { }

    ::dsn::error_code open();

    //
    // Close the app.
    // If `clear_state' is true, means clear the app state after close it.
    // Must be thread safe.
    //
    error_code close(bool clear_state)
    {
        return dsn_layer1_app_destroy(_app_context, clear_state);
    }

    //
    // synchonously checkpoint, and update last_durable_decree internally.
    // which stops replication writes to the app concurrently.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    ::dsn::error_code checkpoint()
    {
        return dsn_layer1_app_checkpoint(_app_context);
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
    ::dsn::error_code checkpoint_async() 
    { 
        return dsn_layer1_app_checkpoint_async(_app_context);
    }
    
    //
    // prepare an app-specific learning request (on learner, to be sent to learneee
    // and used by method get_checkpoint), so that the learning process is more efficient
    //
    void prepare_learn_request(/*out*/ ::dsn::blob& learn_req);

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
    ::dsn::error_code get_checkpoint(
        int64_t start,
        const ::dsn::blob& learn_request,
        /* out */ learn_state& state
        );

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
    // * after apply_checkpoint() done, last_committed_decree() == last_durable_decree()
    // 
    ::dsn::error_code apply_checkpoint(const learn_state& state, dsn_chkpt_apply_mode mode);

    //
    // Query methods.
    //    
    ::dsn::replication::decree last_durable_decree() 
    {
        return _app_info->info.type1.last_durable_decree;
    }

public:
    //
    // utility functions to be used by app
    //   
    const char* replica_name() const;
    const std::string& data_dir() const { return _dir_data; }
    const std::string& learn_dir() const { return _dir_learn; }
    bool is_delta_state_learning_supported() const { return _is_delta_state_learning_supported; }
    ::dsn::replication::decree last_committed_decree() const { return _app_info->info.type1.last_committed_decree; }
    void* app_context() { return _app_context; }
    //
    // set physical error (e.g., disk error) so that the app is dropped by replication later
    //
    void set_physical_error(int err) { _physical_error = err; }
    void set_delta_state_learning_supported() { _is_delta_state_learning_supported = true; }

protected:

    // init the commit decree, usually used by apps when initializing the 
    // state from checkpoints (e.g., update durable and commit decrees)
    void init_last_commit_decree(decree d) { _app_info->info.type1.last_committed_decree = d; }

    // see comments for batch_state, this function is not thread safe
    batch_state get_current_batch_state() { return _batch_state; }

    // reset all states when reopen the app
    void reset_states();
    
private:
    // routines for replica internal usage
    friend class replica;
    friend class replica_stub;
    
    error_code open_internal(replica* r, bool create_new);
    error_code write_internal(mutation_ptr& mu);

    const replica_init_info& init_info() const { return _info; }
    error_code update_init_info(replica* r, int64_t shared_log_offset, int64_t private_log_offset);

    void install_perf_counters();

private:
    void* _app_context;

private:
    std::string _dir_data; // ${replica_dir}/data
    std::string _dir_learn;
    replica*    _replica;

    int         _physical_error; // physical error (e.g., io error) indicates the app needs to be dropped
    bool        _is_delta_state_learning_supported;
    replica_init_info    _info;
    batch_state         _batch_state;
    dsn_app_info *_app_info;

    perf_counter_ _app_commit_throughput;
    perf_counter_ _app_commit_latency;
    perf_counter_ _app_commit_decree;
    perf_counter_ _app_req_throughput;
};

//------------------ inline implementation ---------------------

}} // namespace
