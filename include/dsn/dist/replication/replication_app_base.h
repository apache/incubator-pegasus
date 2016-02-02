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

//
// [CHKPT_LEARN]
// after learn the state from learner, apply the learned state to the local app
//
// Or,
//
// [CHKPT_COPY]
// when an app only implement synchonous checkpoint, the replica
// as a primary needs to copy checkpoint from secondaries instead of
// doing checkpointing by itself, in order to not stall the normal
// write operations.
// 
enum chkpt_apply_mode
{
    CHKPT_COPY,
    CHKPT_LEARN
};

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
    template <typename T> static replication_app_base* create(
        ::dsn::replication::replica* replica
        )
    {
        return new T(replica);
    }

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
    virtual ~replication_app_base() { }

    //
    // Interfaces to be implemented by app, 
    // most of them return error code (0 for success).
    //

    // TODO(qinzuoyan): the return value of following interfaces just uses dsn::error_code?
    // or mixed using error code in different space(dsn::error_code vs rocksdb::Status) is confused.

    //
    // Open the app.
    // If `create_new' is true, means "create_if_missing = true && error_if_exists = true".
    // Will be called in a single thread.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    virtual ::dsn::error_code open(bool create_new) = 0;

    //
    // Close the app.
    // If `clear_state' is true, means clear the app state after close it.
    // Must be thread safe.
    //
    virtual error_code close(bool clear_state) = 0;

    //
    // synchonously checkpoint, and update last_durable_decree internally.
    // which stops replication writes to the app concurrently.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    virtual ::dsn::error_code checkpoint() = 0;

    //
    // asynchonously checkpoint, which will not stall the normal write ops.
    // replication layer will check last_durable_decree() later.
    // 
    // Must be thread safe.
    //
    // It is not always necessary for the apps to implement this method,
    // but if it is implemented, the checkpoint logic in replication will be much simpler.
    //
    virtual ::dsn::error_code checkpoint_async() { return ERR_NOT_IMPLEMENTED; }
    
    //
    // prepare an app-specific learning request (on learner, to be sent to learneee
    // and used by method get_checkpoint), so that the learning process is more efficient
    //
    virtual void prepare_learn_request(/*out*/ ::dsn::blob& learn_req) { }

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
    virtual ::dsn::error_code get_checkpoint(
        ::dsn::replication::decree start,
        const ::dsn::blob& learn_req,
        /*out*/ ::dsn::replication::learn_state& state
        ) = 0;

    //
    // [CHKPT_LEARN]
    // after learn the state from learner, apply the learned state to the local app
    //
    // Or,
    //
    // [CHKPT_COPY]
    // when an app only implement synchonous checkpoint, the primary replica
    // needs to copy checkpoint from secondaries instead of
    // doing checkpointing by itself, in order to not stall the normal
    // write operations.
    //
    // Postconditions:
    // * after apply_checkpoint() done, last_committed_decree() == last_durable_decree()
    // 
    virtual ::dsn::error_code apply_checkpoint(::dsn::replication::learn_state& state, chkpt_apply_mode mode) = 0;

    //
    // Query methods.
    //
    virtual ::dsn::replication::decree last_committed_decree() const { return _last_committed_decree.load(); }
    virtual ::dsn::replication::decree last_durable_decree() const { return _last_durable_decree.load(); }
            
public:
    //
    // utility functions to be used by app
    //   
    const char* replica_name() const;
    const std::string& data_dir() const { return _dir_data; }
    const std::string& learn_dir() const { return _dir_learn; }
    bool is_delta_state_learning_supported() const { return _is_delta_state_learning_supported; }

    //
    // set physical error (e.g., disk error) so that the app is dropped by replication later
    //
    void set_physical_error(int err) { _physical_error = err; }
    void set_delta_state_learning_supported() { _is_delta_state_learning_supported = true; }

protected:
    //
    // rpc handler registration
    //
    template<typename T, typename TRequest, typename TResponse> 
    void register_async_rpc_handler(
        dsn_task_code_t code,
        const char* name,
        void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
        );

    void unregister_rpc_handler(dsn_task_code_t code);

    // init the commit decree, usually used by apps when initializing the 
    // state from checkpoints (e.g., update durable and commit decrees)
    void init_last_commit_decree(decree d) { _last_committed_decree = d; }

    // see comments for batch_state, this function is not thread safe
    batch_state get_current_batch_state() { return _batch_state; }

    // reset all states when reopen the app
    void reset_states();
    
private:
    template<typename T, typename TRequest, typename TResponse>
    void internal_rpc_handler(
        binary_reader& reader,
        dsn_message_t response, 
        void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
        );

private:
    // routines for replica internal usage
    friend class replica;
    friend class replica_stub;
    error_code open_internal(replica* r, bool create_new);
    error_code write_internal(mutation_ptr& mu);
    void       dispatch_rpc_call(dsn_task_code_t code, binary_reader& reader, dsn_message_t response);
    const replica_init_info& init_info() const { return _info; }
    error_code update_init_info(replica* r, int64_t shared_log_offset, int64_t private_log_offset);

    void install_perf_counters();

private:
    std::string _dir_data; // ${replica_dir}/data
    std::string _dir_learn;
    replica*    _replica;

    typedef void(*local_rpc_handler)(replication_app_base*, void* cb, binary_reader&, dsn_message_t);
    struct local_rpc_handler_info 
    {
        std::atomic<int> ref_count;
        local_rpc_handler callback;
        char inner_callback[16]; // max method size
    };
    
    std::unordered_map<dsn_task_code_t, local_rpc_handler_info* > _handlers;
    mutable utils::rw_lock_nr     _handlers_lock;

    int         _physical_error; // physical error (e.g., io error) indicates the app needs to be dropped
    bool        _is_delta_state_learning_supported;
    replica_init_info    _info;
    batch_state         _batch_state;
    std::atomic<decree> _last_committed_decree;

    perf_counter_ _app_commit_throughput;
    perf_counter_ _app_commit_latency;
    perf_counter_ _app_commit_decree;
    perf_counter_ _app_req_throughput;

protected:    
    std::atomic<decree> _last_durable_decree;
};

typedef replication_app_base* (*replica_app_factory)(replica*);
extern void register_replica_provider(replica_app_factory f, const char* name);

template<typename T>
inline void register_replica_provider(const char* name)
{
    register_replica_provider(&replication_app_base::template create<T>, name);
}

//------------------ inline implementation ---------------------
template<typename T, typename TRequest, typename TResponse>
inline void replication_app_base::register_async_rpc_handler(
    dsn_task_code_t code,
    const char* name,
    void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
    )
{
    local_rpc_handler_info* h = new local_rpc_handler_info;    
    h->ref_count = 0;
    static_assert (sizeof(h->inner_callback) >= sizeof(callback), "");
    memcpy(h->inner_callback, (const void*)&callback, sizeof(callback));
    h->callback = [](replication_app_base* this_, void* cb, binary_reader& reader, dsn_message_t response)
    {
        TRequest req;
        unmarshall(reader, req);

        typedef void (T::*callback_t)(const TRequest&, rpc_replier<TResponse>&);
        callback_t callback;
        memcpy((void*)&callback, (const void*)cb, sizeof(callback_t));
        rpc_replier<TResponse> replier(response);
        (static_cast<T*>(this_)->*callback)(req, replier);
    };

    h->ref_count.fetch_add(1, std::memory_order_relaxed);
    _handlers[code] = h;
}

inline void replication_app_base::unregister_rpc_handler(dsn_task_code_t code)
{
    local_rpc_handler_info* h = nullptr;
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(code);
        if (it == _handlers.end())
            return;
        h = it->second;
        _handlers.erase(it);
    }

    if (1 == h->ref_count.fetch_sub(1, std::memory_order_release))
    {
        delete h;
    }
}


}} // namespace
