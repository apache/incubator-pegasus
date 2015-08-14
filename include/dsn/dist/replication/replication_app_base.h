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

//
// replication_app_base is the base class for all app to be replicated using
// this library
// 

# include <dsn/cpp/serverlet.h>
# include <dsn/dist/replication/replication.types.h>
# include <dsn/dist/replication/replication_other_types.h>
# include <dsn/dist/replication/replication.codes.h>

namespace dsn { namespace replication {

using namespace ::dsn::service;

class mutation;
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
    replication_app_base(::dsn::replication::replica* replica);
    virtual ~replication_app_base() {}

    //
    // Interfaces to be implemented by app, most of them return error code.
    //

    //
    // Open the app.
    // If `create_new' is true, means "create_if_missing = true && error_if_exists = true".
    // Will be called in a single thread.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    virtual int  open(bool create_new) = 0;

    //
    // Close the app.
    // If `clear_state' is true, means clear the app state after close it.
    // Must be thread safe.
    //
    virtual int  close(bool clear_state) = 0;

    //
    // Update last_durable_decree internally.
    // If `wait' is true, means wait flush dobne.
    // Must be thread safe.
    //
    // Postconditions:
    // * if `wait' is true, then last_committed_decree() == last_durable_decree()
    //
    virtual int  flush(bool wait) = 0;
    
    //
    // The replication framework may emit empty write request to this app to increase the decree.
    //
    virtual void on_empty_write() { _last_committed_decree++; }

    //
    // Helper routines to accelerate learning.
    // 
    virtual void prepare_learning_request(__out_param ::dsn::blob& learn_req) {}

    // 
    // Learn [start, infinite) from remote replicas.
    // Must be thread safe.
    //
    // Note the files in learn_state are copied from dir /replica@remote/data to dir /replica@local/learn,
    // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to get the
    // full path of the files.
    //
    // Postconditions:
    // * after apply_learn_state() done, last_committed_decree() >= last_durable_decree()
    //
    virtual int  get_learn_state(::dsn::replication::decree start,
            const ::dsn::blob& learn_req, __out_param ::dsn::replication::learn_state& state) = 0;
    virtual int  apply_learn_state(::dsn::replication::learn_state& state) = 0;

    //
    // Query methods.
    //
    virtual ::dsn::replication::decree last_committed_decree() const { return _last_committed_decree.load(); }
    virtual ::dsn::replication::decree last_durable_decree() const { return _last_durable_decree.load(); }
            
public:
    //
    // utility functions to be used by app
    //   
    const std::string& data_dir() const { return _dir_data; }
    const std::string& learn_dir() const { return _dir_learn; }
    //
    // set physical error (e.g., disk error) so that the app is dropped by replication later
    //
    void set_physical_error(int err) { _physical_error = err; }

protected:
    template<typename T, typename TRequest, typename TResponse> 
    void register_async_rpc_handler(
        dsn_task_code_t code,
        const char* name,
        void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
        );

    void unregister_rpc_handler(dsn_task_code_t code);
    
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
    error_code write_internal(mutation_ptr& mu);
    void       dispatch_rpc_call(int code, binary_reader& reader, dsn_message_t response);
    
private:
    std::string _dir_data;
    std::string _dir_learn;
    replica*    _replica;
    std::unordered_map<int, std::function<void(binary_reader&, dsn_message_t)> > _handlers;
    int         _physical_error; // physical error (e.g., io error) indicates the app needs to be dropped

protected:
    std::atomic<decree> _last_committed_decree;
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
    _handlers[code] = std::bind(
        &replication_app_base::internal_rpc_handler<T, TRequest, TResponse>,
        this,
        std::placeholders::_1,
        std::placeholders::_2,
        callback
        );
}

inline void replication_app_base::unregister_rpc_handler(dsn_task_code_t code)
{
    _handlers.erase(code);
}

template<typename T, typename TRequest, typename TResponse>
inline void replication_app_base::internal_rpc_handler(
    binary_reader& reader, 
    dsn_message_t response, 
    void (T::*callback)(const TRequest&, rpc_replier<TResponse>&))
{
    TRequest req;
    unmarshall(reader, req);

    rpc_replier<TResponse> replier(response);
    (static_cast<T*>(this)->*callback)(req, replier);
}

}} // namespace
