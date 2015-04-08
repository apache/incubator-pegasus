/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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

#include "replication_common.h"


namespace dsn { namespace replication {

using namespace dsn::service;

class replication_app_config
{
public:
    virtual bool initialize(configuration_ptr config) = 0;

    // TODO: common configs here
};

class replication_app_base
{
public:
    replication_app_base(replica* replica, const replication_app_config* config);
    virtual ~replication_app_base() {}

    //
    // interfaces to be implemented by app
    // all return values are error code
    //
    virtual int  open(bool create_new) = 0; // singel threaded
    virtual int  close(bool clear_state) = 0; // must be thread-safe

    // update _last_durable_decree internally
    virtual int  compact(bool force) = 0;  // must be thread-safe
    
    //
    // helper routines to accelerate learning
    // 
    virtual void prepare_learning_request(__out_param blob& learnRequest) {};
    virtual int  get_learn_state(decree start, const blob& learnRequest, __out_param learn_state& state) = 0;  // must be thread-safe
    virtual int  apply_learn_state(learn_state& state) = 0;  // must be thread-safe, and last_committed_decree must equal to last_durable_decree after learning
            
public:
    //
    // utility functions to be used by app
    //   
    decree last_committed_decree() const { return _last_committed_decree; }
    decree last_durable_decree() const  { return _last_durable_decree; }
    const std::string& dir() const {return _dir;}

protected:
    template<typename T, typename TRequest, typename TResponse> 
    void register_async_rpc_handler(
        task_code code,
        const char* name,
        void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
        );

    void unregister_rpc_handler(task_code code);
    
private:
    template<typename T, typename TRequest, typename TResponse>
    void internal_rpc_handler(
        message_ptr& request, 
        message_ptr& response, 
        void (T::*callback)(const TRequest&, rpc_replier<TResponse>&)
        );

private:
    // routines for replica internal usage
    friend class replica;
    int  write_internal(mutation_ptr& mu, bool ack_client);
    void dispatch_rpc_call(int code, message_ptr& request, bool ack_client);

protected:
    std::atomic<decree> _last_durable_decree;
    std::atomic<decree> _last_committed_decree;

private:
    std::string _dir;
    replica*    _replica;
    std::map<int, std::function<void(message_ptr&, message_ptr&)> > _handlers;
};


//------------------ inline implementation ---------------------
template<typename T, typename TRequest, typename TResponse>
inline void replication_app_base::register_async_rpc_handler(
    task_code code,
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

inline void replication_app_base::unregister_rpc_handler(task_code code)
{
    _handlers.erase(code);
}

template<typename T, typename TRequest, typename TResponse>
inline void replication_app_base::internal_rpc_handler(
    message_ptr& request, 
    message_ptr& response, 
    void (T::*callback)(const TRequest&, rpc_replier<TResponse>&))
{
    TRequest req;
    unmarshall(request->reader(), req);

    rpc_replier<TResponse> replier(request, response);
    (static_cast<T*>(this)->*callback)(req, replier);
}

}} // namespace
