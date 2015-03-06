#pragma once

//
// replication_app_base is the base class for all app to be replicated using
// this library
// 

#include "replication_common.h"


namespace rdsn { namespace replication {

using namespace rdsn::service;

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
    // application state write and read
    // @requests: update requests – they are batched and delivered to the application
    // @decree: a version number that is used to align between replication and application
    // @ackClient: when it is true, the application needs to reply to the client by invoking 
    //             rpc_response<TResponse>(request, response);
    virtual int  write(std::list<message_ptr>& requests, decree decree, bool ackClient) = 0; // single-threaded
    virtual void read(const client_read_request& meta, message_ptr& request) = 0; // must be thread-safe
    
    
    virtual int  open(bool createNew) = 0; // singel threaded
    virtual int  close(bool clearState) = 0; // must be thread-safe
    virtual int  compact(bool force) = 0;  // must be thread-safe
    
    // helper routines to accelerate learning
    virtual void PrepareLearningRequest(__out utils::blob& learnRequest) {};
    virtual int  get_learn_state(decree start, const utils::blob& learnRequest, __out learn_state& state) = 0;  // must be thread-safe
    virtual int  apply_learn_state(learn_state& state) = 0;  // must be thread-safe, and last_committed_decree must equal to last_durable_decree after learning

    virtual decree last_committed_decree() const = 0;  // must be thread-safe
    virtual decree last_durable_decree() const = 0;  // must be thread-safe

public:
    //
    // utility functions to be used by app
    //   
    template<typename T> void rpc_response(message_ptr& request, const T& response);
    message_ptr PrepareRpcResponse(message_ptr& request);
    void rpc_response(message_ptr& response);
    const std::string& dir() const {return _dir;}

private:
    // routines for replica internal usage
    friend class replica;
    int    WriteInternal(mutation_ptr& mu, bool ackClient);
    void   WriteReplicationResponse(message_ptr& response);
        
private:
    std::string _dir;
    replica*    _replica;
};


//------------------ inline implementation ---------------------
inline message_ptr replication_app_base::PrepareRpcResponse(message_ptr& request)
{
    message_ptr resp = request->create_response();
    WriteReplicationResponse(resp);
    return resp;
}

inline void replication_app_base::rpc_response(message_ptr& response)
{
    rdsn::service::rpc::reply(response);
}

template<typename T> 
inline void replication_app_base::rpc_response(message_ptr& request, const T& response)
{
    auto resp = PrepareRpcResponse(request);
    marshall(resp, response);
    rpc_response(resp);
}

}} // namespace
