# pragma once
# include "simple_kv.code.definition.h"
# include <iostream>


namespace dsn { namespace replication { namespace application { 
class simple_kv_client2 
    : public virtual ::dsn::clientlet
{
public:
    simple_kv_client2(::dsn::rpc_address server) { _server = server; }
    simple_kv_client2() { }
    virtual ~simple_kv_client2() {}

    // from requests to partition index
    // PLEASE DO RE-DEFINE THEM IN A SUB CLASS!!!
    virtual uint64_t get_key_hash(const std::string& key) { return 0; }
    virtual uint64_t get_key_hash(const kv_pair& key) { return 0; }
 
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_READ ------------
    // - synchronous 
    std::pair< ::dsn::error_code, std::string> read_sync(
        const std::string& key, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::wait_and_unwrap<std::string>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_SIMPLE_KV_SIMPLE_KV_READ,
                key,
                nullptr,
                empty_callback,
                hash,
                timeout,
                0,
                get_key_hash(key)
                )
            );
    }
    
    // - asynchronous with on-stack std::string and std::string  
    template<typename TCallback>
    ::dsn::task_ptr read(
        const std::string& key, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_SIMPLE_KV_SIMPLE_KV_READ, 
                    key, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash,
                    get_key_hash(key)
                    );
    }
 
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_WRITE ------------
    // - synchronous 
    std::pair< ::dsn::error_code, int32_t> write_sync(
        const kv_pair& pr, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::wait_and_unwrap<int32_t>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                pr,
                nullptr,
                empty_callback,
                hash,
                timeout,
                0,
                get_key_hash(pr)
                )
            );
    }
    
    // - asynchronous with on-stack kv_pair and int32_t  
    template<typename TCallback>
    ::dsn::task_ptr write(
        const kv_pair& pr, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_SIMPLE_KV_SIMPLE_KV_WRITE, 
                    pr, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash,
                    get_key_hash(pr)
                    );
    }
 
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_APPEND ------------
    // - synchronous 
    std::pair< ::dsn::error_code, int32_t> append_sync(
        const kv_pair& pr, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::wait_and_unwrap<int32_t>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                pr,
                nullptr,
                empty_callback,
                hash,
                timeout,
                0,
                get_key_hash(pr)
                )
            );
    }
    
    // - asynchronous with on-stack kv_pair and int32_t  
    template<typename TCallback>
    ::dsn::task_ptr append(
        const kv_pair& pr, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_SIMPLE_KV_SIMPLE_KV_APPEND, 
                    pr, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash,
                    get_key_hash(pr)
                    );
    }

private:
    ::dsn::rpc_address _server;
};

} } } 