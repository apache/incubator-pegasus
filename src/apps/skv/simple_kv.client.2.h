#pragma once
#include "simple_kv.code.definition.h"
#include "simple_kv.types.h"
#include <iostream>
#include <dsn/utility/optional.h>
#include <dsn/tool-api/async_calls.h>

namespace dsn {
namespace replication {
namespace application {
class simple_kv_client2
{
public:
    simple_kv_client2(::dsn::rpc_address server) { _server = server; }

    simple_kv_client2() {}

    virtual ~simple_kv_client2() {}

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_READ ------------
    // - synchronous
    std::pair<::dsn::error_code, std::string>
    read_sync(const std::string &key,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0,
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<std::string>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_SIMPLE_KV_SIMPLE_KV_READ,
                             key,
                             nullptr,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack std::string and std::string
    template <typename TCallback>
    ::dsn::task_ptr read(const std::string &key,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int thread_hash = 0,
                         uint64_t partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_SIMPLE_KV_SIMPLE_KV_READ,
                                key,
                                nullptr,
                                std::forward<TCallback>(callback),
                                timeout,
                                thread_hash,
                                partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_WRITE ------------
    // - synchronous
    std::pair<::dsn::error_code, int32_t>
    write_sync(const kv_pair &pr,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
               int thread_hash = 0,
               uint64_t partition_hash = 0,
               dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<int32_t>(::dsn::rpc::call(server_addr.unwrap_or(_server),
                                                                     RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                                                                     pr,
                                                                     nullptr,
                                                                     empty_rpc_handler,
                                                                     timeout,
                                                                     thread_hash,
                                                                     partition_hash,
                                                                     0));
    }

    // - asynchronous with on-stack kv_pair and int32_t
    template <typename TCallback>
    ::dsn::task_ptr write(const kv_pair &pr,
                          TCallback &&callback,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                          int thread_hash = 0,
                          uint64_t partition_hash = 0,
                          int reply_thread_hash = 0,
                          dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                                pr,
                                nullptr,
                                std::forward<TCallback>(callback),
                                timeout,
                                thread_hash,
                                partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_APPEND ------------
    // - synchronous
    std::pair<::dsn::error_code, int32_t>
    append_sync(const kv_pair &pr,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                int thread_hash = 0,
                uint64_t partition_hash = 0,
                dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<int32_t>(::dsn::rpc::call(server_addr.unwrap_or(_server),
                                                                     RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                                                                     pr,
                                                                     nullptr,
                                                                     empty_rpc_handler,
                                                                     timeout,
                                                                     thread_hash,
                                                                     partition_hash));
    }

    // - asynchronous with on-stack kv_pair and int32_t
    template <typename TCallback>
    ::dsn::task_ptr append(const kv_pair &pr,
                           TCallback &&callback,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                           int thread_hash = 0,
                           uint64_t partition_hash = 0,
                           int reply_thread_hash = 0,
                           dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                                pr,
                                nullptr,
                                std::forward<TCallback>(callback),
                                timeout,
                                thread_hash,
                                partition_hash,
                                reply_thread_hash);
    }

private:
    ::dsn::rpc_address _server;
};
}
}
}
