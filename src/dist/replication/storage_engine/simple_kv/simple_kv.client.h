#pragma once
#include <iostream>
#include <dsn/utility/optional.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/dist/replication/partition_resolver.h>
#include "dist/replication/storage_engine/simple_kv/simple_kv.code.definition.h"
#include "dist/replication/storage_engine/simple_kv/simple_kv_types.h"

namespace dsn {
namespace replication {
namespace application {
class simple_kv_client
{
public:
    simple_kv_client(const char *cluster_name,
                     const std::vector<dsn::rpc_address> &meta_list,
                     const char *app_name)
    {
        _resolver = partition_resolver::get_resolver(cluster_name, meta_list, app_name);
    }

    simple_kv_client() {}

    virtual ~simple_kv_client() {}

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_READ ------------
    // - synchronous
    std::pair<::dsn::error_code, std::string>
    read_sync(const std::string &key,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              uint64_t partition_hash = 0)
    {
        return ::dsn::rpc::wait_and_unwrap<std::string>(
            _resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_READ,
                               key,
                               nullptr,
                               empty_rpc_handler,
                               timeout,
                               partition_hash,
                               0));
    }

    // - asynchronous with on-stack std::string and std::string
    template <typename TCallback>
    ::dsn::task_ptr read(const std::string &key,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         uint64_t partition_hash = 0,
                         int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_READ,
                                  key,
                                  nullptr,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_WRITE ------------
    // - synchronous
    std::pair<::dsn::error_code, int32_t>
    write_sync(const kv_pair &pr,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
               uint64_t partition_hash = 0)
    {
        return dsn::rpc::wait_and_unwrap<int32_t>(_resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                                                                     pr,
                                                                     nullptr,
                                                                     empty_rpc_handler,
                                                                     timeout,
                                                                     partition_hash,
                                                                     0));
    }

    // - asynchronous with on-stack kv_pair and int32_t
    template <typename TCallback>
    ::dsn::task_ptr write(const kv_pair &pr,
                          TCallback &&callback,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                          uint64_t partition_hash = 0,
                          int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                                  pr,
                                  nullptr,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_APPEND ------------
    // - synchronous
    std::pair<::dsn::error_code, int32_t>
    append_sync(const kv_pair &pr,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                uint64_t partition_hash = 0)
    {
        return ::dsn::rpc::wait_and_unwrap<int32_t>(
            _resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                               pr,
                               nullptr,
                               empty_rpc_handler,
                               timeout,
                               partition_hash,
                               0));
    }

    // - asynchronous with on-stack kv_pair and int32_t
    template <typename TCallback>
    ::dsn::task_ptr append(const kv_pair &pr,
                           TCallback &&callback,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                           uint64_t partition_hash = 0,
                           int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                                  pr,
                                  nullptr,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  partition_hash,
                                  reply_thread_hash);
    }

private:
    dsn::replication::partition_resolver_ptr _resolver;
};
} // namespace application
} // namespace replication
} // namespace dsn
