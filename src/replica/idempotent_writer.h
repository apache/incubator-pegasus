// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "base/pegasus_rpc_types.h"
#include "rpc/rpc_message.h"
#include "rrdb/rrdb_types.h"
#include "utils/ports.h"

namespace pegasus {

// The `idempotent_writer` class is used by the primary replica to cache the idempotent
// single-update requests generated during the "make_idempotent" phase, as well as the original
// atomic write RPC request from the client. Later, during the 2PC process, it can directly
// apply the cached single-update requests to the storage engine and automatically respond to
// the client based on the cached atomic write RPC request.
//
// With `idempotent_writer`, the extra and unnecessary deserialization of the idempotent write
// requests and the original atomic write request can be avoided.
class idempotent_writer
{
public:
    template <typename TRpcHolder>
    using apply_func_t =
        std::function<int(const std::vector<dsn::apps::update_request> &, const TRpcHolder &)>;

    // Parameters for the constructor:
    // - original_rpc: the RPC holder with the deserialized original request, restricted to the
    // atomic write RPC requests (i.e. incr, check_and_set or check_and_mutate).
    // - apply_func: the user-defined function that applies the idempotent single-update requests
    // and builds the response to the client.
    // - updates: the idempotent single-update requests translated from the original atomic write
    // request.
    template <typename TRpcHolder,
              std::enable_if_t<std::disjunction_v<std::is_same<TRpcHolder, incr_rpc>,
                                                  std::is_same<TRpcHolder, check_and_set_rpc>,
                                                  std::is_same<TRpcHolder, check_and_mutate_rpc>>,
                               int> = 0>
    idempotent_writer(TRpcHolder &&original_rpc,
                      apply_func_t<TRpcHolder> &&apply_func,
                      std::vector<dsn::apps::update_request> &&updates)
        : _apply_runner(std::in_place_type<apply_runner<TRpcHolder>>,
                        std::forward<TRpcHolder>(original_rpc),
                        std::move(apply_func)),
          _updates(std::move(updates))
    {
    }

    ~idempotent_writer() = default;

    // Return the serialized message of the original RPC request, which should never be null.
    [[nodiscard]] dsn::message_ex *request() const
    {
        return std::visit([](auto &runner) { return runner.rpc.dsn_request(); }, _apply_runner);
    }

    // Apply single-update requests to the storage engine, and automatically respond to the
    // client -- it won't respond until the internal holder of the RPC is destructed. Return
    // rocksdb::Status::kOk if succeed, otherwise some error code (rocksdb::Status::Code).
    [[nodiscard]] int apply() const
    {
        return std::visit(
            [this](auto &runner) {
                // Enable automatic reply to the client no matter whether it would succeed
                // or some error might occur.
                runner.rpc.enable_auto_reply();

                return runner.func(_updates, runner.rpc);
            },
            _apply_runner);
    }

private:
    template <typename TRpcHolder>
    struct apply_runner
    {
        apply_runner(TRpcHolder &&original_rpc, apply_func_t<TRpcHolder> &&apply_func)
            : rpc(std::forward<TRpcHolder>(original_rpc)), func(std::move(apply_func))
        {
            // Disable automatic reply to make sure we won't respond to the client until we
            // are ready to apply idempotent single-update requests to the storage engine,
            // because:
            // 1. Automatic reply only makes sense after the response is ready, so it should
            // be enabled right before applying to the storage engine.
            // 2. Before applying to the storage engine, an error might occur and the client
            // could be proactively replied to externally (e.g., via reply_with_error()),
            // so automatic reply needs to be disabled during this stage.
            rpc.disable_auto_reply();
        }

        // Holds the original RPC request and the response to it.
        const TRpcHolder rpc;

        // The user-defined function that applies idempotent single-update requests to the
        // storage engine.
        const apply_func_t<TRpcHolder> func;
    };

    using apply_runner_t = std::variant<apply_runner<incr_rpc>,
                                        apply_runner<check_and_set_rpc>,
                                        apply_runner<check_and_mutate_rpc>>;

    const apply_runner_t _apply_runner;

    // The idempotent single-update requests that are translated from the original atomic
    // write request.
    const std::vector<dsn::apps::update_request> _updates;

    DISALLOW_COPY_AND_ASSIGN(idempotent_writer);
    DISALLOW_MOVE_AND_ASSIGN(idempotent_writer);
};

using idempotent_writer_ptr = std::unique_ptr<idempotent_writer>;

} // namespace pegasus
