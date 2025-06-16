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

class idempotent_writer
{
public:
    template <typename TRpcHolder>
    using apply_func_t =
        std::function<int(const std::vector<dsn::apps::update_request> &, const TRpcHolder &)>;

    template <typename TRpcHolder,
              std::enable_if_t<std::disjunction_v<std::is_same<TRpcHolder, incr_rpc>,
                                                  std::is_same<TRpcHolder, check_and_set_rpc>,
                                                  std::is_same<TRpcHolder, check_and_mutate_rpc>>,
                               int> = 0>
    idempotent_writer(dsn::message_ex *original_request,
                      TRpcHolder &&original_rpc,
                      apply_func_t<TRpcHolder> &&apply_func,
                      std::vector<dsn::apps::update_request> &&updates)
        : _original_request(original_request),
          _apply_executor(std::in_place_type<apply_executor<TRpcHolder>>,
                          std::forward<TRpcHolder>(original_rpc),
                          std::move(apply_func)),
          _updates(std::move(updates))
    {
    }

    ~idempotent_writer() = default;

    const dsn::message_ptr &original_request() const { return _original_request; }

    int apply() const
    {
        return std::visit(
            [this](auto &executor) -> int { return executor.func(_updates, executor.rpc); },
            _apply_executor);
    }

private:
    // The original request received from the client. While making an atomic request (incr,
    // check_and_set and check_and_mutate) idempotent, an extra variable is needed to hold
    // its original request for the purpose of replying to the client.
    const dsn::message_ptr _original_request;

    template <typename TRpcHolder>
    struct apply_executor
    {
        apply_executor(TRpcHolder &&original_rpc, apply_func_t<TRpcHolder> &&apply_func)
            : rpc(std::forward<TRpcHolder>(original_rpc)), func(std::move(apply_func))
        {
        }

        const TRpcHolder rpc;
        const apply_func_t<TRpcHolder> func;
    };

    using apply_executor_t = std::variant<apply_executor<incr_rpc>,
                                          apply_executor<check_and_set_rpc>,
                                          apply_executor<check_and_mutate_rpc>>;

    const apply_executor_t _apply_executor;

    const std::vector<dsn::apps::update_request> _updates;

    DISALLOW_COPY_AND_ASSIGN(idempotent_writer);
    DISALLOW_MOVE_AND_ASSIGN(idempotent_writer);
};

using idempotent_writer_ptr = std::unique_ptr<idempotent_writer>;

} // namespace pegasus
