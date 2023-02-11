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

#include "remote_cmd/remote_command.h"

#include <algorithm> // IWYU pragma: keep
#include <memory>
#include <type_traits>
#include <utility>

#include "command_types.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/task/task_code.h"
#include "utils/command_manager.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"

namespace dsn {
class message_ex;

namespace dist {
namespace cmd {

DEFINE_TASK_CODE_RPC(RPC_CLI_CLI_CALL, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

typedef rpc_holder<command, std::string> remote_command_rpc;

task_ptr async_call_remote(rpc_address remote,
                           const std::string &cmd,
                           const std::vector<std::string> &arguments,
                           std::function<void(error_code, const std::string &)> callback,
                           std::chrono::milliseconds timeout)
{
    std::unique_ptr<command> request = std::make_unique<command>();
    request->cmd = cmd;
    request->arguments = arguments;
    remote_command_rpc rpc(std::move(request), RPC_CLI_CLI_CALL, timeout);
    return rpc.call(remote, nullptr, [ cb = std::move(callback), rpc ](error_code ec) {
        cb(ec, rpc.response());
    });
}

bool register_remote_command_rpc()
{
    rpc_request_handler cb = [](dsn::message_ex *msg) {
        auto rpc = remote_command_rpc::auto_reply(msg);
        command_manager::instance().run_command(
            rpc.request().cmd, rpc.request().arguments, rpc.response());
    };

    return dsn_rpc_register_handler(RPC_CLI_CLI_CALL, "call", cb);
}

} // namespace cmd
} // namespace dist
} // namespace dsn
