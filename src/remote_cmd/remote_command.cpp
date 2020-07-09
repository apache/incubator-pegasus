// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/remote_command.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/cpp/rpc_holder.h>
#include <dsn/c/api_layer1.h>
#include <dsn/utility/smart_pointers.h>

#include "command_types.h"

namespace dsn {
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
    std::unique_ptr<command> request = make_unique<command>();
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
