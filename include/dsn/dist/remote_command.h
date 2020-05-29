// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/errors.h>
#include <dsn/tool-api/task.h>

namespace dsn {
namespace dist {
namespace cmd {

/// Calls a remote command to the remote server.
task_ptr async_call_remote(rpc_address remote,
                           const std::string &cmd,
                           const std::vector<std::string> &arguments,
                           std::function<void(error_code, const std::string &)> callback,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

/// Registers the server-side RPC handler of remote commands.
bool register_remote_command_rpc();

} // namespace cmd
} // namespace dist
} // namespace dsn
