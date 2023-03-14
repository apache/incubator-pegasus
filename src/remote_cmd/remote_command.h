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

#include <chrono>
#include <functional>
#include <string>
#include <vector>

#include "runtime/task/task.h"

namespace dsn {
class error_code;
class rpc_address;

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
