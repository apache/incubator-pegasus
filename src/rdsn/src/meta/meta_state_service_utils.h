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

#include "meta/meta_state_service.h"

namespace dsn {
namespace replication {
namespace mss { // abbreviation of meta_state_service

/// This class is a convenience wrapper over meta_state_service.
/// It wraps every operation with a simple error handling mechanism, and provides utilities
/// like recursive node creation.
/// Notice: The operations always run in THREAD_POOL_META_STATE: LPC_META_STATE_HIGH.
///         This class is thread-safe.
///
/// ERROR HANDLING:
/// Currently it retries for every timeout(ERR_TIMEOUT) operation infinitely,
/// and delays 1sec for each attempt. For unexpected failures it will terminate
/// the program. This is somewhat brute force but suitable for most cases.
/// For more fine-grained error handling strategy, use meta_state_service instead.
/// \see meta_state_service_utils_impl.h # operation
struct meta_storage
{
    meta_storage(dist::meta_state_service *remote_storage, task_tracker *tracker);

    ~meta_storage();

    /// Asynchronously create nodes recursively from top down.
    void create_node_recursively(std::queue<std::string> &&nodes,
                                 blob &&value,
                                 std::function<void()> &&cb);

    void create_node(std::string &&node, blob &&value, std::function<void()> &&cb);

    void delete_node_recursively(std::string &&node, std::function<void()> &&cb);

    void delete_node(std::string &&node, std::function<void()> &&cb);

    /// Will fatal if node doesn't exists.
    void set_data(std::string &&node, blob &&value, std::function<void()> &&cb);

    /// If node does not exist, cb will receive an empty blob.
    void get_data(std::string &&node, std::function<void(const blob &)> &&cb);

    /// \param cb: void (bool node_exists, const std::vector<std::string> &children)
    ///            `children` contains the name (not full path) of children nodes.
    ///            `node_exists` indicates whether this node exists.
    void get_children(std::string &&node,
                      std::function<void(bool, const std::vector<std::string> &)> &&cb);

private:
    void delete_node_impl(std::string &&node, std::function<void()> &&cb, bool is_recursive);

private:
    friend struct operation;

    dist::meta_state_service *_remote;
    dsn::task_tracker *_tracker;
};

} // namespace mss
} // namespace replication
} // namespace dsn
