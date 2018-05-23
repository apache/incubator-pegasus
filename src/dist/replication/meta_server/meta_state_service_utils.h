/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <dsn/dist/meta_state_service.h>

namespace dsn {
namespace replication {
namespace mss { // abbreviation of meta_state_service

/// This class is a convenience wrapper over meta_state_service.
/// It wraps every operation in a error handling mechanism, and provides utilities
/// like recursive node creation.
/// Notice: The operations always run in THREAD_POOL_META_STATE: LPC_META_STATE_HIGH.
///
/// ERROR HANDLING:
/// Currently it retries for every timeout(ERR_TIMEOUT) operation infinitely,
/// and delays 1sec for each attempt. For unexpected failures it will terminate
/// the program.
/// \see meta_state_service_utils_impl.h # error_handling
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

    void set_data(std::string &&node, blob &&value, std::function<void()> &&cb);

    /// If node does not exist, cb will receive an empty blob.
    void get_data(std::string &&node, std::function<void(const blob &)> &&cb);

    // TODO(wutao1): get_children_data: retrieve data of all children into a vector.

private:
    void delete_node_impl(std::string &&node, std::function<void()> &&cb, bool is_recursive);

private:
    friend class operation;

    dist::meta_state_service *_remote;
    dsn::task_tracker *_tracker;
};

} // namespace mss
} // namespace replication
} // namespace dsn
