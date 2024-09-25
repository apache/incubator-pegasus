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

#include <memory>

#include "client/partition_resolver.h"
#include "partition_resolver_manager.h"
#include "partition_resolver_simple.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_host_port.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/utils.h"

namespace dsn {

namespace replication {

template <typename T>
bool vector_equal(const std::vector<T> &a, const std::vector<T> &b)
{
    if (a.size() != b.size())
        return false;
    for (const T &item : a) {
        if (!utils::contains(b, item)) {
            return false;
        }
    }
    for (const T &item : b) {
        if (!utils::contains(a, item)) {
            return false;
        }
    }
    return true;
}

partition_resolver_ptr partition_resolver_manager::find_or_create(
    const char *cluster_name, const std::vector<host_port> &meta_list, const char *app_name)
{
    dsn::zauto_lock l(_lock);
    std::map<std::string, partition_resolver_ptr> &app_map = _resolvers[cluster_name];
    partition_resolver_ptr &ptr = app_map[app_name];

    if (ptr == nullptr) {
        dsn::host_port meta_group;
        meta_group.assign_group(cluster_name);
        meta_group.group_host_port()->add_list(meta_list);
        ptr = new partition_resolver_simple(meta_group, app_name);
        return ptr;
    } else {
        const auto &meta_group = ptr->get_meta_server();
        const auto &existing_list = meta_group.group_host_port()->members();
        if (!vector_equal(meta_list, existing_list)) {
            LOG_ERROR("meta list not match for cluster({})", cluster_name);
            return nullptr;
        }
        return ptr;
    }
}

} // namespace replication
} // namespace dsn
