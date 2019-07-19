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

#include <dsn/cpp/rpc_holder.h>
#include <dsn/utility/errors.h>
#include <dsn/dist/replication/replication_types.h>

namespace dsn {
namespace replication {

typedef rpc_holder<duplication_status_change_request, duplication_status_change_response>
    duplication_status_change_rpc;
typedef rpc_holder<duplication_add_request, duplication_add_response> duplication_add_rpc;
typedef rpc_holder<duplication_query_request, duplication_query_response> duplication_query_rpc;
typedef rpc_holder<duplication_sync_request, duplication_sync_response> duplication_sync_rpc;

typedef int32_t dupid_t;

extern const char *duplication_status_to_string(duplication_status::type status);

inline bool is_duplication_status_valid(duplication_status::type status)
{
    return status == duplication_status::DS_PAUSE || status == duplication_status::DS_START;
}

/// Returns the cluster name (i.e, "onebox") if it's configured under
/// "replication" section:
///    [replication]
///      cluster_name = "onebox"
extern const char *get_current_cluster_name();

/// Returns the cluster id of url specified in the duplication-group section
/// of your configuration, for example:
///
/// ```
///   [duplication-group]
///       wuhan-mi-srv-ad = 3
///       tianjin-mi-srv-ad = 4
/// ```
///
/// The returned cluster id of get_duplication_cluster_id("wuhan-mi-srv-ad") is 3.
extern error_with<uint8_t> get_duplication_cluster_id(const std::string &cluster_name);

/// Returns a displayable string for this duplication_entry.
extern std::string duplication_entry_to_string(const duplication_entry &dup);

/// Returns a mapping from cluster_name to cluster_id.
extern const std::map<std::string, uint8_t> &get_duplication_group();

extern const std::set<uint8_t> &get_distinct_cluster_id_set();

inline bool is_cluster_id_configured(uint8_t cid)
{
    return get_distinct_cluster_id_set().find(cid) != get_distinct_cluster_id_set().end();
}

} // namespace replication
} // namespace dsn
