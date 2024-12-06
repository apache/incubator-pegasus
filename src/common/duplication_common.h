/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <stdint.h>
#include <set>
#include <string>

#include "duplication_types.h"
#include "rpc/rpc_holder.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_utils.h"

DSN_DECLARE_uint32(duplicate_log_batch_bytes);

namespace dsn::replication {

using duplication_modify_rpc = rpc_holder<duplication_modify_request, duplication_modify_response>;
using duplication_add_rpc = rpc_holder<duplication_add_request, duplication_add_response>;
using duplication_query_rpc = rpc_holder<duplication_query_request, duplication_query_response>;
using duplication_sync_rpc = rpc_holder<duplication_sync_request, duplication_sync_response>;
using duplication_list_rpc = rpc_holder<duplication_list_request, duplication_list_response>;

using dupid_t = int32_t;

extern const char *duplication_status_to_string(duplication_status::type status);

extern const char *duplication_fail_mode_to_string(duplication_fail_mode::type);

inline bool is_duplication_status_invalid(duplication_status::type status)
{
    return status == duplication_status::DS_INIT || status == duplication_status::DS_REMOVED;
}

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

extern uint8_t get_current_dup_cluster_id_or_default();

extern uint8_t get_current_dup_cluster_id();

/// Returns a json string.
extern std::string duplication_entry_to_string(const duplication_entry &dup);

/// Returns a json string.
extern std::string duplication_query_response_to_string(const duplication_query_response &);

extern const std::set<uint8_t> &get_distinct_cluster_id_set();

extern bool is_dup_cluster_id_configured(uint8_t cluster_id);

struct duplication_constants
{
    const static std::string kDuplicationCheckpointRootDir;
    const static std::string kClustersSectionName;
    // These will fill into app env and mark one app as a "follower app" and record master info
    const static std::string kEnvMasterClusterKey;
    const static std::string kEnvMasterMetasKey;
    const static std::string kEnvMasterAppNameKey;
    const static std::string kEnvFollowerAppStatusKey;
    const static std::string kEnvFollowerAppStatusCreating;
    const static std::string kEnvFollowerAppStatusCreated;
};

USER_DEFINED_ENUM_FORMATTER(duplication_fail_mode::type)
USER_DEFINED_ENUM_FORMATTER(duplication_status::type)

} // namespace dsn::replication
