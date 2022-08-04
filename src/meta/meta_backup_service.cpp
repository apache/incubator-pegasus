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

#include <absl/strings/string_view.h>
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/core.h>
#include <algorithm>
#include <iterator>
#include <type_traits>
#include <utility>

#include "block_service/block_service.h"
#include "block_service/block_service_manager.h"
#include "common/backup_common.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "meta/backup_engine.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_state_service.h"
#include "meta_backup_service.h"
#include "meta_service.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "security/access_controller.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "server_state.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/defer.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/time_utils.h"

DSN_DECLARE_int32(cold_backup_checkpoint_reserve_minutes);
DSN_DECLARE_int32(fd_lease_seconds);

METRIC_DEFINE_entity(backup_policy);
METRIC_DEFINE_gauge_int64(backup_policy,
                          backup_recent_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The duration of recent backup");

namespace dsn {
namespace replication {

backup_service::backup_service(meta_service *meta_svc,
                               const std::string &policy_meta_root,
                               const std::string &backup_root)
    : _meta_svc(meta_svc), _policy_meta_root(policy_meta_root), _backup_root(backup_root)
{
    _state = _meta_svc->get_server_state();
}

// TODO(heyuchen): implement it
void backup_service::start() {}

// TODO(heyuchen): implement it
void backup_service::start_backup_app(start_backup_app_rpc rpc) {}

// TODO(heyuchen): implement it
void backup_service::query_backup_status(query_backup_status_rpc rpc) {}

} // namespace replication
} // namespace dsn
