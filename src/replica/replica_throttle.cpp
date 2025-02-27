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

#include <stdint.h>
#include <chrono>
#include <map>
#include <string>
#include <utility>

#include "common/gpid.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "replica.h"
#include "rpc/rpc_message.h"
#include "task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/throttling_controller.h"

namespace dsn {
namespace replication {

#define THROTTLE_REQUEST(op_type, throttling_type, request, request_units)                         \
    do {                                                                                           \
        int64_t delay_ms = 0;                                                                      \
        auto type = _##op_type##_##throttling_type##_throttling_controller.control(                \
            request->header->client.timeout_ms, request_units, delay_ms);                          \
        if (type != utils::throttling_controller::PASS) {                                          \
            if (type == utils::throttling_controller::DELAY) {                                     \
                tasking::enqueue(                                                                  \
                    LPC_##op_type##_THROTTLING_DELAY,                                              \
                    &_tracker,                                                                     \
                    [this, req = message_ptr(request)]() { on_client_##op_type(req, true); },      \
                    get_gpid().thread_hash(),                                                      \
                    std::chrono::milliseconds(delay_ms));                                          \
                METRIC_VAR_INCREMENT(throttling_delayed_##op_type##_requests);                     \
            } else { /** type == utils::throttling_controller::REJECT **/                          \
                if (delay_ms > 0) {                                                                \
                    tasking::enqueue(                                                              \
                        LPC_##op_type##_THROTTLING_DELAY,                                          \
                        &_tracker,                                                                 \
                        [this, req = message_ptr(request)]() {                                     \
                            response_client_##op_type(req, ERR_BUSY);                              \
                        },                                                                         \
                        get_gpid().thread_hash(),                                                  \
                        std::chrono::milliseconds(delay_ms));                                      \
                } else {                                                                           \
                    response_client_##op_type(request, ERR_BUSY);                                  \
                }                                                                                  \
                METRIC_VAR_INCREMENT(throttling_rejected_##op_type##_requests);                    \
            }                                                                                      \
            return true;                                                                           \
        }                                                                                          \
    } while (0)

bool replica::throttle_write_request(message_ex *request)
{
    THROTTLE_REQUEST(write, qps, request, 1);
    THROTTLE_REQUEST(write, size, request, request->body_size());
    return false;
}

bool replica::throttle_read_request(message_ex *request)
{
    THROTTLE_REQUEST(read, qps, request, 1);
    return false;
}

bool replica::throttle_backup_request(message_ex *request)
{
    int64_t delay_ms = 0;
    auto type = _backup_request_qps_throttling_controller.control(
        request->header->client.timeout_ms, 1, delay_ms);
    if (type != utils::throttling_controller::PASS) {
        if (type == utils::throttling_controller::DELAY) {
            tasking::enqueue(
                LPC_read_THROTTLING_DELAY,
                &_tracker,
                [this, req = message_ptr(request)]() { on_client_read(req, true); },
                get_gpid().thread_hash(),
                std::chrono::milliseconds(delay_ms));
            METRIC_VAR_INCREMENT(throttling_delayed_backup_requests);
        } else { /** type == utils::throttling_controller::REJECT **/
            METRIC_VAR_INCREMENT(throttling_rejected_backup_requests);
        }
        return true;
    }
    return false;
}

void replica::update_throttle_envs(const std::map<std::string, std::string> &envs)
{
    update_throttle_env_internal(
        envs, replica_envs::WRITE_QPS_THROTTLING, _write_qps_throttling_controller);
    update_throttle_env_internal(
        envs, replica_envs::WRITE_SIZE_THROTTLING, _write_size_throttling_controller);
    update_throttle_env_internal(
        envs, replica_envs::READ_QPS_THROTTLING, _read_qps_throttling_controller);
    update_throttle_env_internal(envs,
                                 replica_envs::BACKUP_REQUEST_QPS_THROTTLING,
                                 _backup_request_qps_throttling_controller);
}

void replica::update_throttle_env_internal(const std::map<std::string, std::string> &envs,
                                           const std::string &key,
                                           utils::throttling_controller &cntl)
{
    bool throttling_changed = false;
    std::string old_throttling;
    std::string parse_error;
    auto find = envs.find(key);
    if (find != envs.end()) {
        if (!cntl.parse_from_env(find->second,
                                 _app_info.partition_count,
                                 parse_error,
                                 throttling_changed,
                                 old_throttling)) {
            LOG_WARNING_PREFIX("parse env failed, key = \"{}\", value = \"{}\", error = \"{}\"",
                               key,
                               find->second,
                               parse_error);
            // reset if parse failed
            cntl.reset(throttling_changed, old_throttling);
        }
    } else {
        // reset if env not found
        cntl.reset(throttling_changed, old_throttling);
    }
    if (throttling_changed) {
        LOG_INFO_PREFIX("switch {} from \"{}\" to \"{}\"", key, old_throttling, cntl.env_value());
    }
}

} // namespace replication
} // namespace dsn
