// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replica_envs.h>

namespace dsn {
namespace replication {

#define THROTTLE_REQUEST(op_type, throttling_type, request, request_units)                         \
    do {                                                                                           \
        int64_t delay_ms = 0;                                                                      \
        auto type = _##op_type##_##throttling_type##_throttling_controller.control(                \
            request->header->client.timeout_ms, request_units, delay_ms);                          \
        if (type != throttling_controller::PASS) {                                                 \
            if (type == throttling_controller::DELAY) {                                            \
                tasking::enqueue(                                                                  \
                    LPC_##op_type##_THROTTLING_DELAY,                                              \
                    &_tracker,                                                                     \
                    [ this, req = message_ptr(request) ]() { on_client_##op_type(req, true); },    \
                    get_gpid().thread_hash(),                                                      \
                    std::chrono::milliseconds(delay_ms));                                          \
                _counter_recent_##op_type##_throttling_delay_count->increment();                   \
            } else { /** type == throttling_controller::REJECT **/                                 \
                if (delay_ms > 0) {                                                                \
                    tasking::enqueue(LPC_##op_type##_THROTTLING_DELAY,                             \
                                     &_tracker,                                                    \
                                     [ this, req = message_ptr(request) ]() {                      \
                                         response_client_##op_type(req, ERR_BUSY);                 \
                                     },                                                            \
                                     get_gpid().thread_hash(),                                     \
                                     std::chrono::milliseconds(delay_ms));                         \
                } else {                                                                           \
                    response_client_##op_type(request, ERR_BUSY);                                  \
                }                                                                                  \
                _counter_recent_##op_type##_throttling_reject_count->increment();                  \
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
    THROTTLE_REQUEST(read, size, request, request->body_size());
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
    update_throttle_env_internal(
        envs, replica_envs::READ_SIZE_THROTTLING, _read_size_throttling_controller);
}

void replica::update_throttle_env_internal(const std::map<std::string, std::string> &envs,
                                           const std::string &key,
                                           throttling_controller &cntl)
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
            dwarn_replica("parse env failed, key = \"{}\", value = \"{}\", error = \"{}\"",
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
        ddebug_replica("switch {} from \"{}\" to \"{}\"", key, old_throttling, cntl.env_value());
    }
}

} // namespace replication
} // namespace dsn
