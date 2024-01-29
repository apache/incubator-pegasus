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

#include "result_writer.h"

#include <pegasus/error.h>
#include <chrono>
#include <utility>

#include "pegasus/client.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_code.h"

namespace pegasus {
namespace server {

DSN_DEFINE_int32(pegasus.collector,
                 capacity_unit_saving_ttl_days,
                 90,
                 "the ttl of the CU data, 0 if no ttl");

DEFINE_TASK_CODE(LPC_WRITE_RESULT, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

result_writer::result_writer(pegasus_client *client) : _client(client) {}

void result_writer::set_result(const std::string &hash_key,
                               const std::string &sort_key,
                               const std::string &value,
                               int try_count)
{
    auto async_set_callback = [=](int err, pegasus_client::internal_info &&info) {
        if (err != PERR_OK) {
            int new_try_count = try_count - 1;
            if (new_try_count > 0) {
                LOG_WARNING("set_result fail, hash_key = {}, sort_key = {}, value = {}, "
                            "error = {}, left_try_count = {}, try again after 1 minute",
                            hash_key,
                            sort_key,
                            value,
                            _client->get_error_string(err),
                            new_try_count);
                ::dsn::tasking::enqueue(
                    LPC_WRITE_RESULT,
                    &_tracker,
                    [=]() { set_result(hash_key, sort_key, value, new_try_count); },
                    0,
                    std::chrono::minutes(1));
            } else {
                LOG_ERROR("set_result fail, hash_key = {}, sort_key = {}, value = {}, error = "
                          "{}, left_try_count = {}, do not try again",
                          hash_key,
                          sort_key,
                          value,
                          _client->get_error_string(err),
                          new_try_count);
            }
        } else {
            LOG_DEBUG("set_result succeed, hash_key = {}, sort_key = {}, value = {}",
                      hash_key,
                      sort_key,
                      value);
        }
    };

    _client->async_set(hash_key,
                       sort_key,
                       value,
                       std::move(async_set_callback),
                       5000,
                       FLAGS_capacity_unit_saving_ttl_days * 3600 * 24);
}
} // namespace server
} // namespace pegasus
