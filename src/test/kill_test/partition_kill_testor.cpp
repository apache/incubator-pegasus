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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "partition_kill_testor.h"
#include "remote_cmd/remote_command.h"
#include "task/task.h"
#include "test/kill_test/kill_testor.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DECLARE_uint32(kill_interval_seconds);

namespace pegasus {
namespace test {

partition_kill_testor::partition_kill_testor(const char *config_file) : kill_testor(config_file) {}

void partition_kill_testor::Run()
{
    LOG_INFO("begin the kill-partition");
    while (true) {
        if (!check_cluster_status()) {
            LOG_INFO("check_cluster_status() failed");
        } else {
            run();
        }
        LOG_INFO("sleep {} seconds before checking", FLAGS_kill_interval_seconds);
        sleep(FLAGS_kill_interval_seconds);
    }
}

void partition_kill_testor::run()
{
    if (pcs.empty()) {
        LOG_INFO("partitions empty");
        return;
    }

    int random_num = generate_one_number(0, pcs.size() - 1);
    std::vector<int> random_indexs;
    generate_random(random_indexs, random_num, 0, pcs.size() - 1);

    std::vector<dsn::task_ptr> tasks(random_num);
    std::vector<std::pair<bool, std::string>> results(random_num);

    std::vector<std::string> arguments(2);
    for (int i = 0; i < random_indexs.size(); ++i) {
        int index = random_indexs[i];
        const auto &pc = pcs[index];

        arguments[0] = to_string(pc.pid.get_app_id());
        arguments[1] = to_string(pc.pid.get_partition_index());

        auto callback = [&results, i](::dsn::error_code err, const std::string &resp) {
            if (err == ::dsn::ERR_OK) {
                results[i].first = true;
                results[i].second = resp;
            } else {
                results[i].first = false;
                results[i].second = err.to_string();
            }
        };
        tasks[i] = dsn::dist::cmd::async_call_remote(pc.primary,
                                                     "replica.kill_partition",
                                                     arguments,
                                                     callback,
                                                     std::chrono::milliseconds(5000));
    }

    for (int i = 0; i < tasks.size(); ++i) {
        tasks[i]->wait();
    }

    int failed = 0;
    for (int i = 0; i < results.size(); ++i) {
        if (!results[i].first) {
            failed++;
        }
    }

    if (failed > 0) {
        LOG_ERROR("call replica.kill_partition failed");
    }
}
} // namespace test
} // namespace pegasus
