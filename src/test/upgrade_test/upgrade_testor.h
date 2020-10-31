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

#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <memory>

#include "job.h"
#include "upgrader_handler.h"

namespace pegasus {
namespace test {

class upgrade_testor
{
public:
    upgrade_testor();
    ~upgrade_testor();

    // 1. randomly generate the upgrade plan: how many replica to upgrade
    // 2. execute the upgrade plan
    // 3. start the upgrade job after sleep for a while
    void run();

    // upgrade replica_cnt replica-job
    bool upgrade(int replica_cnt);

    // downgrade all jobs that have been upgraded
    bool downgrade();

    static void stop_verifier_and_exit(const char *msg);

private:
    bool upgrade_job_by_index(job_type type, int index);
    bool downgrade_job_by_index(job_type type, int index);

    // generate cnt number belong to [a, b],
    // if cnt > (b - a + 1), then just return the numbers between a ~ b
    void generate_random(std::vector<int> &res, int cnt, int a, int b);
    // generate one number belong to [a, b]
    int generate_one_number(int a, int b);

    bool check_coredump();

private:
    std::shared_ptr<upgrader_handler> _upgrader_handler;
    uint32_t _sleep_time_before_recover_seconds;

    int32_t _total_meta_count;
    int32_t _total_replica_count;
    int32_t _total_zookeeper_count;

    int32_t _upgrade_replica_max_count;
    int32_t _upgrade_meta_max_count;
    int32_t _upgrade_zk_max_count;

    std::vector<job_type> _job_types;

    int64_t upgrade_round;

    // current upgrade plan:
    // _job_index_to_upgrade[i][j] indicate that index j of job type i will be upgraded
    std::vector<std::vector<int>> _job_index_to_upgrade;
};
}
} // end namespace
