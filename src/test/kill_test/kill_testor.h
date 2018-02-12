// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <memory>

#include "job.h"
#include "killer_handler.h"

namespace pegasus {
namespace test {

class kill_testor
{
public:
    kill_testor();
    ~kill_testor();

    // 1. randomly generate the kill plan: how many meta/replica/zk to kill
    // 2. execute the kill plan
    // 3. start the killed job after sleep for a while
    void run();

    // kill meta_cnt meta-job, replica_cnt replica-job and zk_cnt zookeeper-job
    bool kill(int meta_cnt, int replica_cnt, int zk_cnt);

    // start all jobs that have been killed
    bool start();

    static void stop_verifier_and_exit(const char *msg);

private:
    bool kill_job_by_index(job_type type, int index);
    bool start_job_by_index(job_type type, int index);

    // generate cnt number belong to [a, b],
    // if cnt > (b - a + 1), then just return the numbers between a ~ b
    void generate_random(std::vector<int> &res, int cnt, int a, int b);
    // generate one number belong to [a, b]
    int generate_one_number(int a, int b);

    bool check_coredump();

private:
    std::shared_ptr<killer_handler> _killer_handler;
    uint32_t _sleep_time_before_recover_seconds;

    int32_t _total_meta_count;
    int32_t _total_replica_count;
    int32_t _total_zookeeper_count;

    int32_t _kill_replica_max_count;
    int32_t _kill_meta_max_count;
    int32_t _kill_zk_max_count;

    std::vector<job_type> _job_types;

    int64_t kill_round;

    // current kill plan:
    // _job_index_to_kill[i][j] indicate that index j of job i will be killed
    std::vector<std::vector<int>> _job_index_to_kill;
};
}
} // end namespace
