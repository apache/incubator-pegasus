// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "job.h"
#include "killer_handler.h"
#include "killer_handler_shell.h"
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <memory>

namespace pegasus {
namespace test {

// generate cnt number belong to [a, b],
// if cnt > (b - a + 1), then just return the numbers between a ~ b
void generate_random(std::vector<int> &res, int cnt, int a, int b);
// generate one number belong to [a, b]
int generate_one_number(int a, int b);

class kill_testor
{
public:
    kill_testor();
    ~kill_testor();
    // run once
    void run();
    // kill meta_cnt meta-job, replica_cnt replica-job and zk_cnt zookeeper-job
    bool kill(int meta_cnt, int replica_cnt, int zk_cnt);
    // start all jobs that have been stopped
    bool start();

    void set_killer_handler(std::shared_ptr<killer_handler> ptr) { this->_killer_handler = ptr; }

    void set_log_file(const std::string &filename) { _log_filename = filename; }

private:
    bool kill_job_by_index(job_type type, int index);
    bool start_job_by_index(job_type type, int index);

private:
    std::shared_ptr<killer_handler> _killer_handler;
    // sleep time after kill/recover
    uint32_t _sleep_time_before_recover_seconds;
    // total number of meta/replica/zookeeper
    int32_t _total_meta_count;
    int32_t _total_replica_count;
    int32_t _total_zookeeper_count;
    // max count of the meta/replica/zookeeper that can be killed
    int32_t _replica_killed_max_count;
    int32_t _meta_killed_max_count;
    int32_t _zk_killed_max_count;

    std::vector<job_type> _job_types;

    // path of the log filename, default is ./kill_history.txt
    std::string _log_filename;
    // log handler
    std::shared_ptr<std::ofstream> _log_handler;

    int64_t kill_round;

    std::vector<std::vector<int>> _job_index_to_kill;
};
}
} // end namespace
