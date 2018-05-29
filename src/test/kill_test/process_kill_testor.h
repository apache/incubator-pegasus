// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "kill_testor.h"
#include "killer_handler.h"
#include "job.h"

namespace pegasus {
namespace test {
class process_kill_testor : public kill_testor
{
public:
    process_kill_testor(const char *config_file);
    ~process_kill_testor();

    // 1. randomly generate the kill plan: how many meta/replica/zk to kill
    // 2. execute the kill plan
    // 3. start the killed job after sleep for a while
    virtual void Run();

    static void stop_verifier_and_exit(const char *msg);

private:
    void run();

    // start all jobs that have been killed
    bool start();

    // kill meta_cnt meta-job, replica_cnt replica-job and zk_cnt zookeeper-job
    bool kill(int meta_cnt, int replica_cnt, int zk_cnt);

    bool kill_job_by_index(job_type type, int index);
    bool start_job_by_index(job_type type, int index);

    bool check_coredump();
    bool verifier_process_alive();

    std::shared_ptr<killer_handler> _killer_handler;
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
} // namespace test
} // namespace pegasus
