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

#include <fmt/core.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "killer_handler.h"
#include "killer_registry.h"
#include "process_kill_testor.h"
#include "test/kill_test/job.h"
#include "test/kill_test/kill_testor.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/strings.h"

DSN_DEFINE_int32(pegasus.killtest, total_meta_count, 0, "total meta count");
DSN_DEFINE_int32(pegasus.killtest, total_replica_count, 0, "total replica count");
DSN_DEFINE_int32(pegasus.killtest, total_zookeeper_count, 0, "total zookeeper count");
DSN_DEFINE_int32(pegasus.killtest,
                 kill_replica_max_count,
                 FLAGS_total_replica_count,
                 "replica killed max count");
DSN_DEFINE_int32(pegasus.killtest,
                 kill_meta_max_count,
                 FLAGS_total_meta_count,
                 "meta killed max count");
DSN_DEFINE_int32(pegasus.killtest,
                 kill_zookeeper_max_count,
                 FLAGS_total_zookeeper_count,
                 "zookeeper killed max count");
DSN_DEFINE_group_validator(kill_test_role_count, [](std::string &message) -> bool {
    if (FLAGS_total_meta_count == 0 && FLAGS_total_replica_count == 0 &&
        FLAGS_total_zookeeper_count == 0) {
        message = fmt::format("[pegasus.killtest].total_meta_count, total_replica_count and "
                              "total_zookeeper_count should not all be 0.");
        return false;
    }

    return true;
});

DSN_DEFINE_uint32(pegasus.killtest,
                  sleep_time_before_recover_seconds,
                  30,
                  "sleep time before recover seconds");

DSN_DEFINE_string(pegasus.killtest, killer_handler, "", "killer handler");
DSN_DEFINE_validator(killer_handler,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });

DSN_DECLARE_uint32(kill_interval_seconds);

namespace pegasus {
namespace test {

process_kill_testor::process_kill_testor(const char *config_file) : kill_testor(config_file)
{
    register_kill_handlers();

    kill_round = 0;
    _killer_handler.reset(killer_handler::new_handler(FLAGS_killer_handler));
    CHECK(_killer_handler, "invalid FLAGS_killer_handler({})", FLAGS_killer_handler);

    _job_types = {META, REPLICA, ZOOKEEPER};
    _job_index_to_kill.resize(JOB_LENGTH);
}

process_kill_testor::~process_kill_testor() {}

bool process_kill_testor::verifier_process_alive()
{
    const char *command = "ps aux | grep pegasus | grep verifier | wc -l";
    std::stringstream output;
    int process_count;

    CHECK_EQ(dsn::utils::pipe_execute(command, output), 0);
    output >> process_count;

    // one for the verifier, one for command
    return process_count > 1;
}

void process_kill_testor::Run()
{
    LOG_INFO("begin the kill-thread");
    while (true) {
        if (!check_cluster_status()) {
            stop_verifier_and_exit("check_cluster_status() fail, and exit");
        }
        if (!verifier_process_alive()) {
            stop_verifier_and_exit("the verifier process is dead");
        }
        run();
        LOG_INFO("sleep {} seconds before checking", FLAGS_kill_interval_seconds);
        sleep(FLAGS_kill_interval_seconds);
    }
}

void process_kill_testor::run()
{
    if (check_coredump()) {
        stop_verifier_and_exit("detect core dump in pegasus cluster");
    }

    if (kill_round == 0) {
        LOG_INFO("Number of meta-server: {}", FLAGS_total_meta_count);
        LOG_INFO("Number of replica-server: {}", FLAGS_total_replica_count);
        LOG_INFO("Number of zookeeper: {}", FLAGS_total_zookeeper_count);
    }
    kill_round += 1;
    int meta_cnt = 0;
    int replica_cnt = 0;
    int zk_cnt = 0;
    while ((meta_cnt == 0 && replica_cnt == 0 && zk_cnt == 0) ||
           (meta_cnt == FLAGS_total_meta_count && replica_cnt == FLAGS_total_replica_count &&
            zk_cnt == FLAGS_total_zookeeper_count)) {
        meta_cnt = generate_one_number(0, FLAGS_kill_meta_max_count);
        replica_cnt = generate_one_number(0, FLAGS_kill_replica_max_count);
        zk_cnt = generate_one_number(0, FLAGS_kill_zookeeper_max_count);
    }
    LOG_INFO("************************");
    LOG_INFO("Round [{}]", kill_round);
    LOG_INFO("start kill...");
    LOG_INFO("kill meta number={}, replica number={}, zk number={}", meta_cnt, replica_cnt, zk_cnt);

    if (!kill(meta_cnt, replica_cnt, zk_cnt)) {
        stop_verifier_and_exit("kill jobs failed");
    }

    auto sleep_time_random_seconds =
        generate_one_number(1, FLAGS_sleep_time_before_recover_seconds);
    LOG_INFO("sleep {} seconds before recovery", sleep_time_random_seconds);
    sleep(sleep_time_random_seconds);

    LOG_INFO("start recover...");
    if (!start()) {
        stop_verifier_and_exit("recover jobs failed");
    }
    LOG_INFO("after recover...");
    LOG_INFO("************************");
}

bool process_kill_testor::kill(int meta_cnt, int replica_cnt, int zookeeper_cnt)
{
    std::vector<int> kill_counts = {meta_cnt, replica_cnt, zookeeper_cnt};
    std::vector<int> total_count = {
        FLAGS_total_meta_count, FLAGS_total_replica_count, FLAGS_total_zookeeper_count};
    std::vector<int> random_idxs;
    generate_random(random_idxs, JOB_LENGTH, META, ZOOKEEPER);
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_kill = _job_index_to_kill[_job_types[id]];
        job_index_to_kill.clear();
        generate_random(job_index_to_kill, kill_counts[id], 1, total_count[id]);
        for (auto index : job_index_to_kill) {
            LOG_INFO("start to kill {}@{}", job_type_str(_job_types[id]), index);
            if (!kill_job_by_index(_job_types[id], index)) {
                LOG_INFO("kill {}@{} failed", job_type_str(_job_types[id]), index);
                return false;
            }
            LOG_INFO("kill {}@{} succeed", job_type_str(_job_types[id]), index);
        }
    }
    return true;
}

bool process_kill_testor::start()
{
    std::vector<int> random_idxs;
    generate_random(random_idxs, JOB_LENGTH, META, ZOOKEEPER);
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_kill = _job_index_to_kill[_job_types[id]];
        for (auto index : job_index_to_kill) {
            LOG_INFO("start to recover {}@{}", job_type_str(_job_types[id]), index);
            if (!start_job_by_index(_job_types[id], index)) {
                LOG_INFO("recover {}@{} failed", job_type_str(_job_types[id]), index);
                return false;
            }
            LOG_INFO("recover {}@{} succeed", job_type_str(_job_types[id]), index);
        }
    }
    return true;
}

bool process_kill_testor::kill_job_by_index(job_type type, int index)
{
    if (type == META)
        return _killer_handler->kill_meta(index);
    if (type == REPLICA)
        return _killer_handler->kill_replica(index);
    if (type == ZOOKEEPER)
        return _killer_handler->kill_zookeeper(index);
    return false;
}

bool process_kill_testor::start_job_by_index(job_type type, int index)
{
    if (type == META)
        return _killer_handler->start_meta(index);
    if (type == REPLICA)
        return _killer_handler->start_replica(index);
    if (type == ZOOKEEPER)
        return _killer_handler->start_zookeeper(index);
    return false;
}

void process_kill_testor::stop_verifier_and_exit(const char *msg)
{
    std::stringstream ss;
    int ret = dsn::utils::pipe_execute(
        "ps aux | grep pegasus | grep verifier | awk '{print $2}' | xargs kill -9", ss);
    CHECK(ret == 0 || ret == 256, "");
    LOG_FATAL(msg);
}

bool process_kill_testor::check_coredump()
{
    bool has_core = false;

    // make sure all generated core are logged
    for (int i = 1; i <= FLAGS_total_meta_count; ++i) {
        if (_killer_handler->has_meta_dumped_core(i)) {
            LOG_ERROR("meta server {} generate core dump", i);
            has_core = true;
        }
    }

    for (int i = 1; i <= FLAGS_total_replica_count; ++i) {
        if (_killer_handler->has_replica_dumped_core(i)) {
            LOG_ERROR("replica server {} generate core dump", i);
            has_core = true;
        }
    }

    return has_core;
}
} // namespace test
} // namespace pegasus
