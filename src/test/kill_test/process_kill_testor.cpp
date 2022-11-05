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

#include <vector>
#include <bitset>
#include <thread>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <sys/time.h>

#include "utils/fmt_logging.h"
#include "client/replication_ddl_client.h"

#include <pegasus/client.h>

#include "killer_registry.h"
#include "killer_handler.h"
#include "killer_handler_shell.h"
#include "process_kill_testor.h"

namespace pegasus {
namespace test {
process_kill_testor::process_kill_testor(const char *config_file) : kill_testor(config_file)
{
    register_kill_handlers();

    const char *section = "pegasus.killtest";
    kill_round = 0;

    // initialize killer_handler
    std::string killer_name =
        dsn_config_get_value_string(section, "killer_handler", "", "killer handler");
    CHECK(!killer_name.empty(), "");
    _killer_handler.reset(killer_handler::new_handler(killer_name.c_str()));
    CHECK(_killer_handler, "invalid killer_name({})", killer_name);

    _job_types = {META, REPLICA, ZOOKEEPER};
    _job_index_to_kill.resize(JOB_LENGTH);
    _sleep_time_before_recover_seconds = (uint32_t)dsn_config_get_value_uint64(
        section, "sleep_time_before_recover_seconds", 30, "sleep time before recover seconds");

    _total_meta_count =
        (int32_t)dsn_config_get_value_uint64(section, "total_meta_count", 0, "total meta count");
    _total_replica_count = (int32_t)dsn_config_get_value_uint64(
        section, "total_replica_count", 0, "total replica count");
    _total_zookeeper_count = (int32_t)dsn_config_get_value_uint64(
        section, "total_zookeeper_count", 0, "total zookeeper count");

    if (_total_meta_count == 0 && _total_replica_count == 0 && _total_zookeeper_count == 0) {
        CHECK(false, "total number of meta/replica/zookeeper is 0");
    }

    _kill_replica_max_count = (int32_t)dsn_config_get_value_uint64(
        section, "kill_replica_max_count", _total_replica_count, "replica killed max count");
    _kill_meta_max_count = (int32_t)dsn_config_get_value_uint64(
        section, "kill_meta_max_count", _total_meta_count, "meta killed max count");
    _kill_zk_max_count = (int32_t)dsn_config_get_value_uint64(
        section, "kill_zookeeper_max_count", _total_zookeeper_count, "zookeeper killed max count");
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
        LOG_INFO("sleep %d seconds before checking", kill_interval_seconds);
        sleep(kill_interval_seconds);
    }
}

void process_kill_testor::run()
{
    if (check_coredump()) {
        stop_verifier_and_exit("detect core dump in pegasus cluster");
    }

    if (kill_round == 0) {
        LOG_INFO("Number of meta-server: %d", _total_meta_count);
        LOG_INFO("Number of replica-server: %d", _total_replica_count);
        LOG_INFO("Number of zookeeper: %d", _total_zookeeper_count);
    }
    kill_round += 1;
    int meta_cnt = 0;
    int replica_cnt = 0;
    int zk_cnt = 0;
    while ((meta_cnt == 0 && replica_cnt == 0 && zk_cnt == 0) ||
           (meta_cnt == _total_meta_count && replica_cnt == _total_replica_count &&
            zk_cnt == _total_zookeeper_count)) {
        meta_cnt = generate_one_number(0, _kill_meta_max_count);
        replica_cnt = generate_one_number(0, _kill_replica_max_count);
        zk_cnt = generate_one_number(0, _kill_zk_max_count);
    }
    LOG_INFO("************************");
    LOG_INFO("Round [%d]", kill_round);
    LOG_INFO("start kill...");
    LOG_INFO("kill meta number=%d, replica number=%d, zk number=%d", meta_cnt, replica_cnt, zk_cnt);

    if (!kill(meta_cnt, replica_cnt, zk_cnt)) {
        stop_verifier_and_exit("kill jobs failed");
    }

    auto sleep_time_random_seconds = generate_one_number(1, _sleep_time_before_recover_seconds);
    LOG_INFO("sleep %d seconds before recovery", sleep_time_random_seconds);
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
        _total_meta_count, _total_replica_count, _total_zookeeper_count};
    std::vector<int> random_idxs;
    generate_random(random_idxs, JOB_LENGTH, META, ZOOKEEPER);
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_kill = _job_index_to_kill[_job_types[id]];
        job_index_to_kill.clear();
        generate_random(job_index_to_kill, kill_counts[id], 1, total_count[id]);
        for (auto index : job_index_to_kill) {
            LOG_INFO("start to kill %s@%d", job_type_str(_job_types[id]), index);
            if (!kill_job_by_index(_job_types[id], index)) {
                LOG_INFO("kill %s@%d failed", job_type_str(_job_types[id]), index);
                return false;
            }
            LOG_INFO("kill %s@%d succeed", job_type_str(_job_types[id]), index);
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
            LOG_INFO("start to recover %s@%d", job_type_str(_job_types[id]), index);
            if (!start_job_by_index(_job_types[id], index)) {
                LOG_INFO("recover %s@%d failed", job_type_str(_job_types[id]), index);
                return false;
            }
            LOG_INFO("recover %s@%d succeed", job_type_str(_job_types[id]), index);
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
    system("ps aux | grep pegasus | grep verifier | awk '{print $2}' | xargs kill -9");
    CHECK(false, "{}", msg);
}

bool process_kill_testor::check_coredump()
{
    bool has_core = false;

    // make sure all generated core are logged
    for (int i = 1; i <= _total_meta_count; ++i) {
        if (_killer_handler->has_meta_dumped_core(i)) {
            LOG_ERROR("meta server %d generate core dump", i);
            has_core = true;
        }
    }

    for (int i = 1; i <= _total_replica_count; ++i) {
        if (_killer_handler->has_replica_dumped_core(i)) {
            LOG_ERROR("replica server %d generate core dump", i);
            has_core = true;
        }
    }

    return has_core;
}
} // namespace test
} // namespace pegasus
