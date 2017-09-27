// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "kill_testor.h"
#include <dsn/c/api_utilities.h>
#include <dsn/service_api_cpp.h>
#include <list>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "kill.testor"

namespace pegasus {
namespace test {

// help function that generate random
void generate_random(std::vector<int> &res, int cnt, int a, int b)
{
    res.clear();
    if (a > b)
        std::swap(a, b);
    cnt = std::min(cnt, b - a + 1);
    std::unordered_set<int> numbers;
    int tvalue;
    for (int i = 0; i < cnt; i++) {
        tvalue = (rand() % (b - a + 1)) + a;
        while (numbers.find(tvalue) != numbers.end()) {
            tvalue = (rand() % (b - a + 1)) + a;
        }
        numbers.insert(tvalue);
        res.emplace_back(tvalue);
    }
}

// [a, b]
int generate_one_number(int a, int b)
{
    if (a > b)
        std::swap(a, b);
    return ((rand() % (b - a + 1)) + a);
}

// kill_testor functions
kill_testor::kill_testor()
{
    const char *section = "pegasus.killtest";
    kill_round = 0;
    _log_filename = dsn_config_get_value_string(
        section, "kill_log_file", "./kill_history.txt", "kill log file");
    dassert(_log_filename.size() > 0, "");
    _log_handler.reset(new std::ofstream(_log_filename.c_str(), std::ios_base::out));
    dassert(_log_handler != nullptr, "");
    // initialize killer_handler
    std::string killer_name =
        dsn_config_get_value_string(section, "killer_handler", "", "killer handler");
    dassert(killer_name.size() > 0, "");
    if (killer_name == "shell")
        _killer_handler.reset(new killer_handler_shell(_log_handler));
    else {
        dassert(false, "invalid killer_handler, name = %s", killer_name.c_str());
    }
    _job_types = {META, REPLICA, ZOOKEEPER};
    _job_index_to_kill.resize(JOB_LENGTH);
    _sleep_time_before_recover_seconds =
        (uint32_t)dsn_config_get_value_uint64(section,
                                              "sleep_time_before_recover_seconds",
                                              30, // unit is second,default is 30s
                                              "sleep time before recover seconds");
    _total_meta_count = (int32_t)dsn_config_get_value_uint64(section,
                                                             "total_meta_count",
                                                             0, // default is 0
                                                             "total meta count");
    _total_replica_count = (int32_t)dsn_config_get_value_uint64(section,
                                                                "total_replica_count",
                                                                0, // default is 0
                                                                "total replica count");
    _total_zookeeper_count = (int32_t)dsn_config_get_value_uint64(section,
                                                                  "total_zookeeper_count",
                                                                  0, // default is 0
                                                                  "total zookeeper count");
    if (_total_meta_count == 0 && _total_replica_count == 0 && _total_zookeeper_count == 0) {
        dassert(false, "total number of meta/replica/zookeeper is 0");
    }
    _replica_killed_max_count = (int32_t)dsn_config_get_value_uint64(
        section,
        "replica_killed_max_count",
        _total_replica_count, // default is the total number of replica-servers
        "replica killed max count");
    _meta_killed_max_count = (int32_t)dsn_config_get_value_uint64(
        section,
        "meta_killed_max_count",
        _total_meta_count, // default is the total number of replica-servers
        "meta killed max count");
    _zk_killed_max_count = (int32_t)dsn_config_get_value_uint64(
        section,
        "zookeeper_killed_max_count",
        _total_zookeeper_count, // default is the total number of replica-servers
        "zookeeper killed max count");
    srand((unsigned)time(NULL));
}

kill_testor::~kill_testor() { _log_handler->close(); }

void kill_testor::run()
{
    std::ofstream &flog = *_log_handler;
    if (kill_round == 0) {
        flog << "Number of meta-server: " << _total_meta_count << std::endl;
        flog << "Number of replica-server: " << _total_replica_count << std::endl;
        flog << "Number of zookeeper-server: " << _total_zookeeper_count << std::endl;
    }
    kill_round += 1;
    int meta_cnt = 0;
    int replica_cnt = 0;
    int zk_cnt = 0;
    while ((meta_cnt == 0 && replica_cnt == 0 && zk_cnt == 0) ||
           (meta_cnt == _total_meta_count && replica_cnt == _total_replica_count &&
            zk_cnt == _total_zookeeper_count)) {
        meta_cnt = generate_one_number(0, _meta_killed_max_count);
        replica_cnt = generate_one_number(0, _replica_killed_max_count);
        zk_cnt = generate_one_number(0, _zk_killed_max_count);
    }
    flog << std::endl << "****************" << std::endl;
    flog << "Round [" << kill_round << "]" << std::endl;
    flog << "start kill..." << std::endl;
    flog << "kill meta number=" << meta_cnt << " replica number=" << replica_cnt
         << " zookeeper number=" << zk_cnt << std::endl;
    if (!kill(meta_cnt, replica_cnt, zk_cnt)) {
        dassert(false, "kill jobs failed");
    }
    // sleep time [1, _sleep_time_before_recover_seconds]
    auto sleep_time_random_seconds = generate_one_number(1, _sleep_time_before_recover_seconds);
    flog << "sleep time: " << sleep_time_random_seconds << "s" << std::endl;
    sleep(sleep_time_random_seconds);
    flog << "start recover..." << std::endl;
    if (!start()) {
        dassert(false, "recover jobs failed");
    }
    flog << "after recover..." << std::endl;
}

bool kill_testor::kill(int meta_cnt, int replica_cnt, int zookeeper_cnt)
{
    std::ofstream &out = *_log_handler;
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
            out << "kill ";
            if (!kill_job_by_index(_job_types[id], index)) {
                out << " fail..." << std::endl;
                return false;
            }
            out << " success..." << std::endl;
        }
    }
    return true;
}

bool kill_testor::start()
{
    std::ofstream &out = *_log_handler;
    std::vector<int> random_idxs;
    generate_random(random_idxs, JOB_LENGTH, META, ZOOKEEPER);
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_kill = _job_index_to_kill[_job_types[id]];
        for (auto index : job_index_to_kill) {
            out << "start ";
            if (!start_job_by_index(_job_types[id], index)) {
                out << " fail..." << std::endl;
                return false;
            }
            out << " success..." << std::endl;
        }
    }
    return true;
}

bool kill_testor::kill_job_by_index(job_type type, int index)
{
    if (type == META)
        return _killer_handler->kill_meta(index);
    if (type == REPLICA)
        return _killer_handler->kill_replica(index);
    if (type == ZOOKEEPER)
        return _killer_handler->kill_zookeeper(index);
    return false;
}

bool kill_testor::start_job_by_index(job_type type, int index)
{
    if (type == META)
        return _killer_handler->start_meta(index);
    if (type == REPLICA)
        return _killer_handler->start_replica(index);
    if (type == ZOOKEEPER)
        return _killer_handler->start_zookeeper(index);
    return false;
}
}
} // end namespace
