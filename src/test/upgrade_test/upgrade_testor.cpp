// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <list>
#include <dsn/c/api_utilities.h>
#include <dsn/service_api_cpp.h>

#include "upgrade_testor.h"

namespace pegasus {
namespace test {

// 在[a,b]之间生成cnt个不同的随机数到res
void upgrade_testor::generate_random(std::vector<int> &res, int cnt, int a, int b)
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

int upgrade_testor::generate_one_number(int a, int b)
{
    if (a > b)
        std::swap(a, b);
    return ((rand() % (b - a + 1)) + a);
}

upgrade_testor::upgrade_testor()
{
    const char *section = "pegasus.upgradetest";
    upgrade_round = 0;

    // initialize upgrader_handler
    std::string upgrader_name =
        dsn_config_get_value_string(section, "upgrade_handler", "", "upgrade handler");
    dassert(upgrader_name.size() > 0, "");
    _upgrader_handler.reset(upgrader_handler::new_handler(upgrader_name.c_str()));
    dassert(_upgrader_handler.get() != nullptr, "invalid upgrader_name(%s)", upgrader_name.c_str());

    _job_types = {META, REPLICA, ZOOKEEPER};
    _job_index_to_upgrade.resize(JOB_LENGTH);
    _sleep_time_before_recover_seconds = (uint32_t)dsn_config_get_value_uint64(
        section, "sleep_time_before_recover_seconds", 30, "sleep time before recover seconds");

    _total_meta_count =
        (int32_t)dsn_config_get_value_uint64(section, "total_meta_count", 0, "total meta count");
    _total_replica_count = (int32_t)dsn_config_get_value_uint64(
        section, "total_replica_count", 0, "total replica count");
    _total_zookeeper_count = (int32_t)dsn_config_get_value_uint64(
        section, "total_zookeeper_count", 0, "total zookeeper count");

    if (_total_meta_count == 0 && _total_replica_count == 0 && _total_zookeeper_count == 0) {
        dassert(false, "total number of meta/replica/zookeeper is 0");
    }

    _upgrade_replica_max_count = (int32_t)dsn_config_get_value_uint64(
        section, "upgrade_replica_max_count", _total_replica_count, "replica upgradeed max count");
    _upgrade_meta_max_count = (int32_t)dsn_config_get_value_uint64(
        section, "upgrade_meta_max_count", _total_meta_count, "meta upgradeed max count");
    _upgrade_zk_max_count = (int32_t)dsn_config_get_value_uint64(section,
                                                                 "upgrade_zookeeper_max_count",
                                                                 _total_zookeeper_count,
                                                                 "zookeeper upgradeed max count");
    srand((unsigned)time(nullptr));
}

upgrade_testor::~upgrade_testor() {}

void upgrade_testor::stop_verifier_and_exit(const char *msg)
{
    system("ps aux | grep verifier | grep -v grep| awk '{print $2}' | xargs kill -9");
    dassert(false, "%s", msg);
}

bool upgrade_testor::check_coredump()
{
    bool has_core = false;

    // make sure all generated core are logged
    for (int i = 1; i <= _total_meta_count; ++i) {
        if (_upgrader_handler->has_meta_dumped_core(i)) {
            derror("meta server %d generate core dump", i);
            has_core = true;
        }
    }

    for (int i = 1; i <= _total_replica_count; ++i) {
        if (_upgrader_handler->has_replica_dumped_core(i)) {
            derror("replica server %d generate core dump", i);
            has_core = true;
        }
    }

    return has_core;
}

void upgrade_testor::run()
{
    if (check_coredump()) {
        stop_verifier_and_exit("detect core dump in pegasus cluster");
    }

    if (upgrade_round == 0) {
        ddebug("Number of meta-server: %d", _total_meta_count);
        ddebug("Number of replica-server: %d", _total_replica_count);
        ddebug("Number of zookeeper: %d", _total_zookeeper_count);
    }
    upgrade_round += 1;
    int meta_cnt = 0;
    int replica_cnt = 0;
    int zk_cnt = 0;
    while (replica_cnt == 0) {
        replica_cnt = generate_one_number(1, _upgrade_replica_max_count);
    }
    ddebug("************************");
    ddebug("Round [%d]", upgrade_round);
    ddebug("start upgrade...");
    ddebug(
        "upgrade meta number=%d, replica number=%d, zk number=%d", meta_cnt, replica_cnt, zk_cnt);

    if (!upgrade(replica_cnt)) {
        stop_verifier_and_exit("upgrade jobs failed");
    }

    auto sleep_time_random_seconds = generate_one_number(1, _sleep_time_before_recover_seconds);
    ddebug("sleep %d seconds before downgrade", sleep_time_random_seconds);
    sleep(sleep_time_random_seconds);

    ddebug("start downgrade...");
    if (!downgrade()) {
        stop_verifier_and_exit("downgrade jobs failed");
    }
    ddebug("after downgrade...");
    ddebug("************************");
}

bool upgrade_testor::upgrade(int replica_cnt)
{
    std::vector<int> upgrade_counts = {0, replica_cnt, 0};
    std::vector<int> total_count = {
        _total_meta_count, _total_replica_count, _total_zookeeper_count};
    std::vector<int> random_idxs;
    generate_random(random_idxs, 1 /*REPLICA - REPLICA + 1*/, REPLICA, REPLICA); // 生成type列表
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_upgrade = _job_index_to_upgrade[_job_types[id]];
        job_index_to_upgrade.clear();
        generate_random(job_index_to_upgrade,
                        upgrade_counts[id],
                        1,
                        total_count[id]); // 生成该type需要upgrade的index列表
        for (auto index : job_index_to_upgrade) {
            ddebug("start to upgrade %s@%d", job_type_str(_job_types[id]), index);
            if (!upgrade_job_by_index(_job_types[id], index)) {
                ddebug("upgrade %s@%d failed", job_type_str(_job_types[id]), index);
                return false;
            }
            ddebug("upgrade %s@%d succeed", job_type_str(_job_types[id]), index);
        }
    }
    return true;
}

bool upgrade_testor::downgrade()
{
    std::vector<int> random_idxs;
    generate_random(random_idxs, JOB_LENGTH, META, ZOOKEEPER);
    for (auto id : random_idxs) {
        std::vector<int> &job_index_to_upgrade = _job_index_to_upgrade[_job_types[id]];
        for (auto index : job_index_to_upgrade) {
            ddebug("start to downgrade %s@%d", job_type_str(_job_types[id]), index);
            if (!downgrade_job_by_index(_job_types[id], index)) {
                ddebug("downgrade %s@%d failed", job_type_str(_job_types[id]), index);
                return false;
            }
            ddebug("downgrade %s@%d succeed", job_type_str(_job_types[id]), index);
        }
    }
    return true;
}

bool upgrade_testor::upgrade_job_by_index(job_type type, int index)
{
    if (type == META)
        return _upgrader_handler->upgrade_meta(index);
    if (type == REPLICA)
        return _upgrader_handler->upgrade_replica(index);
    if (type == ZOOKEEPER)
        return _upgrader_handler->upgrade_zookeeper(index);
    return false;
}

bool upgrade_testor::downgrade_job_by_index(job_type type, int index)
{
    if (type == META)
        return _upgrader_handler->downgrade_meta(index);
    if (type == REPLICA)
        return _upgrader_handler->downgrade_replica(index);
    if (type == ZOOKEEPER)
        return _upgrader_handler->downgrade_zookeeper(index);
    return false;
}
}
} // end namespace
