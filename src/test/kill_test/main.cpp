// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "killer_handler_shell.h"
#include "kill_testor.h"
#include <pegasus/client.h>
#include <dsn/dist/replication/replication_ddl_client.h>
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

using namespace std;
using namespace ::pegasus;
using namespace ::pegasus::test;
using ::dsn::partition_configuration;
using ::dsn::replication::replication_ddl_client;

shared_ptr<replication_ddl_client> ddl_client;
pegasus_client *client = nullptr;
string app_name;
string pegasus_cluster_name;
vector<dsn::rpc_address> meta_list;
uint32_t set_and_get_timeout_milliseconds;
shared_ptr<kill_testor> killtestor;
int set_thread_count = 0;

std::atomic_llong set_next(0);
int get_thread_count = 0;
std::vector<long long> set_thread_setting_id;

int kill_interval_seconds = 30; // default 30s

uint32_t max_time_for_partitions_recover = 600;

const char *set_next_key = "set_next";
const char *check_max_key = "check_max";
const char *hash_key_prefix = "kill_test_hash_key_";
const char *sort_key_prefix = "kill_test_sort_key_";
const char *value_prefix = "kill_test_value_";
const long stat_batch = 100000;
const long stat_min_pos = 0;
const long stat_p90_pos = stat_batch - stat_batch / 10 - 1;
const long stat_p99_pos = stat_batch - stat_batch / 100 - 1;
const long stat_p999_pos = stat_batch - stat_batch / 1000 - 1;
const long stat_p9999_pos = stat_batch - stat_batch / 10000 - 1;
const long stat_max_pos = stat_batch - 1;

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "pegasus.kill.test"

// return time in us.
long get_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

long long get_min_thread_setting_id()
{
    long long id = set_thread_setting_id[0];
    for (int i = 1; i < set_thread_count; ++i) {
        if (set_thread_setting_id[i] < id)
            id = set_thread_setting_id[i];
    }
    return id;
}

void do_set(int thread_id)
{
    char buf[1024];
    std::string hash_key;
    std::string sort_key;
    std::string value;
    long long id = 0;
    int try_count = 0;
    long stat_count = 0;
    std::vector<long> stat_time;
    stat_time.resize(stat_batch);
    long last_time = get_time();
    while (true) {
        if (try_count == 0) {
            id = set_next++;
            set_thread_setting_id[thread_id] = id;
            sprintf(buf, "%s%lld", hash_key_prefix, id);
            hash_key.assign(buf);
            sprintf(buf, "%s%lld", sort_key_prefix, id);
            sort_key.assign(buf);
            sprintf(buf, "%s%lld", value_prefix, id);
            value.assign(buf);
        }
        pegasus_client::internal_info info;
        int ret =
            client->set(hash_key, sort_key, value, set_and_get_timeout_milliseconds, 0, &info);
        if (ret == PERR_OK) {
            long cur_time = get_time();
            ddebug("SetThread[%d]: set succeed: id=%lld, try=%d, time=%ld (gpid=%d.%d, "
                   "decree=%lld, server=%s)",
                   thread_id,
                   id,
                   try_count,
                   (cur_time - last_time),
                   info.app_id,
                   info.partition_index,
                   info.decree,
                   info.server.c_str());
            stat_time[stat_count++] = cur_time - last_time;
            if (stat_count == stat_batch) {
                std::sort(stat_time.begin(), stat_time.end());
                long total_time = 0;
                for (auto t : stat_time)
                    total_time += t;
                ddebug("SetThread[%d]: set statistics: count=%lld, min=%lld, P90=%lld, P99=%lld, "
                       "P999=%lld, P9999=%lld, max=%lld, avg=%lld",
                       thread_id,
                       stat_count,
                       stat_time[stat_min_pos],
                       stat_time[stat_p90_pos],
                       stat_time[stat_p99_pos],
                       stat_time[stat_p999_pos],
                       stat_time[stat_p9999_pos],
                       stat_time[stat_max_pos],
                       total_time / stat_batch);
                stat_count = 0;
            }
            last_time = cur_time;
            try_count = 0;
        } else {
            derror("SetThread[%d]: set failed: id=%lld, try=%d, ret=%d, error=%s (gpid=%d.%d, "
                   "decree=%lld, server=%s)",
                   thread_id,
                   id,
                   try_count,
                   ret,
                   client->get_error_string(ret),
                   info.app_id,
                   info.partition_index,
                   info.decree,
                   info.server.c_str());
            try_count++;
            if (try_count > 3) {
                sleep(1);
            }
        }
    }
}

// for each round:
// - loop from range [start_id, end_id]
void do_get_range(int thread_id, int round_id, long long start_id, long long end_id)
{
    ddebug(
        "GetThread[%d]: round(%d): start get range [%u,%u]", thread_id, round_id, start_id, end_id);
    char buf[1024];
    std::string hash_key;
    std::string sort_key;
    std::string value;
    long long id = start_id;
    int try_count = 0;
    long stat_count = 0;
    std::vector<long> stat_time;
    stat_time.resize(stat_batch);
    long last_time = get_time();
    while (id <= end_id) {
        if (try_count == 0) {
            sprintf(buf, "%s%lld", hash_key_prefix, id);
            hash_key.assign(buf);
            sprintf(buf, "%s%lld", sort_key_prefix, id);
            sort_key.assign(buf);
            sprintf(buf, "%s%lld", value_prefix, id);
            value.assign(buf);
        }
        pegasus_client::internal_info info;
        std::string get_value;
        int ret =
            client->get(hash_key, sort_key, get_value, set_and_get_timeout_milliseconds, &info);
        if (ret == PERR_OK || ret == PERR_NOT_FOUND) {
            long cur_time = get_time();
            if (ret == PERR_NOT_FOUND) {
                dfatal("GetThread[%d]: round(%d): get not found: id=%lld, try=%d, time=%ld "
                       "(gpid=%d.%d, server=%s), and exit",
                       thread_id,
                       round_id,
                       id,
                       try_count,
                       (cur_time - last_time),
                       info.app_id,
                       info.partition_index,
                       info.server.c_str());
                exit(-1);
            } else if (value != get_value) {
                dfatal("GetThread[%d]: round(%d): get mismatched: id=%lld, try=%d, time=%ld, "
                       "expect_value=%s, real_value=%s (gpid=%d.%d, server=%s), and exit",
                       thread_id,
                       round_id,
                       id,
                       try_count,
                       (cur_time - last_time),
                       value.c_str(),
                       get_value.c_str(),
                       info.app_id,
                       info.partition_index,
                       info.server.c_str());
                exit(-1);
            } else {
                dinfo("GetThread[%d]: round(%d): get succeed: id=%lld, try=%d, time=%ld "
                      "(gpid=%d.%d, server=%s)",
                      thread_id,
                      round_id,
                      id,
                      try_count,
                      (cur_time - last_time),
                      info.app_id,
                      info.partition_index,
                      info.server.c_str());
                stat_time[stat_count++] = cur_time - last_time;
                if (stat_count == stat_batch) {
                    std::sort(stat_time.begin(), stat_time.end());
                    long total_time = 0;
                    for (auto t : stat_time)
                        total_time += t;
                    ddebug("GetThread[%d]: get statistics: count=%lld, min=%lld, P90=%lld, "
                           "P99=%lld, P999=%lld, P9999=%lld, max=%lld, avg=%lld",
                           thread_id,
                           stat_count,
                           stat_time[stat_min_pos],
                           stat_time[stat_p90_pos],
                           stat_time[stat_p99_pos],
                           stat_time[stat_p999_pos],
                           stat_time[stat_p9999_pos],
                           stat_time[stat_max_pos],
                           total_time / stat_batch);
                    stat_count = 0;
                }
            }
            last_time = cur_time;
            try_count = 0;
            id++;
        } else {
            derror("GetThread[%d]: round(%d): get failed: id=%lld, try=%d, ret=%d, error=%s "
                   "(gpid=%d.%d, server=%s)",
                   thread_id,
                   round_id,
                   id,
                   try_count,
                   ret,
                   client->get_error_string(ret),
                   info.app_id,
                   info.partition_index,
                   info.server.c_str());
            try_count++;
            if (try_count > 3) {
                sleep(1);
            }
        }
    }
    ddebug("GetThread[%d]: round(%d): finish get range [%u,%u]",
           thread_id,
           round_id,
           start_id,
           end_id);
}

void do_check(int thread_count)
{
    int round_id = 1;
    while (true) {
        long long range_end = get_min_thread_setting_id() - 1;
        if (range_end < thread_count) {
            sleep(1);
            continue;
        }
        ddebug("CheckThread: round(%d): start check round, range_end=%lld", round_id, range_end);
        long start_time = get_time();
        std::vector<std::thread> worker_threads;
        long long piece_count = range_end / thread_count;
        for (int i = 0; i < thread_count; ++i) {
            long long start_id = piece_count * i;
            long long end_id = (i == thread_count - 1) ? range_end : (piece_count * (i + 1) - 1);
            worker_threads.emplace_back(do_get_range, i, round_id, start_id, end_id);
        }
        for (auto &t : worker_threads) {
            t.join();
        }
        long finish_time = get_time();
        ddebug("CheckThread: round(%d): finish check round, range_end=%lld, total_time=%ld seconds",
               round_id,
               range_end,
               (finish_time - start_time) / 1000000);

        // update check_max
        while (true) {
            char buf[1024];
            sprintf(buf, "%lld", range_end);
            int ret = client->set(check_max_key, "", buf, set_and_get_timeout_milliseconds);
            if (ret == PERR_OK) {
                ddebug("CheckThread: round(%d): update \"%s\" succeed: check_max=%lld",
                       round_id,
                       check_max_key,
                       range_end);
                break;
            } else {
                derror("CheckThread: round(%d): update \"%s\" failed: check_max=%lld, ret=%d, "
                       "error=%s",
                       round_id,
                       check_max_key,
                       range_end,
                       ret,
                       client->get_error_string(ret));
            }
        }

        round_id++;
    }
}

void do_mark()
{
    char buf[1024];
    long last_time = get_time();
    long long old_id = 0;
    std::string value;
    while (true) {
        sleep(1);
        long long new_id = get_min_thread_setting_id();
        dassert(new_id >= old_id, "%" PRId64 " VS %" PRId64 "", new_id, old_id);
        if (new_id == old_id) {
            continue;
        }
        sprintf(buf, "%lld", new_id);
        value.assign(buf);
        int ret = client->set(set_next_key, "", value, set_and_get_timeout_milliseconds);
        if (ret == PERR_OK) {
            long cur_time = get_time();
            ddebug("MarkThread: update \"%s\" succeed: set_next=%lld, time=%ld",
                   set_next_key,
                   new_id,
                   (cur_time - last_time));
            old_id = new_id;
        } else {
            derror("MarkThread: update \"%s\" failed: set_next=%lld, ret=%d, error=%s",
                   set_next_key,
                   new_id,
                   ret,
                   client->get_error_string(ret));
        }
    }
}

dsn::error_code get_partition_info(int &healthy_partition_cnt, int &unhealthy_partition_cnt)
{
    healthy_partition_cnt = 0, unhealthy_partition_cnt = 0;
    int32_t app_id;
    int32_t partition_count;
    std::vector<partition_configuration> partitions;
    dsn::error_code err = ddl_client->list_app(app_name, app_id, partition_count, partitions);
    if (err == ::dsn::ERR_OK) {
        ddebug("access meta and query partition status success");
        for (int i = 0; i < partitions.size(); i++) {
            const dsn::partition_configuration &p = partitions[i];
            int replica_count = 0;
            if (!p.primary.is_invalid()) {
                replica_count++;
            }
            replica_count += p.secondaries.size();
            if (replica_count == p.max_replica_count) {
                healthy_partition_cnt++;
            } else {
                std::stringstream info;
                info << "gpid=" << p.pid.get_app_id() << "." << p.pid.get_partition_index() << ", ";
                info << "primay=" << p.primary.to_std_string() << ", ";
                info << "secondaries=[";
                for (int idx = 0; idx < p.secondaries.size(); idx++) {
                    if (idx != 0)
                        info << "," << p.secondaries[idx].to_std_string();
                    else
                        info << p.secondaries[idx].to_std_string();
                }
                info << "], ";
                info << "last_committed_decree=" << p.last_committed_decree;
                ddebug("found unhealthy partition, %s", info.str().c_str());
            }
        }
        unhealthy_partition_cnt = partition_count - healthy_partition_cnt;
    } else {
        derror("access meta and query partition status fail");
        healthy_partition_cnt = 0;
        unhealthy_partition_cnt = 0;
    }
    return err;
}

// false == partition unhealth, true == health
bool check_cluster_status()
{
    int healthy_partition_cnt = 0;
    int unhealthy_partition_cnt = 0;
    int try_count = 1;
    while (try_count <= max_time_for_partitions_recover) {
        dsn::error_code err = get_partition_info(healthy_partition_cnt, unhealthy_partition_cnt);
        if (err == dsn::ERR_OK) {
            if (unhealthy_partition_cnt > 0) {
                ddebug("query partition status success, but still have unhealthy partition, "
                       "healthy_partition_count = %d, unhealthy_partition_count = %d",
                       healthy_partition_cnt,
                       unhealthy_partition_cnt);
                sleep(1);
            } else
                return true;
        } else {
            ddebug("query partition status fail, try times = %d", try_count);
            sleep(1);
        }
        try_count += 1;
    }
    return (try_count <= max_time_for_partitions_recover);
}

void killer_func()
{
    ddebug("begin the kill-thread");
    while (true) {
        killtestor->run();
        if (!check_cluster_status()) {
            dfatal("killer_func: check_cluster_status() fail, and exit");
            exit(-1);
        }
        sleep(kill_interval_seconds); /* sleep time to restart the kill */
    }
}

// whether server crash and generate the coredump
bool find_coredump() { return false; }

// before running, should call this function.
bool initialize(const char *config_file)
{
    const char *section = "pegasus.killtest";
    // initialize the _client.
    if (!pegasus_client_factory::initialize(config_file))
        return false;
    app_name = dsn_config_get_value_string(
        section, "verify_app_name", "temp", "verify app name"); // default using temp
    pegasus_cluster_name =
        dsn_config_get_value_string(section, "pegasus_cluster_name", "", "pegasus cluster name");
    if (pegasus_cluster_name.empty()) {
        ddebug("Should config the cluster name for killtest");
        return false;
    }
    client = pegasus_client_factory::get_client(pegasus_cluster_name.c_str(), app_name.c_str());
    if (client == nullptr) {
        ddebug("Initialize the _client failed");
        return false;
    }
    // load meta_list
    meta_list.clear();
    {
        std::string tmp_section = "uri-resolver.dsn://" + pegasus_cluster_name;
        dsn::replication::replica_helper::load_meta_servers(
            meta_list, tmp_section.c_str(), "arguments");
    }
    if (meta_list.empty()) {
        ddebug("Should config the meta address for killtest");
        return false;
    }
    ddl_client.reset(new replication_ddl_client(meta_list));
    if (ddl_client == nullptr) {
        ddebug("Initialize the _ddl_client failed");
        return false;
    }
    set_and_get_timeout_milliseconds =
        (uint32_t)dsn_config_get_value_uint64(section,
                                              "set_and_get_timeout_milliseconds",
                                              3000, // unit is millisecond,default is 3000ms
                                              "set and get timeout milliseconds");
    set_thread_count = (uint32_t)dsn_config_get_value_uint64(section,
                                                             "set_thread_count",
                                                             5, // default is 5 clients
                                                             "set thread count");
    get_thread_count =
        (uint32_t)dsn_config_get_value_uint64(section,
                                              "get_thread_count",
                                              set_thread_count * 4, // default is 4*set_thread_count
                                              "get thread count");
    kill_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64(section,
                                              "kill_interval_seconds",
                                              30, // unit is second,default is 30s
                                              "kill interval seconds");
    max_time_for_partitions_recover =
        (uint32_t)dsn_config_get_value_uint64(section,
                                              "max_time_for_all_partitions_to_recover_seconds",
                                              600, // unit is second,default is 30s
                                              "max time for all partitions to recover seconds");
    killtestor.reset(new kill_testor());
    if (killtestor == nullptr) {
        ddebug("killtestor initialize fail");
        return false;
    }
    return true;
}

int main(int argc, const char **argv)
{
    if (argc != 2) {
        derror("Usage: %s <config_file>", argv[0]);
        return -1;
    }
    if (!initialize(argv[1])) {
        ddebug("Initialize the killtest fail");
        return -2;
    }
    // check the set_next
    while (true) {
        std::string set_next_value;
        int ret = client->get(set_next_key, "", set_next_value, set_and_get_timeout_milliseconds);
        if (ret == PERR_OK) {
            long long i = atoll(set_next_value.c_str());
            if (i == 0 && !set_next_value.empty()) {
                derror("MainThread: read \"%s\" failed: value_str=%s",
                       set_next_key,
                       set_next_value.c_str());
                return -1;
            }
            ddebug("MainThread: read \"%s\" succeed: value=%lld", set_next_key, i);
            set_next.store(i);
            break;
        } else if (ret == PERR_NOT_FOUND) {
            ddebug("MainThread: read \"%s\" not found, init set_next to 0", set_next_key);
            set_next.store(0);
            break;
        } else {
            derror("MainThread: read \"%s\" failed: error=%s",
                   set_next_key,
                   client->get_error_string(ret));
        }
    }
    set_thread_setting_id.resize(set_thread_count);

    std::vector<std::thread> set_threads;
    for (int i = 0; i < set_thread_count; ++i) {
        set_threads.emplace_back(do_set, i);
    }

    std::thread mark_thread(do_mark);
    std::thread kill_thread(killer_func);

    do_check(get_thread_count);

    mark_thread.join();

    kill_thread.join();

    for (auto &t : set_threads) {
        t.join();
    }
    return 0;
}
