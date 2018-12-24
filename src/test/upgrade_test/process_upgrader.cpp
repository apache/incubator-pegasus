// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>

#include "base/pegasus_const.h"
#include "upgrader_registry.h"
#include "upgrade_testor.h"
#include "process_upgrader.h"

using namespace std;
using namespace ::pegasus;
using namespace ::pegasus::test;
using ::dsn::partition_configuration;
using ::dsn::replication::replication_ddl_client;

static shared_ptr<replication_ddl_client> ddl_client;
static string app_name;
static string pegasus_cluster_name;
static vector<dsn::rpc_address> meta_list;
static shared_ptr<upgrade_testor> upgradetestor;

static int upgrade_interval_seconds = 30;
static uint32_t max_seconds_for_partitions_recover = 600;

dsn::error_code
get_partition_info(bool debug_unhealthy, int &healthy_partition_cnt, int &unhealthy_partition_cnt)
{
    healthy_partition_cnt = 0, unhealthy_partition_cnt = 0;
    int32_t app_id;
    int32_t partition_count;
    std::vector<partition_configuration> partitions;
    dsn::error_code err = ddl_client->list_app(app_name, app_id, partition_count, partitions);
    if (err == ::dsn::ERR_OK) {
        dinfo("access meta and query partition status success");
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
                if (debug_unhealthy) {
                    ddebug("found unhealthy partition, %s", info.str().c_str());
                } else {
                    dinfo("found unhealthy partition, %s", info.str().c_str());
                }
            }
        }
        unhealthy_partition_cnt = partition_count - healthy_partition_cnt;
    } else {
        dinfo("access meta and query partition status fail");
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
    while (try_count <= max_seconds_for_partitions_recover) {
        dsn::error_code err = get_partition_info(try_count == max_seconds_for_partitions_recover,
                                                 healthy_partition_cnt,
                                                 unhealthy_partition_cnt);
        if (err == dsn::ERR_OK) {
            if (unhealthy_partition_cnt > 0) {
                dinfo("query partition status success, but still have unhealthy partition, "
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

    return false;
}

void upgrader_initialize(const char *config_file)
{
    register_upgrade_handlers();
    const char *section = "pegasus.upgradetest";
    // initialize the _client.
    if (!pegasus_client_factory::initialize(config_file)) {
        exit(-1);
    }

    app_name = dsn_config_get_value_string(
        section, "verify_app_name", "temp", "verify app name"); // default using temp
    pegasus_cluster_name =
        dsn_config_get_value_string(section, "pegasus_cluster_name", "", "pegasus cluster name");
    if (pegasus_cluster_name.empty()) {
        derror("Should config the cluster name for upgradetest");
        exit(-1);
    }

    // load meta_list
    meta_list.clear();
    dsn::replication::replica_helper::load_meta_servers(
        meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), pegasus_cluster_name.c_str());
    if (meta_list.empty()) {
        derror("Should config the meta address for upgrader");
        exit(-1);
    }

    ddl_client.reset(new replication_ddl_client(meta_list));
    if (ddl_client == nullptr) {
        derror("Initialize the _ddl_client failed");
        exit(-1);
    }

    upgrade_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64(section, "upgrade_interval_seconds", 30, "");
    max_seconds_for_partitions_recover = (uint32_t)dsn_config_get_value_uint64(
        section, "max_seconds_for_all_partitions_to_recover", 600, "");

    upgradetestor.reset(new upgrade_testor());
    if (upgradetestor == nullptr) {
        derror("upgradetestor initialize fail");
        exit(-1);
    }
}

bool verifier_process_alive()
{
    const char *command = "ps aux | grep pegasus | grep verifier | wc -l";
    std::stringstream output;
    int process_count;

    assert(dsn::utils::pipe_execute(command, output) == 0);
    output >> process_count;

    // one for the verifier, one for command
    return process_count > 1;
}

void upgrader_start()
{
    ddebug("begin the upgrade-thread");
    while (true) {
        if (!check_cluster_status()) {
            upgrade_testor::stop_verifier_and_exit("check_cluster_status() fail, and exit");
        }
        if (!verifier_process_alive()) {
            upgrade_testor::stop_verifier_and_exit("the verifier process is dead");
        }
        upgradetestor->run();
        ddebug("sleep %d seconds before checking", upgrade_interval_seconds);
        sleep(upgrade_interval_seconds);
    }
}
