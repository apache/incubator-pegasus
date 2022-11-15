// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "perf_counter/perf_counter_wrapper.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/zlocks.h"
#include "utils/flags.h"

#include "replication_common.h"

namespace dsn {
namespace replication {

DSN_DECLARE_int32(disk_min_available_space_ratio);

struct dir_node
{
public:
    std::string tag;
    std::string full_dir;
    int64_t disk_capacity_mb;
    int64_t disk_available_mb;
    int disk_available_ratio;
    disk_status::type status;
    std::map<app_id, std::set<gpid>> holding_replicas;
    std::map<app_id, std::set<gpid>> holding_primary_replicas;
    std::map<app_id, std::set<gpid>> holding_secondary_replicas;

public:
    dir_node(const std::string &tag_,
             const std::string &dir_,
             int64_t disk_capacity_mb_ = 0,
             int64_t disk_available_mb_ = 0,
             int disk_available_ratio_ = 0,
             disk_status::type status_ = disk_status::NORMAL)
        : tag(tag_),
          full_dir(dir_),
          disk_capacity_mb(disk_capacity_mb_),
          disk_available_mb(disk_available_mb_),
          disk_available_ratio(disk_available_ratio_),
          status(status_)
    {
    }
    unsigned replicas_count(app_id id) const;
    unsigned replicas_count() const;
    bool has(const dsn::gpid &pid) const;
    unsigned remove(const dsn::gpid &pid);
    bool update_disk_stat(const bool update_disk_status);
};

class fs_manager
{
public:
    fs_manager(bool for_test);
    ~fs_manager() {}

    // this should be called before open/load any replicas
    dsn::error_code initialize(const replication_options &opts);
    dsn::error_code initialize(const std::vector<std::string> &data_dirs,
                               const std::vector<std::string> &tags,
                               bool for_test);

    dsn::error_code get_disk_tag(const std::string &dir, /*out*/ std::string &tag);
    void allocate_dir(const dsn::gpid &pid,
                      const std::string &type,
                      /*out*/ std::string &dir);
    void add_replica(const dsn::gpid &pid, const std::string &pid_dir);
    void remove_replica(const dsn::gpid &pid);
    bool for_each_dir_node(const std::function<bool(const dir_node &)> &func) const;
    void update_disk_stat(bool check_status_changed = true);

    void add_new_dir_node(const std::string &data_dir, const std::string &tag);
    bool is_dir_node_available(const std::string &data_dir, const std::string &tag) const;
    const std::vector<std::string> &get_available_data_dirs() const
    {
        zauto_read_lock l(_lock);
        return _available_data_dirs;
    }

private:
    void reset_disk_stat()
    {
        _total_capacity_mb = 0;
        _total_available_mb = 0;
        _total_available_ratio = 0;
        _min_available_ratio = 100;
        _max_available_ratio = 0;
        _status_updated_dir_nodes.clear();
    }

    dir_node *get_dir_node(const std::string &subdir);

    // when visit the tag/storage of the _dir_nodes map, there's no need to protect by the lock.
    // but when visit the holding_replicas, you must take care.
    mutable zrwlock_nr _lock;

    int64_t _total_capacity_mb = 0;
    int64_t _total_available_mb = 0;
    int _total_available_ratio = 0;
    int _min_available_ratio = 100;
    int _max_available_ratio = 0;

    std::vector<std::shared_ptr<dir_node>> _dir_nodes;
    std::vector<std::string> _available_data_dirs;

    // Used for disk available space check
    // disk status will be updated periodically, this vector record nodes whose disk_status changed
    // in this round
    std::vector<std::shared_ptr<dir_node>> _status_updated_dir_nodes;

    perf_counter_wrapper _counter_total_capacity_mb;
    perf_counter_wrapper _counter_total_available_mb;
    perf_counter_wrapper _counter_total_available_ratio;
    perf_counter_wrapper _counter_min_available_ratio;
    perf_counter_wrapper _counter_max_available_ratio;

    friend class replica_test;
    friend class replica_stub;
    friend class mock_replica_stub;
    friend class replica_disk_migrator;
    friend class replica_disk_test_base;
    friend class open_replica_test;
};
} // replication
} // dsn
