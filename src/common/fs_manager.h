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

#include <stdint.h>
#include <functional>
#include <gtest/gtest_prod.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/zlocks.h"

namespace dsn {
class gpid;

namespace replication {
class replication_options;

DSN_DECLARE_int32(disk_min_available_space_ratio);

struct dir_node
{
public:
    const std::string tag;
    const std::string full_dir;
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
    // All functions are not thread-safe. However, they are only used in fs_manager
    // and protected by the lock in fs_manager.
    uint64_t replicas_count(app_id id) const;
    uint64_t replicas_count() const;
    bool has(const dsn::gpid &pid) const;
    uint64_t remove(const dsn::gpid &pid);
    bool update_disk_stat(const bool update_disk_status);
};

class fs_manager
{
public:
    fs_manager();

    // Should be called before open/load any replicas.
    // NOTE: 'data_dirs' and 'tags' must have the same size and in the same order.
    void initialize(const std::vector<std::string> &data_dirs,
                    const std::vector<std::string> &data_dir_tags);

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

    dir_node *get_dir_node(const std::string &subdir) const;

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
    FRIEND_TEST(replica_test, test_auto_trash);
};
} // replication
} // dsn
