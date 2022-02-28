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

#pragma once
#include "replica/replica.h"

namespace dsn {
namespace replication {

class replica_follower : replica_base
{
public:
    explicit replica_follower(replica *r);
    ~replica_follower();
    error_code duplicate_checkpoint();

    const std::string &get_master_cluster_name() const { return _master_cluster_name; };

    const std::string &get_master_app_name() const { return _master_app_name; };

    const std::vector<rpc_address> &get_master_meta_list() const { return _master_meta_list; };

    const bool is_need_duplicate() const { return need_duplicate; }

private:
    replica *_replica;
    task_tracker _tracker;

    std::string _master_cluster_name;
    std::string _master_app_name;
    std::vector<rpc_address> _master_meta_list;

    bool need_duplicate{false};

    void init_master_info();

    friend class replica_follower_test;
};

} // namespace replication
} // namespace dsn
