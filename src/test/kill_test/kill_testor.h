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

#include <fstream>
#include <memory>
#include <vector>

#include "dsn.layer2_types.h"
#include "rpc/rpc_host_port.h"
#include "utils/error_code.h"

namespace dsn {
namespace replication {
class replication_ddl_client;
} // namespace replication
} // namespace dsn

namespace pegasus {
namespace test {
using namespace std;
using ::dsn::partition_configuration;
using ::dsn::replication::replication_ddl_client;

class kill_testor
{
public:
    kill_testor(const char *config_file);
    ~kill_testor();

    virtual void Run() = 0;

protected:
    kill_testor();

    // generate cnt number belong to [a, b],
    // if cnt > (b - a + 1), then just return the numbers between a ~ b
    void generate_random(std::vector<int> &res, int cnt, int a, int b);

    // generate one number belong to [a, b]
    int generate_one_number(int a, int b);

    dsn::error_code get_partition_info(bool debug_unhealthy,
                                       int &healthy_partition_cnt,
                                       int &unhealthy_partition_cnt);
    bool check_cluster_status();

protected:
    shared_ptr<replication_ddl_client> ddl_client;
    vector<dsn::host_port> meta_list;

    std::vector<partition_configuration> pcs;
};
} // namespace test
} // namespace pegasus
