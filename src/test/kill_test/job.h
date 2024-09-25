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

#include <vector>
#include <string>
#include <unordered_map>

#include "common/replication_common.h"

namespace pegasus {
namespace test {

enum job_type
{
    META = 0,
    REPLICA = 1,
    ZOOKEEPER = 2,
    JOB_LENGTH = 3
};

inline const char *job_type_str(enum job_type type)
{
    switch (type) {
    case META:
        return "meta";
    case REPLICA:
        return dsn::replication::replication_options::kReplicaAppType.c_str();
    case ZOOKEEPER:
        return "zookeeper";
    default:
        return "invalid";
    }
}

struct job
{
    std::vector<std::string> addrs;
    std::unordered_map<std::string, bool> status;
    std::string name;
    job() {}
    job(const std::string &_name) : name(_name) {}
    void append_addrs(const std::string &addr);
    std::string get_addr_by_index(int index);
    void set_name(const std::string &_name);
};
} // namespace test
} // namespace pegasus
