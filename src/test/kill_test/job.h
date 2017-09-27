// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <vector>
#include <string>
#include <unordered_map>

namespace pegasus {
namespace test {

enum job_type
{
    META = 0,
    REPLICA = 1,
    ZOOKEEPER = 2,
    JOB_LENGTH = 3
};

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
}
} // end namespace
