// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <unordered_set>

namespace pegasus {
namespace test {

// define the interface that how to kill and start the jobs [meta, replica, zookeeper].
class killer_handler
{
public:
    // index begin from 1, not zero
    // kill one
    virtual bool kill_meta(int index) = 0;
    virtual bool kill_replica(int index) = 0;
    virtual bool kill_zookeeper(int index) = 0;
    // start one
    virtual bool start_meta(int index) = 0;
    virtual bool start_replica(int index) = 0;
    virtual bool start_zookeeper(int index) = 0;
    // kill all meta/replica/zookeeper
    virtual bool kill_all_meta(std::unordered_set<int> &) = 0;
    virtual bool kill_all_replica(std::unordered_set<int> &) = 0;
    virtual bool kill_all_zookeeper(std::unordered_set<int> &) = 0;
    // start all meta/replica/zookeeper
    virtual bool start_all_meta(std::unordered_set<int> &) = 0;
    virtual bool start_all_replica(std::unordered_set<int> &) = 0;
    virtual bool start_all_zookeeper(std::unordered_set<int> &) = 0;

    virtual bool has_meta_dumped_core(int index) { return false; }
    virtual bool has_replica_dumped_core(int index) { return false; }
    virtual bool has_zookeeper_dumped_core(int index) { return false; }
};
}
} // end namespace
