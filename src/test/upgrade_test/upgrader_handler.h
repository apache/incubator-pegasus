// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <unordered_set>

namespace pegasus {
namespace test {

// define the interface that how to upgrade and downgrade the jobs [meta, replica, zookeeper].
class upgrader_handler
{
public:
    // index begin from 1, not zero
    // upgrade one
    virtual bool upgrade_meta(int index) = 0;
    virtual bool upgrade_replica(int index) = 0;
    virtual bool upgrade_zookeeper(int index) = 0;
    // downgrade one
    virtual bool downgrade_meta(int index) = 0;
    virtual bool downgrade_replica(int index) = 0;
    virtual bool downgrade_zookeeper(int index) = 0;
    // upgrade all meta/replica/zookeeper
    virtual bool upgrade_all_meta(std::unordered_set<int> &) = 0;
    virtual bool upgrade_all_replica(std::unordered_set<int> &) = 0;
    virtual bool upgrade_all_zookeeper(std::unordered_set<int> &) = 0;
    // downgrade all meta/replica/zookeeper
    virtual bool downgrade_all_meta(std::unordered_set<int> &) = 0;
    virtual bool downgrade_all_replica(std::unordered_set<int> &) = 0;
    virtual bool downgrade_all_zookeeper(std::unordered_set<int> &) = 0;

    virtual bool has_meta_dumped_core(int index) { return false; }
    virtual bool has_replica_dumped_core(int index) { return false; }
    virtual bool has_zookeeper_dumped_core(int index) { return false; }
};
}
} // end namespace
