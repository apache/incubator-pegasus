// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "upgrader_handler.h"
#include <memory>
#include <list>

namespace pegasus {
namespace test {

class upgrader_handler_shell : public upgrader_handler
{
public:
    upgrader_handler_shell();
    virtual ~upgrader_handler_shell() {}
    // index begin from 1, not zero
    // upgrade one
    virtual bool upgrade_meta(int index) override;
    virtual bool upgrade_replica(int index) override;
    virtual bool upgrade_zookeeper(int index) override;
    // downgrade one
    virtual bool downgrade_meta(int index) override;
    virtual bool downgrade_replica(int index) override;
    virtual bool downgrade_zookeeper(int index) override;
    // upgrade all meta/replica/zookeeper
    virtual bool upgrade_all_meta(std::unordered_set<int> &) override;
    virtual bool upgrade_all_replica(std::unordered_set<int> &) override;
    virtual bool upgrade_all_zookeeper(std::unordered_set<int> &) override;
    // downgrade all meta/replica/zookeeper
    virtual bool downgrade_all_meta(std::unordered_set<int> &) override;
    virtual bool downgrade_all_replica(std::unordered_set<int> &) override;
    virtual bool downgrade_all_zookeeper(std::unordered_set<int> &) override;

    virtual bool has_meta_dumped_core(int index) override;
    virtual bool has_replica_dumped_core(int index) override;

private:
    // action = upgrade | downgrade.
    std::list<std::string>
    generate_cmd(int index, const std::string &job, const std::string &action);
    // check whether the command execute success.
    bool check(const std::string &job, int index, const std::string &type);

private:
    // using ${_run_script_path}/run.sh to upgrade/downgrade
    std::string _run_script_path;
    std::string _new_version_path;
    std::string _old_version_path;
};
}
} // end namespace
