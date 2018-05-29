// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <memory>

#include "killer_handler.h"

namespace pegasus {
namespace test {

class killer_handler_shell : public killer_handler
{
public:
    killer_handler_shell();
    virtual ~killer_handler_shell() {}
    // index begin from 1, not zero
    // kill one
    virtual bool kill_meta(int index) override;
    virtual bool kill_replica(int index) override;
    virtual bool kill_zookeeper(int index) override;
    // start one
    virtual bool start_meta(int index) override;
    virtual bool start_replica(int index) override;
    virtual bool start_zookeeper(int index) override;
    // kill all meta/replica/zookeeper
    virtual bool kill_all_meta(std::unordered_set<int> &) override;
    virtual bool kill_all_replica(std::unordered_set<int> &) override;
    virtual bool kill_all_zookeeper(std::unordered_set<int> &) override;
    // start all meta/replica/zookeeper
    virtual bool start_all_meta(std::unordered_set<int> &) override;
    virtual bool start_all_replica(std::unordered_set<int> &) override;
    virtual bool start_all_zookeeper(std::unordered_set<int> &) override;

    virtual bool has_meta_dumped_core(int index) override;
    virtual bool has_replica_dumped_core(int index) override;

private:
    // action = start | stop | restart.
    std::string generate_cmd(int index, const std::string &job, const std::string &action);
    // check whether the command execute success.
    bool check(const std::string &job, int index, const std::string &type);

private:
    // using ${_run_script_path}/run.sh to kill/start
    std::string _run_script_path;
};
}
} // end namespace
