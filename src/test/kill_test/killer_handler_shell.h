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

#include <memory>

#include "killer_handler.h"

namespace pegasus {
namespace test {

class killer_handler_shell : public killer_handler
{
public:
    killer_handler_shell() = default;
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
};
}
} // end namespace
