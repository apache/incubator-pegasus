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

#include <string>
#include <unordered_set>

#include <dsn/utility/factory_store.h>

namespace pegasus {
namespace test {

// define the interface that how to upgrade and downgrade the jobs [meta, replica, zookeeper].
class upgrader_handler
{
public:
    template <typename T>
    static void register_factory(const char *name)
    {
        dsn::utils::factory_store<upgrader_handler>::register_factory(
            name, create<T>, dsn::PROVIDER_TYPE_MAIN);
    }
    static upgrader_handler *new_handler(const char *name)
    {
        return dsn::utils::factory_store<upgrader_handler>::create(name, dsn::PROVIDER_TYPE_MAIN);
    }

public:
    virtual ~upgrader_handler() {}
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

private:
    template <typename T>
    static upgrader_handler *create()
    {
        return new T();
    }
};
}
} // end namespace
