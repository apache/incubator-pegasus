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

/**
 * @file pegasus_client_factory_impl.h
 * @brief Factory utilities for creating Pegasus C++ client instances.
 *
 * The factory hides the lifecycle management of client instances. All
 * functions are annotated for Doxygen so that the C++ client documentation
 * can be generated with `doxygen`.
 *
 * @addtogroup pegasus_cpp_client
 * @{
 */

#pragma once

#include <string>
#include <unordered_map>

namespace dsn {
class zlock;
} // namespace dsn

namespace pegasus {
class pegasus_client;

namespace client {
class pegasus_client_impl;

/**
 * @brief Implementation of Pegasus client factory.
 *
 * This class manages the lifecycle of Pegasus client instances and provides
 * a centralized way to create and access client objects.
 *
 * Typical usage:
 * @code
 * pegasus::client::pegasus_client_factory::initialize("config.ini");
 * auto *client = pegasus::client::pegasus_client_factory::get_client("cluster", "table");
 * @endcode
 */
class pegasus_client_factory_impl
{
public:
    /**
     * @brief Initialize the client factory with configuration.
     *
     * Call this once before creating any clients when you need to override
     * the default Pegasus configuration path.
     *
     * @param config_file Path to the configuration file.
     * @return bool True if initialization succeeded, false otherwise.
     */
    static bool initialize(const char *config_file);

    /**
     * @brief Get or create a Pegasus client instance.
     *
     * This method caches clients per cluster/app, so repeated calls return
     * the same pointer.
     *
     * @param cluster_name Name of the Pegasus cluster.
     * @param app_name Name of the Pegasus table (app).
     * @return pegasus_client* Pointer to the client instance.
     * @note The returned client is owned by the factory and must not be deleted.
     */
    static pegasus_client *get_client(const char *cluster_name, const char *app_name);

private:
    typedef std::unordered_map<std::string, pegasus_client_impl *> app_to_client_map;
    typedef std::unordered_map<std::string, app_to_client_map> cluster_to_app_map;
    static cluster_to_app_map _cluster_to_clients;
    static ::dsn::zlock *_map_lock;
};
} // namespace client
} // namespace pegasus

/** @} */
