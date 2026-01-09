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
#include <unordered_map>

namespace dsn {
class zlock;
} // namespace dsn

namespace pegasus {
class pegasus_client;

namespace client {
class pegasus_client_impl;

/**
 * @brief Implementation of Pegasus client factory
 *
 * This class manages the lifecycle of Pegasus client instances and provides
 * a centralized way to create and access client objects.
 */
class pegasus_client_factory_impl
{
public:
    /**
     * @brief Initialize the client factory with configuration
     * @param config_file Path to the configuration file
     * @return bool True if initialization succeeded, false otherwise
     */
    static bool initialize(const char *config_file);

    /**
     * @brief Get or create a Pegasus client instance
     * @param cluster_name Name of the Pegasus cluster
     * @param app_name Name of the Pegasus table (app)
     * @return pegasus_client* Pointer to the client instance
     * @note The returned client should not be deleted by caller
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
