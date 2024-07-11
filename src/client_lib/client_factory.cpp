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

#include <pegasus/client.h>
#include "pegasus_client_factory_impl.h"

namespace pegasus {

bool pegasus_client_factory::initialize(const char *config_file)
{
    return client::pegasus_client_factory_impl::initialize(config_file);
}

pegasus_client *pegasus_client_factory::get_client(const char *cluster_name, const char *app_name)
{
    return client::pegasus_client_factory_impl::get_client(cluster_name, app_name);
}

} // namespace pegasus
