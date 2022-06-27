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
include "base.thrift"

namespace cpp dsn.replication
namespace java org.apache.pegasus.replication

struct create_app_options
{
    1:i32              partition_count;
    2:i32              replica_count;
    3:bool             success_if_exist;
    4:string           app_type;
    5:bool             is_stateful;
    6:map<string, string>  envs;
}

// client => meta_server
struct configuration_create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

// meta_server => client
struct configuration_create_app_response
{
    1:base.error_code  err;
    2:i32              appid;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
    2:optional i64     reserve_seconds;
}

struct configuration_drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}

struct configuration_drop_app_response
{
    1:base.error_code   err;
}
