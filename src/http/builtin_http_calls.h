// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

namespace dsn {
struct http_request;
struct http_response;

// Register basic services for the HTTP server.
extern void register_builtin_http_calls();

extern void get_perf_counter_handler(const http_request &req, http_response &resp);

extern void get_help_handler(const http_request &req, http_response &resp);

// Get <meta_server_ipport>/version
// Request body:
// {
//    Version: "2.1.SNAPSHOT",
//    GitCommit: "88783e1ec28c326974f808d91c1531391d38acb5"
// }
extern void get_version_handler(const http_request &req, http_response &resp);

extern void get_recent_start_time_handler(const http_request &req, http_response &resp);

extern void list_all_configs(const http_request &req, http_response &resp);

extern void get_config(const http_request &req, http_response &resp);
} // namespace dsn
