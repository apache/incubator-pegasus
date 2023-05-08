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

#include <unordered_map>
#include <vector>

#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/errors.h"
#include "utils/synchronize.h"

namespace dsn {

// This class provide a way to resolve host_port to rpc_address.
class dns_resolver
{
public:
    explicit dns_resolver() = default;

    void add_item(const host_port &hp, const rpc_address &addr);

    // Resolve this host_port to an unique rpc_address.
    rpc_address resolve_address(const host_port &hp);

private:
    bool get_cached_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    error_s resolve_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    error_s do_resolution(const host_port &hp, std::vector<rpc_address> &addresses);

    mutable utils::rw_lock_nr _lock;
    std::unordered_map<host_port, rpc_address> _dsn_cache;
};

} // namespace dsn
