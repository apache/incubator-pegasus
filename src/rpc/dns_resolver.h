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
#include <vector>

#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "utils/errors.h"
#include "utils/metrics.h"
#include "utils/singleton.h"
#include "utils/synchronize.h"

namespace dsn {

// This class provide a way to resolve host_port to rpc_address.
// Now each host_post will be resolved just once, and then cached the first rpc_address result in
// the resolved result list.
// If some host_port's rpc_address changes, you need to restart the Pegasus process to make it take
// effect.
// TODO(yingchun): Now the cache is unlimited, the cache size may be huge. Implement an expiration
// mechanism to limit the cache size and make it possible to update the resolve result.
class dns_resolver : public utils::singleton<dns_resolver>
{
public:
    // Resolve this host_port to an unique rpc_address.
    rpc_address resolve_address(const host_port &hp);

    // Resolve comma separated host:port list 'host_ports' to comma separated ip:port list.
    static std::string ip_ports_from_host_ports(const std::string &host_ports);

private:
    dns_resolver();
    ~dns_resolver() = default;

    friend class utils::singleton<dns_resolver>;

    bool get_cached_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    error_s resolve_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    mutable utils::rw_lock_nr _lock;
    // Cache the host_port resolve results, the cached rpc_address is the first one in the resolved
    // list.
    std::unordered_map<host_port, rpc_address> _dns_cache;

    METRIC_VAR_DECLARE_gauge_int64(dns_resolver_cache_size);
    METRIC_VAR_DECLARE_percentile_int64(dns_resolver_resolve_duration_ns);
    METRIC_VAR_DECLARE_percentile_int64(dns_resolver_resolve_by_dns_duration_ns);
};

} // namespace dsn
