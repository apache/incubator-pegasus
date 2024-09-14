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

#include <algorithm>
#include <memory>
#include <set>
#include <string_view>
#include <utility>

#include "fmt/core.h"
#include "fmt/format.h"
#include "rpc/dns_resolver.h"
#include "rpc/group_address.h"
#include "rpc/group_host_port.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/strings.h"

METRIC_DEFINE_gauge_int64(server,
                          dns_resolver_cache_size,
                          dsn::metric_unit::kKeys,
                          "The size of the host_port to rpc_address resolve results cache");

METRIC_DEFINE_percentile_int64(
    server,
    dns_resolver_resolve_duration_ns,
    dsn::metric_unit::kNanoSeconds,
    "The duration of resolving a host port, may either get from cache or resolve by DNS lookup");

METRIC_DEFINE_percentile_int64(server,
                               dns_resolver_resolve_by_dns_duration_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The duration of resolving a host port by DNS lookup");
namespace dsn {

dns_resolver::dns_resolver()
    : METRIC_VAR_INIT_server(dns_resolver_cache_size),
      METRIC_VAR_INIT_server(dns_resolver_resolve_duration_ns),
      METRIC_VAR_INIT_server(dns_resolver_resolve_by_dns_duration_ns)
{
#ifndef MOCK_TEST
    static int only_one_instance = 0;
    only_one_instance++;
    CHECK_EQ_MSG(1, only_one_instance, "dns_resolver should only created once!");
#endif
}

bool dns_resolver::get_cached_addresses(const host_port &hp, std::vector<rpc_address> &addresses)
{
    utils::auto_read_lock l(_lock);
    const auto &found = _dns_cache.find(hp);
    if (found == _dns_cache.end()) {
        return false;
    }

    addresses = {found->second};
    return true;
}

error_s dns_resolver::resolve_addresses(const host_port &hp, std::vector<rpc_address> &addresses)
{
    CHECK(addresses.empty(), "invalid addresses, not empty");
    if (get_cached_addresses(hp, addresses)) {
        return error_s::ok();
    }

    std::vector<rpc_address> resolved_addresses;
    {
        METRIC_VAR_AUTO_LATENCY(dns_resolver_resolve_by_dns_duration_ns);
        RETURN_NOT_OK(hp.resolve_addresses(resolved_addresses));
    }

    {
        if (resolved_addresses.size() > 1) {
            LOG_DEBUG("host_port '{}' resolves to {} different addresses {}, only the first one {} "
                      "will be cached.",
                      hp,
                      resolved_addresses.size(),
                      fmt::join(resolved_addresses, ","),
                      resolved_addresses[0]);
        }

        utils::auto_write_lock l(_lock);
        const auto it = _dns_cache.insert(std::make_pair(hp, resolved_addresses[0]));
        if (it.second) {
            METRIC_VAR_INCREMENT(dns_resolver_cache_size);
        }
    }

    addresses = std::move(resolved_addresses);
    return error_s::ok();
}

rpc_address dns_resolver::resolve_address(const host_port &hp)
{
    METRIC_VAR_AUTO_LATENCY(dns_resolver_resolve_duration_ns);
    switch (hp.type()) {
    case HOST_TYPE_GROUP: {
        rpc_address addr;
        auto hp_group = hp.group_host_port();
        addr.assign_group(hp_group->name());

        for (const auto &hp : hp_group->members()) {
            CHECK_TRUE(addr.group_address()->add(resolve_address(hp)));
        }
        addr.group_address()->set_update_leader_automatically(
            hp_group->is_update_leader_automatically());
        addr.group_address()->set_leader(resolve_address(hp_group->leader()));
        return addr;
    }
    case HOST_TYPE_IPV4: {
        std::vector<rpc_address> addresses;
        CHECK_OK(resolve_addresses(hp, addresses), "host_port '{}' can not be resolved", hp);
        CHECK(!addresses.empty(), "host_port '{}' can not be resolved to any address", hp);

        if (addresses.size() > 1) {
            LOG_WARNING("host_port '{}' resolves to {} different addresses, using the first one {}",
                        hp,
                        addresses.size(),
                        addresses[0]);
        }
        return addresses[0];
    }
    default:
        return {};
    }
}

std::string dns_resolver::ip_ports_from_host_ports(const std::string &host_ports)
{
    std::vector<std::string> host_port_vec;
    dsn::utils::split_args(host_ports.c_str(), host_port_vec, ',');

    if (dsn_unlikely(host_port_vec.empty())) {
        return host_ports;
    }

    std::vector<std::string> ip_port_vec;
    ip_port_vec.reserve(host_port_vec.size());
    for (const auto &hp : host_port_vec) {
        const auto addr = dsn::dns_resolver::instance().resolve_address(host_port::from_string(hp));
        ip_port_vec.emplace_back(addr.to_string());
    }

    return fmt::format("{}", fmt::join(ip_port_vec, ","));
}

} // namespace dsn
