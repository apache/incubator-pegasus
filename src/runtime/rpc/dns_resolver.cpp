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
#include <utility>

#include "fmt/format.h"
#include "runtime/rpc/dns_resolver.h"
#include "runtime/rpc/group_address.h"
#include "runtime/rpc/group_host_port.h"
#include "utils/fmt_logging.h"

namespace dsn {

void dns_resolver::add_item(const host_port &hp, const rpc_address &addr)
{
    utils::auto_write_lock l(_lock);
    _dsn_cache.insert(std::make_pair(hp, addr));
}

bool dns_resolver::get_cached_addresses(const host_port &hp, std::vector<rpc_address> &addresses)
{
    utils::auto_read_lock l(_lock);
    const auto &found = _dsn_cache.find(hp);
    if (found == _dsn_cache.end()) {
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
    RETURN_NOT_OK(hp.resolve_addresses(resolved_addresses));

    {
        utils::auto_write_lock l(_lock);
        if (resolved_addresses.size() > 1) {
            LOG_DEBUG(
                "host_port '{}' resolves to {} different addresses {}, using the first one {}.",
                hp,
                resolved_addresses.size(),
                fmt::join(resolved_addresses, ","),
                resolved_addresses[0]);
        }
        _dsn_cache.insert(std::make_pair(hp, resolved_addresses[0]));
    }

    addresses = std::move(resolved_addresses);
    return error_s::ok();
}

rpc_address dns_resolver::resolve_address(const host_port &hp)
{
    switch (hp.type()) {
    case HOST_TYPE_GROUP: {
        rpc_address addr;
        auto group_address = hp.group_host_port();
        addr.assign_group(group_address->name());

        for (const auto &hp : group_address->members()) {
            CHECK_TRUE(addr.group_address()->add(resolve_address(hp)));
        }
        addr.group_address()->set_update_leader_automatically(
            group_address->is_update_leader_automatically());
        addr.group_address()->set_leader(resolve_address(group_address->leader()));
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
        return rpc_address();
    }
}

} // namespace dsn
