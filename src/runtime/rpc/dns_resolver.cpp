/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "runtime/rpc/dns_resolver.h"

namespace dsn {

void dns_resolver::add_item(const host_port &hp, const rpc_address &addr)
{
    utils::auto_write_lock l(_lock);
    _dsn_cache.insert(std::make_pair(hp, addr));
}

bool dns_resolver::get_cached_addresses(const host_port &hp, std::vector<rpc_address> addresses)
{
    utils::auto_read_lock l(_lock);
    const auto &found = _dsn_cache.find(hp);
    if (found == _dsn_cache.end()) {
        return false;
    }

    addresses = {found->second};
    return true;
}

error_s dns_resolver::resolve_addresses(const host_port &hp, std::vector<rpc_address> addresses)
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
            LOG_WARNING("host_port '{}' resolves to {} different addresses, using {}.",
                        hp,
                        resolved_addresses.size(),
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
            LOG_WARNING("host_port '{}' resolves to {} different addresses, using {}",
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
