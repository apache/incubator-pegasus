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

#include <netinet/in.h>
#include <utility>

#include "fmt/core.h"
#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/utils.h"

namespace dsn {

const host_port host_port::s_invalid_host_port;

host_port::host_port(std::string host, uint16_t port)
    : _host(std::move(host)), _port(port), _type(HOST_TYPE_IPV4)
{
    CHECK_NE_MSG(rpc_address::ipv4_from_host(_host.c_str()), 0, "invalid hostname: {}", _host);
}

host_port::host_port(rpc_address addr)
{
    switch (addr.type()) {
    case HOST_TYPE_IPV4: {
        CHECK(utils::hostname_from_ip(htonl(addr.ip()), &_host),
              "invalid address {}",
              addr.ipv4_str());
        _port = addr.port();
    } break;
    case HOST_TYPE_GROUP: {
        _group_host_port = new rpc_group_host_port(addr.group_address());
    } break;
    default:
        break;
    }
    _type = addr.type();
}

void host_port::reset()
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        _host.clear();
        _port = 0;
        break;
    case HOST_TYPE_GROUP:
        group_host_port()->release_ref();
        break;
    default:
        break;
    }
    _type = HOST_TYPE_INVALID;
}

host_port &host_port::operator=(const host_port &other)
{
    if (this == &other) {
        return *this;
    }

    reset();
    switch (other.type()) {
    case HOST_TYPE_IPV4:
        _host = other.host();
        _port = other.port();
        break;
    case HOST_TYPE_GROUP:
        _group_host_port = other._group_host_port;
        group_host_port()->add_ref();
        break;
    default:
        break;
    }
    _type = other.type();
    return *this;
}

std::string host_port::to_string() const
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        return fmt::format("{}:{}", _host, _port);
    case HOST_TYPE_GROUP:
        return fmt::format("address group {}", group_host_port()->name());
    default:
        return "invalid address";
    }
}

void host_port::assign_group(const char *name)
{
    reset();
    _type = HOST_TYPE_GROUP;
    _group_host_port = new rpc_group_host_port(name);
    // take the lifetime of rpc_uri_address, release_ref when change value or call destructor
    _group_host_port->add_ref();
}

error_s host_port::resolve_addresses(std::vector<rpc_address> &addresses) const
{
    CHECK(addresses.empty(), "invalid addresses, not empty");
    if (type() != HOST_TYPE_IPV4) {
        return std::move(error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type"));
    }

    rpc_address rpc_addr;
    if (rpc_addr.from_string_ipv4(this->to_string().c_str())) {
        addresses.emplace_back(rpc_addr);
        return error_s::ok();
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    AddrInfo result;
    RETURN_NOT_OK(GetAddrInfo(_host, hints, &result));

    // DNS may return the same host multiple times. We want to return only the unique
    // addresses, but in the same order as DNS returned them. To do so, we keep track
    // of the already-inserted elements in a set.
    std::unordered_set<rpc_address> inserted;
    std::vector<rpc_address> result_addresses;
    for (const addrinfo *ai = result.get(); ai != nullptr; ai = ai->ai_next) {
        CHECK_EQ(AF_INET, ai->ai_family);
        sockaddr_in *addr = reinterpret_cast<sockaddr_in *>(ai->ai_addr);
        addr->sin_port = htons(_port);
        rpc_address rpc_addr(*addr);
        LOG_INFO("resolved address {} for host_port {}", rpc_addr, to_string());
        if (inserted.insert(rpc_addr).second) {
            result_addresses.emplace_back(rpc_addr);
        }
    }
    addresses = std::move(result_addresses);
    return error_s::ok();
}

} // namespace dsn
