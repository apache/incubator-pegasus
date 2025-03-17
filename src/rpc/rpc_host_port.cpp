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

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <unordered_set>
#include <utility>

#include "fmt/core.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_host_port.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/ports.h"
#include "utils/safe_strerror_posix.h"
#include "utils/string_conv.h"
#include "utils/timer.h"

namespace dsn {

const host_port host_port::s_invalid_host_port;

host_port::host_port(std::string host, uint16_t port)
    : _host(std::move(host)), _port(port), _type(HOST_TYPE_IPV4)
{
    // Solve the problem of not translating "0.0.0.0"
    if (_host != "0.0.0.0") {
        // ipv4_from_host may be slow, just call it in DEBUG version.
        DCHECK_OK(rpc_address::ipv4_from_host(_host, nullptr), "invalid hostname: {}", _host);
    }
}

host_port host_port::from_address(rpc_address addr)
{
    host_port hp;
    SCOPED_LOG_SLOW_EXECUTION(
        WARNING, 100, "construct host_port '{}' from rpc_address '{}'", hp, addr);
    switch (addr.type()) {
    case HOST_TYPE_IPV4: {
        const auto s = lookup_hostname(htonl(addr.ip()), &hp._host);
        if (dsn_unlikely(!s)) {
            LOG_WARNING("lookup_hostname failed for {}: {}", addr.ipv4_str(), s.description());
            return hp;
        }
        hp._port = addr.port();
    } break;
    case HOST_TYPE_GROUP: {
        hp._group_host_port = std::make_shared<rpc_group_host_port>(addr.group_address());
    } break;
    default:
        break;
    }

    // 'hp' become valid now.
    hp._type = addr.type();
    return hp;
}

host_port host_port::from_string(const std::string &host_port_str)
{
    host_port hp;
    SCOPED_LOG_SLOW_EXECUTION(
        WARNING, 100, "construct host_port '{}' from string '{}'", hp, host_port_str);
    const auto pos = host_port_str.find_last_of(':');
    if (dsn_unlikely(pos == std::string::npos)) {
        return hp;
    }

    hp._host = host_port_str.substr(0, pos);
    const auto port_str = host_port_str.substr(pos + 1);
    if (dsn_unlikely(!dsn::buf2uint16(port_str, hp._port))) {
        return hp;
    }

    // Validate the hostname.
    if (dsn_unlikely(!rpc_address::ipv4_from_host(hp._host, nullptr))) {
        return hp;
    }

    // Now is valid.
    hp._type = HOST_TYPE_IPV4;
    return hp;
}

void host_port::reset()
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        _host.clear();
        _port = 0;
        break;
    case HOST_TYPE_GROUP:
        _group_host_port = nullptr;
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
        return fmt::format("host_port group {}", group_host_port()->name());
    default:
        return "invalid host_port";
    }
}

void host_port::assign_group(const char *name)
{
    reset();
    _type = HOST_TYPE_GROUP;
    _group_host_port = std::make_shared<rpc_group_host_port>(name);
}

error_s host_port::resolve_addresses(std::vector<rpc_address> &addresses) const
{
    CHECK(addresses.empty(), "");

    switch (type()) {
    case HOST_TYPE_INVALID:
        return error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type: HOST_TYPE_INVALID");
    case HOST_TYPE_GROUP:
        return error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type: HOST_TYPE_GROUP");
    case HOST_TYPE_IPV4:
        break;
    default:
        CHECK(false, "");
        __builtin_unreachable();
    }

    // 1. Try to resolve hostname in the form of "192.168.0.1:8080".
    uint32_t ip_addr;
    if (inet_pton(AF_INET, this->_host.c_str(), &ip_addr)) {
        const auto rpc_addr = rpc_address::from_ip_port(this->to_string());
        if (rpc_addr) {
            addresses.emplace_back(rpc_addr);
            return error_s::ok();
        }
    }

    // 2. Try to resolve hostname in the form of "host1:80".
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    AddrInfo result;
    RETURN_NOT_OK(rpc_address::GetAddrInfo(_host, hints, &result));

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

    if (result_addresses.empty()) {
        return error_s::make(dsn::ERR_NETWORK_FAILURE,
                             fmt::format("can not resolve host_port {}.", to_string()));
    }

    addresses = std::move(result_addresses);
    return error_s::ok();
}

error_s host_port::lookup_hostname(uint32_t ip, std::string *hostname)
{
    struct sockaddr_in addr_in;
    addr_in.sin_family = AF_INET;
    addr_in.sin_port = 0;
    addr_in.sin_addr.s_addr = ip;
    char host[NI_MAXHOST];
    int rc = ::getnameinfo((struct sockaddr *)(&addr_in),
                           sizeof(struct sockaddr),
                           host,
                           sizeof(host),
                           nullptr,
                           0,
                           NI_NAMEREQD);
    if (dsn_unlikely(rc != 0)) {
        if (rc == EAI_SYSTEM) {
            return error_s::make(dsn::ERR_NETWORK_FAILURE,
                                 fmt::format("{}: {}: getnameinfo failed",
                                             gai_strerror(rc),
                                             dsn::utils::safe_strerror(errno)));
        }

        return error_s::make(dsn::ERR_NETWORK_FAILURE,
                             fmt::format("{}: getnameinfo failed", gai_strerror(rc)));
    }

    *hostname = host;
    return error_s::ok();
}

bool remove_node(const rpc_address &to_rm_addr,
                 const host_port &to_rm_hp,
                 /*inout*/ std::vector<rpc_address> &dst_addrs,
                 /*inout*/ std::vector<host_port> &dst_hps)
{
    const auto it_addr = std::find(dst_addrs.begin(), dst_addrs.end(), to_rm_addr);
    const auto it_hp = std::find(dst_hps.begin(), dst_hps.end(), to_rm_hp);
    bool found_addr = (it_addr != dst_addrs.end());
    bool found_hp = (it_hp != dst_hps.end());
    DCHECK_EQ(found_addr, found_hp);
    if (found_addr) {
        dst_addrs.erase(it_addr);
        dst_hps.erase(it_hp);
    }
    return found_addr;
}

} // namespace dsn
