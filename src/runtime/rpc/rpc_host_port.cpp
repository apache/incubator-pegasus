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

#include "runtime/rpc/rpc_host_port.h"
#include "utils/safe_strerror_posix.h"
#include "utils/utils.h"

#include <unordered_set>

namespace dsn {

const host_port host_port::s_invalid_host_port;

host_port::host_port(std::string host, uint16_t port)
    : _host(std::move(host)), _port(port), _type(HOST_TYPE_IPV4)
{
    uint32_t ip = rpc_address::ipv4_from_host(_host.c_str());
    CHECK_NE_MSG(ip, 0, "invalid hostname: {}", _host);
}

host_port::host_port(rpc_address addr)
{
    switch (addr.type()) {
    case HOST_TYPE_IPV4: {
        std::string hostname;
        CHECK(!utils::hostname_from_ip(addr.ipv4_str(), &hostname),
              "invalid address {}",
              addr.ipv4_str());
        *this = host_port(std::move(hostname), addr.port());
    } break;
    // TODO(liguohao): Perfect it when group_host_port implemented
    case HOST_TYPE_GROUP: {
        auto group_address = addr.group_address();
        *this = host_port();
        this->assign_group(group_address->name());
    } break;
    default:
        break;
    }
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
    if (this != &other) {
        reset();
        _type = other.type();
        switch (type()) {
        case HOST_TYPE_IPV4:
            _host = other.host();
            _port = other.port();
            break;
        case HOST_TYPE_GROUP:
            _group_host_port = other.group_host_port();
            group_host_port()->add_ref();
            break;
        default:
            break;
        }
    }
    return *this;
}

void host_port::assign_group(const char *name)
{
    reset();
    _type = HOST_TYPE_GROUP;
    _group_host_port = new rpc_group_host_port(name);
    _group_host_port->add_ref();
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
} // namespace dsn
