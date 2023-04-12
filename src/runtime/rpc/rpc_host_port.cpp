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
        auto group_address = addr.group_address();
        *this = host_port();
        this->assign_group(group_address->name());
        for (const auto &address : group_address->members()) {
            CHECK_TRUE(this->group_host_port()->add(host_port(address)));
        }
        this->group_host_port()->set_update_leader_automatically(
            group_address->is_update_leader_automatically());
        this->group_host_port()->set_leader(host_port(group_address->leader()));
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
        _group_host_port = other.group_host_port();
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

} // namespace dsn
