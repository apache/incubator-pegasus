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

#include <cstddef>
#include <cstdint>
// IWYU pragma: no_include <experimental/string_view>
#include <functional>
#include <iosfwd>
#include <string>
#include <string_view>
#include <vector>

#include "runtime/rpc/rpc_address.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/fmt_utils.h"

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace apache

namespace dsn {

class rpc_group_host_port;

class host_port
{
public:
    static const host_port s_invalid_host_port;
    explicit host_port() = default;
    explicit host_port(std::string host, uint16_t port);
    explicit host_port(rpc_address addr);

    host_port(const host_port &other) { *this = other; }
    host_port &operator=(const host_port &other);

    void reset();
    ~host_port() { reset(); }

    dsn_host_type_t type() const { return _type; }
    const std::string &host() const { return _host; }
    uint16_t port() const { return _port; }

    bool is_invalid() const { return _type == HOST_TYPE_INVALID; }

    std::string to_string() const;

    friend std::ostream &operator<<(std::ostream &os, const host_port &hp)
    {
        return os << hp.to_string();
    }

    rpc_group_host_port *group_host_port() const
    {
        CHECK_NOTNULL(_group_host_port, "group_host_port cannot be null!");
        return _group_host_port;
    }
    void assign_group(const char *name);

    // Resolve host_port to rpc_addresses.
    // Trere may be multiple rpc_addresses for one host_port.
    error_s resolve_addresses(std::vector<rpc_address> &addresses) const;

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

private:
    std::string _host;
    uint16_t _port = 0;
    dsn_host_type_t _type = HOST_TYPE_INVALID;
    rpc_group_host_port *_group_host_port = nullptr;
};

inline bool operator==(const host_port &hp1, const host_port &hp2)
{
    if (&hp1 == &hp2) {
        return true;
    }

    if (hp1.type() != hp2.type()) {
        return false;
    }

    switch (hp1.type()) {
    case HOST_TYPE_IPV4:
        return hp1.host() == hp2.host() && hp1.port() == hp2.port();
    case HOST_TYPE_GROUP:
        return hp1.group_host_port() == hp2.group_host_port();
    default:
        return true;
    }
}

inline bool operator!=(const host_port &hp1, const host_port &hp2) { return !(hp1 == hp2); }

} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::host_port);

namespace std {
template <>
struct hash<::dsn::host_port>
{
    size_t operator()(const ::dsn::host_port &hp) const
    {
        switch (hp.type()) {
        case HOST_TYPE_IPV4:
            return std::hash<std::string>()(hp.host()) ^ std::hash<uint16_t>()(hp.port());
        case HOST_TYPE_GROUP:
            return std::hash<void *>()(hp.group_host_port());
        default:
            return 0;
        }
    }
};
} // namespace std
