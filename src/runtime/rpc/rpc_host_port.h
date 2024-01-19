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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest_prod.h>

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

#define GET_HOST_PORT(obj, field, target)                                                          \
    do {                                                                                           \
        const auto &_obj = (obj);                                                                  \
        if (_obj.__isset.hp_##field) {                                                             \
            target = _obj.hp_##field;                                                              \
        } else {                                                                                   \
            target = std::move(dsn::host_port::from_address(_obj.field));                          \
        }                                                                                          \
    } while (0)

namespace dsn {

class rpc_group_host_port;

class host_port
{
public:
    static const host_port s_invalid_host_port;
    explicit host_port() = default;
    explicit host_port(std::string host, uint16_t port);

    host_port(const host_port &other) { *this = other; }
    host_port &operator=(const host_port &other);

    void reset();
    ~host_port() { reset(); }

    dsn_host_type_t type() const { return _type; }
    const std::string &host() const { return _host; }
    uint16_t port() const { return _port; }

    [[nodiscard]] bool is_invalid() const { return _type == HOST_TYPE_INVALID; }

    operator bool() const { return !is_invalid(); }

    std::string to_string() const;

    friend std::ostream &operator<<(std::ostream &os, const host_port &hp)
    {
        return os << hp.to_string();
    }

    std::shared_ptr<rpc_group_host_port> group_host_port() const
    {
        CHECK_NOTNULL(_group_host_port, "group_host_port cannot be null!");
        return _group_host_port;
    }
    void assign_group(const char *name);

    // Construct a host_port object from 'addr'
    static host_port from_address(rpc_address addr);

    // Construct a host_port object from 'host_port_str', the latter is in the format of
    // "localhost:8888".
    // NOTE: The constructed host_port object maybe invalid, remember to check it by is_invalid()
    // before using it.
    static host_port from_string(const std::string &host_port_str);

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    static void fill_host_ports_from_addresses(const std::vector<rpc_address> &addr_v,
                                               /*output*/ std::vector<host_port> &hp_v);

private:
    friend class dns_resolver;
    FRIEND_TEST(host_port_test, transfer_rpc_address);

    // Resolve host_port to rpc_addresses.
    // There may be multiple rpc_addresses for one host_port.
    error_s resolve_addresses(std::vector<rpc_address> &addresses) const;

    std::string _host;
    uint16_t _port = 0;
    dsn_host_type_t _type = HOST_TYPE_INVALID;
    std::shared_ptr<rpc_group_host_port> _group_host_port;
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

inline bool operator<(const host_port &hp1, const host_port &hp2)
{
    if (hp1.type() != hp2.type()) {
        return hp1.type() < hp2.type();
    }

    switch (hp1.type()) {
    case HOST_TYPE_IPV4:
        return hp1.host() < hp2.host() || (hp1.host() == hp2.host() && hp1.port() < hp2.port());
    case HOST_TYPE_GROUP:
        return hp1.group_host_port().get() < hp2.group_host_port().get();
    default:
        return true;
    }
}
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
            return std::hash<void *>()(hp.group_host_port().get());
        default:
            return 0;
        }
    }
};
} // namespace std
