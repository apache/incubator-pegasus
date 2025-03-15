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

#include "rpc/rpc_address.h"
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

// Get host_port from 'obj', the result is filled in 'target', the source is from host_port type
// field 'hp_<field>' if it is set, otherwise, reverse resolve from the rpc_address '<field>'.
#define GET_HOST_PORT(obj, field, target)                                                          \
    do {                                                                                           \
        const auto &_obj = (obj);                                                                  \
        auto &_target = (target);                                                                  \
        if (_obj.__isset.hp_##field) {                                                             \
            DCHECK(_obj.field, "invalid address: {}", _obj.field);                                 \
            DCHECK_EQ(_obj.field, dsn::dns_resolver::instance().resolve_address(_obj.hp_##field)); \
            _target = _obj.hp_##field;                                                             \
        } else {                                                                                   \
            _target = std::move(dsn::host_port::from_address(_obj.field));                         \
        }                                                                                          \
    } while (0)

// Get std::vector<host_port> from 'obj', the result is filled in 'target', the source is from
// std::vector<host_port> type field 'hp_<field>' if it is set, otherwise, reverse resolve from the
// std::vector<rpc_address> '<field>'.
#define GET_HOST_PORTS(obj, field, target)                                                         \
    do {                                                                                           \
        const auto &_obj = (obj);                                                                  \
        auto &_target = (target);                                                                  \
        CHECK(_target.empty(), "");                                                                \
        if (_obj.__isset.hp_##field) {                                                             \
            DCHECK_EQ(_obj.field.size(), _obj.hp_##field.size());                                  \
            _target = _obj.hp_##field;                                                             \
        } else {                                                                                   \
            _target.reserve(_obj.field.size());                                                    \
            for (const auto &addr : _obj.field) {                                                  \
                _target.emplace_back(host_port::from_address(addr));                               \
            }                                                                                      \
        }                                                                                          \
    } while (0)

// Set 'addr' and 'hp' to the '<field>' and optional 'hp_<field>' of 'obj'. The types of the
// fields are rpc_address and host_port, respectively.
#define SET_IP_AND_HOST_PORT(obj, field, addr, hp)                                                 \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_addr = (addr);                                                                \
        const auto &_hp = (hp);                                                                    \
        DCHECK_EQ(_addr, dsn::dns_resolver::instance().resolve_address(_hp));                      \
        _obj.field = _addr;                                                                        \
        _obj.__set_hp_##field(_hp);                                                                \
    } while (0)

// GTest check whether the '<field>' and 'hp_<field>' of 'obj' equal to 'addr' and 'hp'. The types
// of the fields are rpc_address and host_port, respectively.
#define ASSERT_IP_AND_HOST_PORT(obj, field, addr, hp)                                              \
    do {                                                                                           \
        const auto &_obj = (obj);                                                                  \
        const auto &_addr = (addr);                                                                \
        const auto &_hp = (hp);                                                                    \
        ASSERT_EQ(_addr, dsn::dns_resolver::instance().resolve_address(_hp));                      \
        ASSERT_EQ(_addr, _obj.field);                                                              \
        ASSERT_EQ(_hp, _obj.hp_##field);                                                           \
    } while (0)

// Set '<src_field>' and 'hp_<src_field>' of 'src_obj' to the '<dst_field>' and optional
// 'hp_<dst_field>' of 'dst_obj'. The types of the fields are rpc_address and host_port,
// respectively.
#define SET_OBJ_IP_AND_HOST_PORT(dst_obj, dst_field, src_obj, src_field)                           \
    do {                                                                                           \
        const auto &_src_obj = (src_obj);                                                          \
        auto &_dst_obj = (dst_obj);                                                                \
        DCHECK_EQ(_src_obj.src_field,                                                              \
                  dsn::dns_resolver::instance().resolve_address(_src_obj.hp_##src_field));         \
        _dst_obj.dst_field = _src_obj.src_field;                                                   \
        _dst_obj.__set_hp_##dst_field(_src_obj.hp_##src_field);                                    \
    } while (0)

// Set 'hp' and its DNS resolved rpc_address to the optional 'hp_<field>' and '<field>' of 'obj'.
// The types of the fields are host_port and rpc_address, respectively.
#define SET_IP_AND_HOST_PORT_BY_DNS(obj, field, hp)                                                \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp = (hp);                                                                    \
        _obj.field = dsn::dns_resolver::instance().resolve_address(_hp);                           \
        _obj.__set_hp_##field(_hp);                                                                \
    } while (0)

// Reset the '<field>' and optional 'hp_<field>' of 'obj'. The types of the fields are rpc_address
// and host_port, respectively.
#define RESET_IP_AND_HOST_PORT(obj, field)                                                         \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        _obj.field.set_invalid();                                                                  \
        _obj.hp_##field.reset();                                                                   \
    } while (0)

// Clear the '<field>' and optional 'hp_<field>' of 'obj'. The types of the fields are std::vector
// with rpc_address and host_port elements, respectively.
#define CLEAR_IP_AND_HOST_PORT(obj, field)                                                         \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        _obj.field.clear();                                                                        \
        _obj.__set_hp_##field({});                                                                 \
    } while (0)

// Add 'addr' and 'hp' to the vector '<field>' and optional vector 'hp_<field>' of 'obj'. The types
// of the fields are std::vector<rpc_address> and std::vector<host_port>, respectively.
#define ADD_IP_AND_HOST_PORT(obj, field, addr, hp)                                                 \
    do {                                                                                           \
        const auto &_addr = (addr);                                                                \
        const auto &_hp = (hp);                                                                    \
        DCHECK_EQ(_addr, dsn::dns_resolver::instance().resolve_address(_hp));                      \
        auto &_obj = (obj);                                                                        \
        _obj.field.push_back(_addr);                                                               \
        if (!_obj.__isset.hp_##field) {                                                            \
            _obj.__set_hp_##field({_hp});                                                          \
        } else {                                                                                   \
            _obj.hp_##field.push_back(_hp);                                                        \
        }                                                                                          \
    } while (0)

// Add 'hp' and its DNS resolved rpc_address to the optional vector 'hp_<field>' and vector
// '<field>' of 'obj'. The types of the fields are std::vector<rpc_address> and
// std::vector<host_port>, respectively.
#define ADD_IP_AND_HOST_PORT_BY_DNS(obj, field, hp)                                                \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp = (hp);                                                                    \
        _obj.field.push_back(dsn::dns_resolver::instance().resolve_address(_hp));                  \
        if (!_obj.__isset.hp_##field) {                                                            \
            _obj.__set_hp_##field({_hp});                                                          \
        } else {                                                                                   \
            _obj.hp_##field.push_back(_hp);                                                        \
        }                                                                                          \
        DCHECK_EQ(_obj.field.size(), _obj.hp_##field.size());                                      \
    } while (0)

#define SET_IPS_AND_HOST_PORTS_BY_DNS_1(obj, field, hp1)                                           \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp1 = (hp1);                                                                  \
        _obj.field = {dsn::dns_resolver::instance().resolve_address(_hp1)};                        \
        _obj.__set_hp_##field({_hp1});                                                             \
    } while (0)
#define SET_IPS_AND_HOST_PORTS_BY_DNS_2(obj, field, hp1, hp2)                                      \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp1 = (hp1);                                                                  \
        const auto &_hp2 = (hp2);                                                                  \
        _obj.field = {dsn::dns_resolver::instance().resolve_address(_hp1),                         \
                      dsn::dns_resolver::instance().resolve_address(_hp2)};                        \
        _obj.__set_hp_##field({_hp1, _hp2});                                                       \
    } while (0)
#define SET_IPS_AND_HOST_PORTS_BY_DNS_3(obj, field, hp1, hp2, hp3)                                 \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp1 = (hp1);                                                                  \
        const auto &_hp2 = (hp2);                                                                  \
        const auto &_hp3 = (hp3);                                                                  \
        _obj.field = {dsn::dns_resolver::instance().resolve_address(_hp1),                         \
                      dsn::dns_resolver::instance().resolve_address(_hp2),                         \
                      dsn::dns_resolver::instance().resolve_address(_hp3)};                        \
        _obj.__set_hp_##field({_hp1, _hp2, _hp3});                                                 \
    } while (0)
#define SET_IPS_AND_HOST_PORTS_BY_DNS_GET_MACRO(hp1, hp2, hp3, NAME, ...) NAME
#define SET_IPS_AND_HOST_PORTS_BY_DNS_GET_MACRO_(tuple)                                            \
    SET_IPS_AND_HOST_PORTS_BY_DNS_GET_MACRO tuple

// Set ... and their DNS resolved rpc_addresses to the vector '<field>' and optional vector
// 'hp_<field>' of 'obj'. The types of the fields are std::vector<rpc_address> and
// std::vector<host_port>, respectively.
#define SET_IPS_AND_HOST_PORTS_BY_DNS(obj, field, ...)                                             \
    SET_IPS_AND_HOST_PORTS_BY_DNS_GET_MACRO_((__VA_ARGS__,                                         \
                                              SET_IPS_AND_HOST_PORTS_BY_DNS_3,                     \
                                              SET_IPS_AND_HOST_PORTS_BY_DNS_2,                     \
                                              SET_IPS_AND_HOST_PORTS_BY_DNS_1))                    \
    (obj, field, __VA_ARGS__);

// Head insert 'hp' and its DNS resolved rpc_address to the optional vector 'hp_<field>' and vector
// '<field>' of 'obj'. The types of the fields are std::vector<rpc_address> and
// std::vector<host_port>, respectively.
#define HEAD_INSERT_IP_AND_HOST_PORT_BY_DNS(obj, field, hp)                                        \
    do {                                                                                           \
        auto &_obj = (obj);                                                                        \
        const auto &_hp = (hp);                                                                    \
        _obj.field.insert(_obj.field.begin(), dsn::dns_resolver::instance().resolve_address(_hp)); \
        if (!_obj.__isset.hp_##field) {                                                            \
            _obj.__set_hp_##field({_hp});                                                          \
        } else {                                                                                   \
            _obj.hp_##field.insert(_obj.hp_##field.begin(), _hp);                                  \
        }                                                                                          \
        DCHECK_EQ(_obj.field.size(), _obj.hp_##field.size());                                      \
    } while (0)

#define REMOVE_IP_AND_HOST_PORT(to_rm_addr, to_rm_hp, dst_obj, dst_field)                          \
    remove_node(to_rm_addr, to_rm_hp, dst_obj.dst_field, dst_obj.hp_##dst_field)
#define REMOVE_IP_AND_HOST_PORT_BY_OBJ(to_rm_obj, to_rm_field, dst_obj, dst_field)                 \
    remove_node((to_rm_obj).to_rm_field,                                                           \
                (to_rm_obj).hp_##to_rm_field,                                                      \
                dst_obj.dst_field,                                                                 \
                dst_obj.hp_##dst_field)

// TODO(yingchun): the 'hp' can be reduced.
// Set 'value' to the '<field>' map and optional 'hp_<field>' map of 'obj'. The key of the
// maps are rpc_address and host_port type and indexed by 'addr' and 'hp', respectively.
#define SET_VALUE_FROM_IP_AND_HOST_PORT(obj, field, addr, hp, value)                               \
    do {                                                                                           \
        const auto &_hp = (hp);                                                                    \
        const auto &_addr = (addr);                                                                \
        DCHECK_EQ(_addr, dsn::dns_resolver::instance().resolve_address(_hp));                      \
        auto &_obj = (obj);                                                                        \
        const auto &_value = (value);                                                              \
        _obj.field[_addr] = _value;                                                                \
        if (!_obj.__isset.hp_##field) {                                                            \
            _obj.__set_hp_##field({});                                                             \
        }                                                                                          \
        _obj.hp_##field[_hp] = _value;                                                             \
        DCHECK_EQ(_obj.field.size(), _obj.hp_##field.size());                                      \
    } while (0)

// Set 'value' to the '<field>' map and optional 'hp_<field>' map of 'obj'. The key of the
// maps are rpc_address and host_port type and indexed by 'addr' and reverse resolve result of
// 'addr', respectively.
#define SET_VALUE_FROM_HOST_PORT(obj, field, hp, value)                                            \
    do {                                                                                           \
        const auto &__hp = (hp);                                                                   \
        const auto addr = dsn::dns_resolver::instance().resolve_address(__hp);                     \
        SET_VALUE_FROM_IP_AND_HOST_PORT(obj, field, addr, __hp, value);                            \
    } while (0)

#define FMT_HOST_PORT_AND_IP(obj, field) fmt::format("{}({})", (obj).hp_##field, (obj).field)

namespace dsn {

class rpc_group_host_port;

class host_port
{
public:
    explicit host_port() = default;
    explicit host_port(std::string host, uint16_t port);

    host_port(const host_port &other) { *this = other; }
    host_port &operator=(const host_port &other);

    void reset();
    ~host_port() { reset(); }

    dsn_host_type_t type() const { return _type; }
    const std::string &host() const { return _host; }
    uint16_t port() const { return _port; }
    operator bool() const { return _type != HOST_TYPE_INVALID; }

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
    // NOTE: The constructed host_port object maybe invalid, remember to check if it's valid
    // before using it.
    static host_port from_string(const std::string &host_port_str);

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

private:
    friend class dns_resolver;
    friend class rpc_group_host_port;
    FRIEND_TEST(host_port_test, lookup_hostname);
    FRIEND_TEST(host_port_test, transfer_rpc_address);

    static const host_port s_invalid_host_port;

    // Resolve host_port to rpc_addresses.
    // There may be multiple rpc_addresses for one host_port.
    error_s resolve_addresses(std::vector<rpc_address> &addresses) const;

    // Does reverse DNS lookup of the address and stores it in hostname.
    static error_s lookup_hostname(uint32_t ip, std::string *hostname);

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

bool remove_node(const rpc_address &to_rm_addr,
                 const host_port &to_rm_hp,
                 /*inout*/ std::vector<rpc_address> &dst_addrs,
                 /*inout*/ std::vector<host_port> &dst_hps);
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
