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
#include <functional>
#include <iosfwd>
#include <string>

#include "runtime/rpc/rpc_address.h"
#include "utils/fmt_logging.h"

namespace dsn {

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

private:
    std::string _host;
    uint16_t _port = 0;
    dsn_host_type_t _type = HOST_TYPE_INVALID;
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
        CHECK(false, "type HOST_TYPE_GROUP not support!");
    default:
        return true;
    }
}

inline bool operator!=(const host_port &hp1, const host_port &hp2) { return !(hp1 == hp2); }

} // namespace dsn

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
            CHECK(false, "type HOST_TYPE_GROUP not support!");
        default:
            return 0;
        }
    }
};
} // namespace std
