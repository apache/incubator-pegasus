// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cassert>
#include <type_traits>

namespace dsn {

// Downcasting is to convert a base-class pointer(reference) to a derived-class
// pointer(reference). As a usual approach, RTTI (dynamic_cast<>) is not efficient.
// Instead, we can perform a compile-time assertion check whether one is derived
// from another; then, just use static_cast<> to do the conversion faster. RTTI is
// also run in debug mode to do double-check.

template <typename To, typename From>
inline To down_cast(From *from)
{
    // Perform a compile-time assertion to check whether <To> class is derived from <From> class.
    static_assert(std::is_base_of<typename std::remove_pointer<From>::type,
                                  typename std::remove_pointer<To>::type>::value,
                  "<To> class is not derived from <From> class");

    // Use RTTI to do double-check, though in practice the unit tests are seldom built in debug
    // mode. For example, the unit tests of github CI for both rDSN and Pegasus are built in
    // release mode.
    CHECK(from == NULL || dynamic_cast<To>(from) != NULL, "");

    return static_cast<To>(from);
}

} // namespace dsn
