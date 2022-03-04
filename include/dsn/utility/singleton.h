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

#include <boost/noncopyable.hpp>

namespace dsn {
namespace utils {

/**
 * Someone can derived from class `singleton<T>` if he wants to make it's class to be a singleton.
 * And it is strongly recommended that making constuctor and destructor to be private.
 * So that the lifecycle of this singleton instance is maintained by the base class `singleton<T>`
 */

template <typename T>
class singleton : private boost::noncopyable
{
public:
    singleton() = default;

    static T &instance()
    {
        static T _instance;
        return _instance;
    }
};

} // namespace utils
} // namespace dsn
