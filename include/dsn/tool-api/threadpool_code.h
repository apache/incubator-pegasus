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

#include <dsn/utility/ports.h>

namespace dsn {
class threadpool_code
{
public:
    threadpool_code() { _internal_code = 0; }
    explicit threadpool_code(int c) : _internal_code(c) {}
    threadpool_code(const threadpool_code &r) { _internal_code = r._internal_code; }
    explicit threadpool_code(const char *name);
    const char *to_string() const;
    threadpool_code &operator=(const threadpool_code &source)
    {
        _internal_code = source._internal_code;
        return *this;
    }
    bool operator==(const threadpool_code &r) { return _internal_code == r._internal_code; }
    bool operator!=(const threadpool_code &r) { return !(*this == r); }
    operator int() const { return _internal_code; }

    static int max();
    static bool is_exist(const char *name);

private:
    int _internal_code;
};

/*! define a new thread pool named x*/
#define DEFINE_THREAD_POOL_CODE(x) __selectany const ::dsn::threadpool_code x(#x);

DEFINE_THREAD_POOL_CODE(THREAD_POOL_INVALID)
DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEFAULT)
}
