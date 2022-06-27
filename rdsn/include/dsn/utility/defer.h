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

#include <utility>

namespace dsn {

// `defer` is an useful util to implement RAII in golang, much alike
// `try...finally...` in java. In C++ we used to implement an RAII class
// wrapping around the resource:
//
// ```cpp
// struct object_raii
// {
//   object_raii() {
//     _obj = c_object_new();
//   }
//   ~object_raii() {
//     c_object_free(_obj);
//   }
// private:
//   c_object *_obj;
// };
// ```
//
// Now with `defer`, things will be simplified:
//
// ```cpp
// c_object *obj = c_object_new();
// auto cleanup = dsn::defer([obj]() { c_object_free(obj); });
// ```

template <typename Func>
struct deferred_action
{
    explicit deferred_action(Func &&func) noexcept : _func(std::move(func)) {}
    ~deferred_action() { _func(); }
private:
    Func _func;
};

template <typename Func>
inline deferred_action<Func> defer(Func &&func)
{
    return deferred_action<Func>(std::forward<Func>(func));
}

} // namespace dsn
