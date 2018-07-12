/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
