// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/ports.h>

namespace dsn {
namespace utils {

template <typename T>
class preloadable
{
protected:
    preloadable() {}
    DISALLOW_COPY_AND_ASSIGN(preloadable);

public:
    static T s_instance;
};

template <typename T>
T preloadable<T>::s_instance;
}
}
