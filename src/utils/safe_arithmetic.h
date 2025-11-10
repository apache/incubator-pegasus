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

#include <limits>
#include <type_traits>

namespace dsn {

// This function performs safe addition of two signed integers `a` and `b`: if it returns true,
// then `a + b` is safe and `result` holds the sum; otherwise, an overflow or underflow has
// occurred.
//
// Directly adding two integers may lead to undefined behavior if an overflow occurs. Therefore,
// in our implementation, we avoid performing the addition directly when checking for potential
// overflow. Also see the following links for more details:
// * https://github.com/apache/incubator-pegasus/issues/2270
// * https://en.cppreference.com/w/cpp/language/ub.html
//
// To ensure that `a + b` does not cause overflow or underflow, the result must satisfy
// `min <= a + b <= max`, which implies `min - b <= a <= max - b`:
// - When `b = 0`, overflow or underflow cannot occur.
// - When `b > 0`, `a` is necessarily greater than `min - b`, so we only need to ensure that
// `a <= max - b`.
// - When `b < 0`, `a` is necessarily less than `max - b`, so we only need to ensure that
// `a >= min - b`.
template <typename TInt>
typename std::enable_if_t<std::conjunction_v<std::is_integral<TInt>, std::is_signed<TInt>>, bool>
safe_add(TInt a, TInt b, TInt &result)
{
    if ((b > 0) && (a > std::numeric_limits<TInt>::max() - b)) {
        return false;
    }

    if ((b < 0) && (a < std::numeric_limits<TInt>::min() - b)) {
        return false;
    }

    result = a + b;
    return true;
}

// This function performs safe addition of two unsigned integers `a` and `b`: if it returns true,
// then `a + b` is safe and `result` holds the sum; otherwise, an overflow has occurred.
//
// Similar to the signed version of safe_add(), the unsigned safe_add() also does not directly
// add the two integers to check for overflow. Since the unsigned integer `b` is always greater
// than or equal to 0, we only need to ensure that `a <= max - b`.
template <typename TInt>
typename std::enable_if_t<std::conjunction_v<std::is_integral<TInt>, std::is_unsigned<TInt>>, bool>
safe_add(TInt a, TInt b, TInt &result)
{
    if (a > std::numeric_limits<TInt>::max() - b) {
        return false;
    }

    result = a + b;
    return true;
}

} // namespace dsn
