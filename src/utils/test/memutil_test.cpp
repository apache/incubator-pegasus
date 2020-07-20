// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Unit test for memutil.cc

#include "utils/memutil.h"

#include <gtest/gtest.h>

TEST(MemUtilTest, memmatch)
{
    const char kHaystack[] = "0123456789";
    EXPECT_EQ(dsn::strings_internal::memmatch(kHaystack, 0, "", 0), kHaystack);
    EXPECT_EQ(dsn::strings_internal::memmatch(kHaystack, 10, "012", 3), kHaystack);
    EXPECT_EQ(dsn::strings_internal::memmatch(kHaystack, 10, "0xx", 1), kHaystack);
    EXPECT_EQ(dsn::strings_internal::memmatch(kHaystack, 10, "789", 3), kHaystack + 7);
    EXPECT_EQ(dsn::strings_internal::memmatch(kHaystack, 10, "9xx", 1), kHaystack + 9);
    EXPECT_TRUE(dsn::strings_internal::memmatch(kHaystack, 10, "9xx", 3) == nullptr);
    EXPECT_TRUE(dsn::strings_internal::memmatch(kHaystack, 10, "xxx", 1) == nullptr);
}
