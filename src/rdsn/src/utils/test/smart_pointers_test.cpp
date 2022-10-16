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

#include "utils/smart_pointers.h"

#include <gtest/gtest.h>

using namespace dsn;

TEST(MakeUniqueTest, Basic)
{
    std::unique_ptr<std::string> p = make_unique<std::string>();
    EXPECT_EQ("", *p);
    p = make_unique<std::string>("hi");
    EXPECT_EQ("hi", *p);
}

struct MoveOnly
{
    MoveOnly() = default;
    explicit MoveOnly(int i1) : ip1{new int{i1}} {}
    MoveOnly(int i1, int i2) : ip1{new int{i1}}, ip2{new int{i2}} {}
    std::unique_ptr<int> ip1;
    std::unique_ptr<int> ip2;
};

struct AcceptMoveOnly
{
    explicit AcceptMoveOnly(MoveOnly m) : m_(std::move(m)) {}
    MoveOnly m_;
};

TEST(MakeUniqueTest, MoveOnlyTypeAndValue)
{
    using ExpectedType = std::unique_ptr<MoveOnly>;
    {
        auto p = make_unique<MoveOnly>();
        static_assert(std::is_same<decltype(p), ExpectedType>::value, "unexpected return type");
        EXPECT_TRUE(!p->ip1);
        EXPECT_TRUE(!p->ip2);
    }
    {
        auto p = make_unique<MoveOnly>(1);
        static_assert(std::is_same<decltype(p), ExpectedType>::value, "unexpected return type");
        EXPECT_TRUE(p->ip1 && *p->ip1 == 1);
        EXPECT_TRUE(!p->ip2);
    }
    {
        auto p = make_unique<MoveOnly>(1, 2);
        static_assert(std::is_same<decltype(p), ExpectedType>::value, "unexpected return type");
        EXPECT_TRUE(p->ip1 && *p->ip1 == 1);
        EXPECT_TRUE(p->ip2 && *p->ip2 == 2);
    }
}

TEST(MakeUniqueTest, AcceptMoveOnly)
{
    auto p = make_unique<AcceptMoveOnly>(MoveOnly());
    p = std::unique_ptr<AcceptMoveOnly>(new AcceptMoveOnly(MoveOnly()));
}

struct ArrayWatch
{
    void *operator new[](size_t n)
    {
        allocs().push_back(n);
        return ::operator new[](n);
    }
    void operator delete[](void *p) { return ::operator delete[](p); }
    static std::vector<size_t> &allocs()
    {
        static auto &v = *new std::vector<size_t>;
        return v;
    }
};

TEST(Make_UniqueTest, Array)
{
    // Ensure state is clean before we start so that these tests
    // are order-agnostic.
    ArrayWatch::allocs().clear();

    auto p = make_unique<ArrayWatch[]>(5);
    static_assert(std::is_same<decltype(p), std::unique_ptr<ArrayWatch[]>>::value,
                  "unexpected return type");

    // TODO(wutao1): fix this. EXPECT_THAT is not available since it's an gmock macro,
    // but we do not depend on gmock.
    // EXPECT_THAT(ArrayWatch::allocs(), ElementsAre(5 * sizeof(ArrayWatch)));
}

TEST(Make_UniqueTest, NotAmbiguousWithStdMakeUnique)
{
    // Ensure that make_unique is not ambiguous with std::make_unique.
    // In C++14 mode, the below call to make_unique has both types as candidates.
    struct TakesStdType
    {
        explicit TakesStdType(const std::vector<int> &vec) {}
    };
    make_unique<TakesStdType>(std::vector<int>());
}
