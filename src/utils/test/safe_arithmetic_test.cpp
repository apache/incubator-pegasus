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

#include "gtest/gtest.h"
#include "utils/safe_arithmetic.h"

namespace dsn {

template <typename TInt,
              std::enable_if_t<std::is_integral<TInt>,
                               int> = 0>
struct safe_int_add_case
{
    TInt a;
    TInt b;
    TInt expected_result;
    bool expected_safe;
};

template <typename TInt,
              std::enable_if_t<std::is_integral<TInt>,
                               int> = 0>
class SafeIntAddTest : public testing::TestWithParam<safe_int_add_case<TInt>>
{
protected:
    void test_safe_add() const {
        const auto &test_case = GetParam();

        TInt actual_result{0};
        ASSERT_EQ(test_case.expected_safe, safe_add(test_case.a, test_case.b, actual_result));

        if (!test_case.expected_safe) {
            return;
        }

        EXPECT_EQ(test_case.expected_result, actual_result);
    }
};

class SafeSignedInt64AddTest : public SafeIntAddTest<int64_t>
{

};

TEST_P(SafeSignedInt64AddTest, SafeAdd) {
    test_safe_add();
}

const std::vector<safe_int_add_case<int64_t>> safe_signed_int64_add_tests = {
    // Common cases: both a and b are non-zero.
    {271828, 314159, 585987, true},
    {-271828, 314159, 42331, true},
    {271828, -314159, -42331, true},
    {-271828, -314159, -585987, true},

    // Common cases: either a or b is zero.
    {271828, 0, 271828, true},
    {0, 271828, 271828, true},
    {-314159, 0, -314159, true},
    {0, -314159, -314159, true},

    // Common case: both a and b are zero.
    {0, 0, 0, true},
    
    // Common cases: the result is zero.
    {271828, -271828, 0, true},
    {-271828, 271828, 0, true},
    
    // Eage cases: both a and b are non-zero and the result is max.
    {std::numeric_limits<int64_t>::max() / 2, (std::numeric_limits<int64_t>::max()-1)/2+1, std::numeric_limits<int64_t>::max(), true},
    {(std::numeric_limits<int64_t>::max()-1)/2+1, std::numeric_limits<int64_t>::max()/2, std::numeric_limits<int64_t>::max(),true},

    // Eage cases: both a and b are non-zero and the result is min.
    {std::numeric_limits<int64_t>::min() / 2, (std::numeric_limits<int64_t>::min()+1)/2-1, std::numeric_limits<int64_t>::min(), true},
    {(std::numeric_limits<int64_t>::min()+1)/2-1, std::numeric_limits<int64_t>::min()/2, std::numeric_limits<int64_t>::min(),true},

    // Eage cases: either a or b is zero and the result is max.
    {0, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max(), true},
    {std::numeric_limits<int64_t>::max(), 0, std::numeric_limits<int64_t>::max(), true},

    // Eage cases: either a or b is zero and the result is min.
    {0, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min(), true},
    {std::numeric_limits<int64_t>::min(), 0, std::numeric_limits<int64_t>::min(), true},

    // Eage cases: the result overflows.
    {1, std::numeric_limits<int64_t>::max(), 0, false},
    {std::numeric_limits<int64_t>::max(), 1, 0, false},
    {10, std::numeric_limits<int64_t>::max(), 0, false},
    {std::numeric_limits<int64_t>::max(), 10, 0, false},
    {std::numeric_limits<int64_t>::max() / 2 + 1, (std::numeric_limits<int64_t>::max()-1)/2+1, 0, false},
    {(std::numeric_limits<int64_t>::max()-1)/2+1, 0, std::numeric_limits<int64_t>::max()/2+1,0, false},

    // Eage cases: the result underflows.
    {-1, std::numeric_limits<int64_t>::min(), 0, false},
    {std::numeric_limits<int64_t>::min(), -1, 0, false},
    {-10, std::numeric_limits<int64_t>::min(), 0, false},
    {std::numeric_limits<int64_t>::min(), -10, 0, false},
    {std::numeric_limits<int64_t>::min() / 2 - 1, (std::numeric_limits<int64_t>::min()+1)/2-1, 0, false},
    {(std::numeric_limits<int64_t>::min()+1)/2-1, 0, std::numeric_limits<int64_t>::min()/2-1,0, false},
};

INSTANTIATE_TEST_SUITE_P(SafeArithmeticTest, SafeSignedInt64AddTest, testing::ValuesIn(safe_signed_int64_add_tests));

class SafeUnsignedInt64AddTest : public SafeIntAddTest<uint64_t>
{

};

TEST_P(SafeUnsignedInt64AddTest, SafeAdd) {
    test_safe_add();
}

const std::vector<safe_int_add_case<uint64_t>> safe_unsigned_int64_add_tests = {
    // Common cases: both a and b are non-zero.
    {271828, 314159, 585987, true},
    {314159, 271828, 585987, true},

    // Common cases: either a or b is zero.
    {271828, 0, 271828, true},
    {0, 314159, 314159, true},

    // Common case: both a and b are zero.
    {0, 0, 0, true},
    
    // Eage cases: both a and b are non-zero and the result is max.
    {std::numeric_limits<uint64_t>::max() / 2, (std::numeric_limits<uint64_t>::max()-1)/2+1, std::numeric_limits<uint64_t>::max(), true},
    {(std::numeric_limits<uint64_t>::max()-1)/2+1, std::numeric_limits<uint64_t>::max()/2, std::numeric_limits<uint64_t>::max(),true},

    // Eage cases: either a or b is zero and the result is max.
    {0, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(), true},
    {std::numeric_limits<uint64_t>::max(), 0, std::numeric_limits<uint64_t>::max(), true},

    // Eage cases: the result overflows.
    {1, std::numeric_limits<uint64_t>::max(), 0, false},
    {std::numeric_limits<uint64_t>::max(), 1, 0, false},
    {10, std::numeric_limits<uint64_t>::max(), 0, false},
    {std::numeric_limits<uint64_t>::max(), 10, 0, false},
    {std::numeric_limits<uint64_t>::max() / 2 + 1, (std::numeric_limits<uint64_t>::max()-1)/2+1, 0, false},
    {(std::numeric_limits<uint64_t>::max()-1)/2+1, 0, std::numeric_limits<uint64_t>::max()/2+1,0, false},
};

INSTANTIATE_TEST_SUITE_P(SafeArithmeticTest, SafeUnsignedInt64AddTest, testing::ValuesIn(safe_unsigned_int64_add_tests));

} // namespace dsn
