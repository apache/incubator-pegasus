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

// These macros and functions are inspired by Apache Kudu.

#pragma once

#include <gmock/gmock.h>
#include <string>

// ASSERT_NO_FATAL_FAILURE is just too long to type.
#define NO_FATALS(expr) ASSERT_NO_FATAL_FAILURE(expr)

// Detect fatals in the surrounding scope. NO_FATALS() only checks for fatals
// in the expression passed to it.
#define NO_PENDING_FATALS()                                                                        \
    do {                                                                                           \
        if (testing::Test::HasFatalFailure()) {                                                    \
            return;                                                                                \
        }                                                                                          \
    } while (0)

// Assert that `str` should contain the given `substr`.
#define ASSERT_STR_CONTAINS(str, substr) ASSERT_THAT(str, testing::HasSubstr(substr))

// Assert that `str` should not contain the given `substr`.
#define ASSERT_STR_NOT_CONTAINS(str, substr)                                                       \
    ASSERT_THAT(str, testing::Not(testing::HasSubstr(substr)))

// Assert that `str` should begin with the given `prefix`.
#define ASSERT_STR_STARTSWITH(str, prefix) ASSERT_THAT(str, testing::StartsWith(prefix))

// Assert that `str` should not begin with the given `prefix`.
#define ASSERT_STR_NOT_STARTSWITH(str, prefix)                                                     \
    ASSERT_THAT(str, testing::Not(testing::StartsWith(prefix)))

// Assert that `str` should end with the given `suffix`.
#define ASSERT_STR_ENDSWITH(str, suffix) ASSERT_THAT(str, testing::EndsWith(suffix))

// Assert that `str` should not end with the given `suffix`.
#define ASSERT_STR_NOT_ENDSWITH(str, suffix)                                                       \
    ASSERT_THAT(str, testing::Not(testing::EndsWith(suffix)))
