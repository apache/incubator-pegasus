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

#include "utils/rand.h"
#include <gtest/gtest.h>
#include <thread>

namespace dsn {

TEST(random, sanity)
{
    { // edge cases
        ASSERT_EQ(rand::next_u64(0), 0);
        ASSERT_EQ(rand::next_u64(0, 0), 0);
        ASSERT_EQ(rand::next_u32(0), 0);
        ASSERT_EQ(rand::next_u32(0, 0), 0);

        ASSERT_EQ(rand::next_u64(12, 12), 12);
        ASSERT_EQ(rand::next_u32(12, 12), 12);
    }

    constexpr int kTestSize = 1000;

    { // 32-bit repeatability, uniqueness
        rand::reseed_thread_local_rng(0xdeadbeef);
        std::vector<uint32_t> vals(kTestSize);
        for (int i = 0; i < kTestSize; ++i) {
            vals[i] = rand::next_u32();
        }

        rand::reseed_thread_local_rng(0xdeadbeef);
        for (int i = 0; i < kTestSize; ++i) {
            ASSERT_EQ(rand::next_u32(), vals[i]);
        }
    }

    { // 64-bit repeatability, uniqueness
        rand::reseed_thread_local_rng(0xdeadbeef);
        std::vector<uint64_t> vals(kTestSize);
        for (int i = 0; i < kTestSize; ++i) {
            vals[i] = rand::next_u64();
        }

        rand::reseed_thread_local_rng(0xdeadbeef);
        for (int i = 0; i < kTestSize; ++i) {
            ASSERT_EQ(rand::next_u64(), vals[i]);
        }
    }
}

TEST(random, multi_threaded)
{
    const int n = 100;
    std::vector<uint32_t> seeds(n);
    std::vector<std::thread> threads;
    for (int i = 0; i < n; ++i) {
        threads.push_back(std::thread([i, &seeds] { seeds[i] = rand::next_u32(); }));
    }
    for (auto &t : threads) {
        t.join();
    }
    std::sort(seeds.begin(), seeds.end());
    for (int i = 0; i < n - 1; ++i) {
        EXPECT_LT(seeds[i], seeds[i + 1]);
    }
}

} // namespace dsn
