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

// This file is inspired by Apache Kudu.

#include "utils/map_util.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <absl/strings/string_view.h>
#include <gtest/gtest.h>

using std::map;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace dsn {

TEST(FloorTest, TestMapUtil)
{
    map<int, int> my_map;

    ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 5));

    my_map[5] = 5;
    ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
    ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
    ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 4));

    my_map[1] = 1;
    ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
    ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
    ASSERT_EQ(1, *FindFloorOrNull(my_map, 4));
    ASSERT_EQ(1, *FindFloorOrNull(my_map, 1));
    ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 0));
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsent)
{
    map<string, string> my_map;
    auto result = ComputeIfAbsent(&my_map, "key", [] { return "hello_world"; });
    ASSERT_EQ(*result, "hello_world");
    auto result2 = ComputeIfAbsent(&my_map, "key", [] { return "hello_world2"; });
    ASSERT_EQ(*result2, "hello_world");
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsentAndReturnAbsense)
{
    map<string, string> my_map;
    auto result = ComputeIfAbsentReturnAbsense(&my_map, "key", [] { return "hello_world"; });
    ASSERT_TRUE(result.second);
    ASSERT_EQ(*result.first, "hello_world");
    auto result2 = ComputeIfAbsentReturnAbsense(&my_map, "key", [] { return "hello_world2"; });
    ASSERT_FALSE(result2.second);
    ASSERT_EQ(*result2.first, "hello_world");
}

namespace {
// Simple struct to act as a container for a string. While not necessary per
// se, this is more representative of the expected usage of
// ComputePairIfAbsent* (i.e. pointing to internal state of more complex
// objects).
struct SimpleStruct
{
    string str;
};
} // anonymous namespace

TEST(ComputePairIfAbsentTest, TestComputePairDestructState)
{
    unordered_map<absl::string_view, int> string_to_idx;
    const string kBigKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    string big_key = kBigKey;
    vector<SimpleStruct> big_structs;
    auto result = ComputePairIfAbsentReturnAbsense(
        &string_to_idx, big_key, [&]() -> std::pair<absl::string_view, int> {
            int idx = big_structs.size();
            big_structs.emplace_back(SimpleStruct({big_key}));
            return {big_structs.back().str, idx};
        });
    // Clear the original key state. This shouldn't have any effect on the map.
    big_key.clear();
    ASSERT_TRUE(result.second);
    ASSERT_EQ(*result.first, FindOrDie(string_to_idx, kBigKey));
    ASSERT_EQ(kBigKey, big_structs[*result.first].str);
}

TEST(FindPointeeOrNullTest, TestFindPointeeOrNull)
{
    map<string, unique_ptr<string>> my_map;
    auto iter = my_map.emplace("key", unique_ptr<string>(new string("hello_world")));
    ASSERT_TRUE(iter.second);
    string *value = FindPointeeOrNull(my_map, "key");
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, "hello_world");
    my_map.erase(iter.first);
    value = FindPointeeOrNull(my_map, "key");
    ASSERT_TRUE(value == nullptr);
}

TEST(EraseKeyReturnValuePtrTest, TestRawAndSmartSmartPointers)
{
    map<string, unique_ptr<string>> my_map;
    unique_ptr<string> value = EraseKeyReturnValuePtr(&my_map, "key");
    ASSERT_TRUE(value.get() == nullptr);
    my_map.emplace("key", unique_ptr<string>(new string("hello_world")));
    value = EraseKeyReturnValuePtr(&my_map, "key");
    ASSERT_EQ(*value, "hello_world");
    value.reset();
    value = EraseKeyReturnValuePtr(&my_map, "key");
    ASSERT_TRUE(value.get() == nullptr);
    map<string, shared_ptr<string>> my_map2;
    shared_ptr<string> value2 = EraseKeyReturnValuePtr(&my_map2, "key");
    ASSERT_TRUE(value2.get() == nullptr);
    my_map2.emplace("key", std::make_shared<string>("hello_world"));
    value2 = EraseKeyReturnValuePtr(&my_map2, "key");
    ASSERT_EQ(*value2, "hello_world");
    map<string, string *> my_map_raw;
    my_map_raw.emplace("key", new string("hello_world"));
    value.reset(EraseKeyReturnValuePtr(&my_map_raw, "key"));
    ASSERT_EQ(*value, "hello_world");
}

TEST(EmplaceTest, TestEmplace)
{
    string key1("k");
    string key2("k2");
    // Map with move-only value type.
    map<string, unique_ptr<string>> my_map;
    unique_ptr<string> val(new string("foo"));
    ASSERT_TRUE(EmplaceIfNotPresent(&my_map, key1, std::move(val)));
    ASSERT_TRUE(ContainsKey(my_map, key1));
    ASSERT_FALSE(EmplaceIfNotPresent(&my_map, key1, nullptr))
        << "Should return false for already-present";

    val = unique_ptr<string>(new string("bar"));
    ASSERT_TRUE(EmplaceOrUpdate(&my_map, key2, std::move(val)));
    ASSERT_TRUE(ContainsKey(my_map, key2));
    ASSERT_EQ("bar", *FindOrDie(my_map, key2));
    val = unique_ptr<string>(new string("foobar"));
    ASSERT_FALSE(EmplaceOrUpdate(&my_map, key2, std::move(val)));
    ASSERT_EQ("foobar", *FindOrDie(my_map, key2));
}

TEST(LookupOrEmplaceTest, IntMap)
{
    const string key = "mega";
    map<string, int> int_map;

    {
        const auto &val = LookupOrEmplace(&int_map, key, 0);
        ASSERT_EQ(0, val);
        auto *val_ptr = FindOrNull(int_map, key);
        ASSERT_NE(nullptr, val_ptr);
        ASSERT_EQ(0, *val_ptr);
    }

    {
        auto &val = LookupOrEmplace(&int_map, key, 10);
        ASSERT_EQ(0, val);
        ++val;
        auto *val_ptr = FindOrNull(int_map, key);
        ASSERT_NE(nullptr, val_ptr);
        ASSERT_EQ(1, *val_ptr);
    }

    {
        LookupOrEmplace(&int_map, key, 100) += 1000;
        auto *val_ptr = FindOrNull(int_map, key);
        ASSERT_NE(nullptr, val_ptr);
        ASSERT_EQ(1001, *val_ptr);
    }
}

TEST(LookupOrEmplaceTest, UniquePtrMap)
{
    constexpr int key = 0;
    const string ref_str = "turbo";
    map<int, unique_ptr<string>> uptr_map;

    {
        unique_ptr<string> val(new string(ref_str));
        const auto &lookup_val = LookupOrEmplace(&uptr_map, key, std::move(val));
        ASSERT_EQ(nullptr, val.get());
        ASSERT_NE(nullptr, lookup_val.get());
        ASSERT_EQ(ref_str, *lookup_val);
    }

    {
        unique_ptr<string> val(new string("giga"));
        auto &lookup_val = LookupOrEmplace(&uptr_map, key, std::move(val));
        ASSERT_NE(nullptr, lookup_val.get());
        ASSERT_EQ(ref_str, *lookup_val);
        // Update the stored value.
        *lookup_val = "giga";
    }

    {
        unique_ptr<string> val(new string(ref_str));
        const auto &lookup_val = LookupOrEmplace(&uptr_map, key, std::move(val));
        ASSERT_NE(nullptr, lookup_val.get());
        ASSERT_EQ("giga", *lookup_val);
    }
}

TEST(EmplaceOrDieTest, MapLikeContainer)
{
    constexpr int key = 0;
    const string ref_value_0 = "turbo";
    const string ref_value_1 = "giga";
    map<int, string> int_to_string;

    {
        auto &mapped = EmplaceOrDie(&int_to_string, key, "turbo");
        ASSERT_EQ(ref_value_0, mapped);
        mapped = "giga";
    }
    {
        const auto &mapped = FindOrDie(int_to_string, key);
        ASSERT_EQ(ref_value_1, mapped);
    }
}

TEST(EmplaceOrDieTest, SetLikeContainer)
{
    const string ref_str = "turbo";
    set<string> strings;

    auto &value = EmplaceOrDie(&strings, "turbo");
    ASSERT_EQ(ref_str, value);
}

} // namespace dsn
