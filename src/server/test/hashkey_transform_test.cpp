/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "server/hashkey_transform.h"

#include <fmt/core.h>
#include <rocksdb/comparator.h>
#include <string>

#include "base/pegasus_key_schema.h"
#include "gtest/gtest.h"
#include "utils/blob.h"

// User define SliceTransform must obey the 4 rules of ColumnFamilyOptions.prefix_extractor:
// 1) key.starts_with(prefix(key))
// 2) Compare(prefix(key), key) <= 0.
// 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
// 4) prefix(prefix(key)) == prefix(key)
TEST(HashkeyTransformTest, Basic)
{
    pegasus::server::HashkeyTransform prefix_extractor;
    const rocksdb::Comparator *comp = rocksdb::BytewiseComparator();

    dsn::blob bkey1, bkey2, bkey3, bkey4;
    pegasus::pegasus_generate_key(bkey1, std::string("h1"), std::string("s1"));
    pegasus::pegasus_generate_key(bkey2, std::string("h2"), std::string("s1"));
    pegasus::pegasus_generate_key(bkey3, std::string("h1"), std::string("s2"));
    pegasus::pegasus_generate_key(bkey4, std::string("h1"), std::string(""));
    rocksdb::Slice skey1(bkey1.data(), bkey1.size());
    rocksdb::Slice skey2(bkey2.data(), bkey2.size());
    rocksdb::Slice skey3(bkey3.data(), bkey3.size());
    rocksdb::Slice skey4(bkey4.data(), bkey4.size());

    // 1) key.starts_with(prefix(key))
    ASSERT_TRUE(skey1.starts_with(prefix_extractor.Transform(skey1)));
    ASSERT_TRUE(skey2.starts_with(prefix_extractor.Transform(skey2)));
    ASSERT_TRUE(skey3.starts_with(prefix_extractor.Transform(skey3)));
    ASSERT_TRUE(skey4.starts_with(prefix_extractor.Transform(skey4)));

    // 2) Compare(prefix(key), key) <= 0.
    ASSERT_LT(comp->Compare(prefix_extractor.Transform(skey1), skey1), 0); // h1 < h1s1
    ASSERT_LT(comp->Compare(prefix_extractor.Transform(skey2), skey2), 0); // h2 < h2s1
    ASSERT_LT(comp->Compare(prefix_extractor.Transform(skey3), skey3), 0); // h1 < h1s2
    ASSERT_EQ(comp->Compare(prefix_extractor.Transform(skey4), skey4), 0); // h1 == h1

    // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
    ASSERT_LT(comp->Compare(skey1, skey2), 0); // h1s1 < h2s1
    ASSERT_LT(comp->Compare(prefix_extractor.Transform(skey1), prefix_extractor.Transform(skey2)),
              0);                              // h1 < h2
    ASSERT_LT(comp->Compare(skey1, skey3), 0); // h1s1 < h1s2
    ASSERT_EQ(comp->Compare(prefix_extractor.Transform(skey1), prefix_extractor.Transform(skey3)),
              0);                              // h1 == h1
    ASSERT_GT(comp->Compare(skey1, skey4), 0); // h1s1 > h1
    ASSERT_EQ(comp->Compare(prefix_extractor.Transform(skey1), prefix_extractor.Transform(skey4)),
              0); // h1 == h1

    // 4) prefix(prefix(key)) == prefix(key)
    ASSERT_EQ(prefix_extractor.Transform(prefix_extractor.Transform(skey1)),
              prefix_extractor.Transform(skey1));
    ASSERT_EQ(prefix_extractor.Transform(prefix_extractor.Transform(skey2)),
              prefix_extractor.Transform(skey2));
    ASSERT_EQ(prefix_extractor.Transform(prefix_extractor.Transform(skey3)),
              prefix_extractor.Transform(skey3));
    ASSERT_EQ(prefix_extractor.Transform(prefix_extractor.Transform(skey4)),
              prefix_extractor.Transform(skey4));
}
