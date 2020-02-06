// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_value_schema.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(value_schema, generate_and_extract_v0)
{
    struct test_case
    {
        int value_schema_version;

        uint32_t expire_ts;
        std::string user_data;
    } tests[] = {
        {0, 1000, ""},
        {0, std::numeric_limits<uint32_t>::max(), "pegasus"},
        {0, 0, "a"},
        {0, std::numeric_limits<uint32_t>::max(), ""},
    };

    for (auto &t : tests) {
        pegasus_value_generator gen;
        rocksdb::SliceParts sparts =
            gen.generate_value(t.value_schema_version, t.user_data, t.expire_ts);

        std::string raw_value;
        for (int i = 0; i < sparts.num_parts; i++) {
            raw_value += sparts.parts[i].ToString();
        }

        ASSERT_EQ(t.expire_ts, pegasus_extract_expire_ts(t.value_schema_version, raw_value));

        dsn::blob user_data;
        pegasus_extract_user_data(t.value_schema_version, std::move(raw_value), user_data);
        ASSERT_EQ(t.user_data, user_data.to_string());
    }
}
