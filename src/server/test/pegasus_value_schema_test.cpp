// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_value_schema.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(value_schema, generate_and_extract_timetag)
{
    struct test_case
    {
        uint64_t timestamp;
        bool delete_tag;

        uint64_t wtimestamp;
    } tests[] = {
        {1000, true, 1000},
        {1000, false, 1000},

        {std::numeric_limits<uint64_t>::max() >> 8,
         true,
         std::numeric_limits<uint64_t>::max() >> 8},

        {std::numeric_limits<uint64_t>::max() >> 8,
         false,
         std::numeric_limits<uint64_t>::max() >> 8},

        {std::numeric_limits<uint64_t>::max(), false, std::numeric_limits<uint64_t>::max() >> 8},

        {std::numeric_limits<uint64_t>::max(), false, std::numeric_limits<uint64_t>::max() >> 8},

        // Wed, 12 Dec 2018 09:48:48 GMT
        {1544583472297055, false, 1544583472297055},
    };

    for (auto &t : tests) {
        for (uint8_t cluster_id = 1; cluster_id <= 0x7F; cluster_id++) {
            uint64_t timetag = generate_timetag(t.timestamp, cluster_id, t.delete_tag);
            ASSERT_EQ(cluster_id, extract_cluster_id_from_timetag(timetag));
            ASSERT_EQ(t.wtimestamp, extract_timestamp_from_timetag(timetag));
        }
    }
}

TEST(value_schema, generate_and_extract_v1_v0)
{
    struct test_case
    {
        int value_schema_version;

        uint32_t expire_ts;
        uint64_t timetag;
        std::string user_data;
    } tests[] = {
        {1, 1000, 10001, ""},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), "pegasus"},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), ""},

        {0, 1000, 0, ""},
        {0, std::numeric_limits<uint32_t>::max(), 0, "pegasus"},
        {0, std::numeric_limits<uint32_t>::max(), 0, ""},
        {0, 0, 0, "a"},
    };

    for (auto &t : tests) {
        pegasus_value_generator gen;
        rocksdb::SliceParts sparts =
            gen.generate_value(t.value_schema_version, t.user_data, t.expire_ts, t.timetag);

        std::string raw_value;
        for (int i = 0; i < sparts.num_parts; i++) {
            raw_value += sparts.parts[i].ToString();
        }

        ASSERT_EQ(t.expire_ts, pegasus_extract_expire_ts(t.value_schema_version, raw_value));

        if (t.value_schema_version == 1) {
            ASSERT_EQ(t.timetag, pegasus_extract_timetag(t.value_schema_version, raw_value));
        }

        dsn::blob user_data;
        pegasus_extract_user_data(t.value_schema_version, std::move(raw_value), user_data);
        ASSERT_EQ(t.user_data, user_data.to_string());
    }
}
