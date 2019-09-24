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
