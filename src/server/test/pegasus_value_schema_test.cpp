// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_value_schema.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(value_schema, generate_and_extract_timetag)
{
    uint64_t timestamp = 10000;
    uint8_t cluster_id = 1;
    uint64_t timetag = generate_timetag(timestamp, cluster_id, false);

    ASSERT_EQ(cluster_id, extract_cluster_id_from_timetag(timetag));
}

TEST(value_schema, generate_and_extract_from_value_v1)
{
    uint32_t expire_ts = 1000;
    uint64_t timetag = generate_timetag(10000, 1, false);

    pegasus_value_generator gen;
    rocksdb::SliceParts sparts = gen.generate_value(1, dsn::blob(), expire_ts, timetag);

    std::string raw_value;
    for (int i = 0; i < sparts.num_parts; i++) {
        raw_value += sparts.parts[i].ToString();
    }

    ASSERT_EQ(expire_ts, pegasus_extract_expire_ts(1, raw_value));
    ASSERT_EQ(timetag, pegasus_extract_timetag(1, raw_value));
}

TEST(value_schema, generate_and_extract_user_value)
{
    for (int version = 0; version <= 1; version++) {
        std::string data = "hello";

        pegasus_value_generator gen;
        rocksdb::SliceParts sparts =
            gen.generate_value(version, dsn::blob(data.data(), 0, data.length()), 1000, 10000);

        std::string raw_value;
        for (int i = 0; i < sparts.num_parts; i++) {
            raw_value += sparts.parts[i].ToString();
        }

        dsn::blob user_value;
        pegasus_extract_user_data(version, std::move(raw_value), user_value);

        ASSERT_EQ(user_value.to_string(), data);
    }
}
