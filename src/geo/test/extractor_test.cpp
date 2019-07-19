// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <memory>
#include <gtest/gtest.h>
#include <geo/lib/latlng_extractor.h>
#include <dsn/utility/errors.h>

namespace pegasus {
namespace geo {

TEST(latlng_extractor_test, set_latlng_indices)
{
    latlng_extractor extractor;
    ASSERT_FALSE(extractor.set_latlng_indices(std::make_pair(3, 3)).is_ok());
    ASSERT_TRUE(extractor.set_latlng_indices(std::make_pair(3, 4)).is_ok());
    ASSERT_TRUE(extractor.set_latlng_indices(std::make_pair(4, 3)).is_ok());
}

TEST(latlng_extractor_for_lbs_test, extract_from_value)
{
    latlng_extractor extractor;
    ASSERT_TRUE(extractor.set_latlng_indices(std::make_pair(5, 4)).is_ok());

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    S2LatLng latlng;

    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                             "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "|2018-04-26|2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e||2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) + "||4.15921|0|-1";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                 "|24.043028|4.15921|0|";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr||" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "||24.043028|4.15921|0|-1";
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|||24.043028|4.15921|0|-1";
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));
}

TEST(latlng_extractor_for_aibox_test, extract_from_value)
{
    latlng_extractor extractor;
    ASSERT_TRUE(extractor.set_latlng_indices(std::make_pair(0, 1)).is_ok());

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    S2LatLng latlng;

    std::string test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees);
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) + "|24.043028";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) + "||";
    ASSERT_TRUE(extractor.extract_from_value(test_value, latlng));
    EXPECT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    EXPECT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "|" + std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees);
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));

    test_value = "|" + std::to_string(lat_degrees);
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));

    test_value = std::to_string(lng_degrees) + "|";
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));

    test_value = "|";
    ASSERT_FALSE(extractor.extract_from_value(test_value, latlng));
}

} // namespace geo
} // namespace pegasus
