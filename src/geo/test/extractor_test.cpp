// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <memory>
#include <gtest/gtest.h>
#include <geo/lib/latlng_extractor.h>

namespace pegasus {
namespace geo {

static std::shared_ptr<latlng_extractor_for_lbs> lbs_extractor =
    std::make_shared<latlng_extractor_for_lbs>();
TEST(latlng_extractor_for_lbs_test, extract_from_value)
{
    ASSERT_EQ(std::string(lbs_extractor->name()), "latlng_extractor_for_lbs");

    S2LatLng latlng;
    ASSERT_TRUE(lbs_extractor->extract_from_value(lbs_extractor->value_sample(), latlng));

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                             "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(lbs_extractor->extract_from_value(test_value, latlng));
    ASSERT_LE(std::abs(latlng.lat().degrees() - lat_degrees), 0.000001);
    ASSERT_LE(std::abs(latlng.lng().degrees() - lng_degrees), 0.000001);

    test_value = "|2018-04-26|2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(lbs_extractor->extract_from_value(test_value, latlng));
    ASSERT_LE(std::abs(latlng.lat().degrees() - lat_degrees), 0.000001);
    ASSERT_LE(std::abs(latlng.lng().degrees() - lng_degrees), 0.000001);

    test_value = "00:00:00:00:01:5e||2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(lbs_extractor->extract_from_value(test_value, latlng));
    ASSERT_LE(std::abs(latlng.lat().degrees() - lat_degrees), 0.000001);
    ASSERT_LE(std::abs(latlng.lng().degrees() - lng_degrees), 0.000001);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) + "||4.15921|0|-1";
    ASSERT_TRUE(lbs_extractor->extract_from_value(test_value, latlng));
    ASSERT_LE(std::abs(latlng.lat().degrees() - lat_degrees), 0.000001);
    ASSERT_LE(std::abs(latlng.lng().degrees() - lng_degrees), 0.000001);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                 "|24.043028|4.15921|0|";
    ASSERT_TRUE(lbs_extractor->extract_from_value(test_value, latlng));
    ASSERT_LE(std::abs(latlng.lat().degrees() - lat_degrees), 0.000001);
    ASSERT_LE(std::abs(latlng.lng().degrees() - lng_degrees), 0.000001);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr||" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_FALSE(lbs_extractor->extract_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "||24.043028|4.15921|0|-1";
    ASSERT_FALSE(lbs_extractor->extract_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|||24.043028|4.15921|0|-1";
    ASSERT_FALSE(lbs_extractor->extract_from_value(test_value, latlng));
}

} // namespace geo
} // namespace pegasus
