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

#include <memory>
#include <gtest/gtest.h>
#include <geo/lib/latlng_codec.h>
#include "utils/errors.h"

namespace pegasus {
namespace geo {

TEST(latlng_codec_test, set_latlng_indices)
{
    latlng_codec codec;
    ASSERT_FALSE(codec.set_latlng_indices(3, 3).is_ok());
    ASSERT_TRUE(codec.set_latlng_indices(3, 4).is_ok());
    ASSERT_TRUE(codec.set_latlng_indices(4, 3).is_ok());
}

TEST(latlng_codec_for_lbs_test, decode_from_value)
{
    latlng_codec codec;
    ASSERT_TRUE(codec.set_latlng_indices(5, 4).is_ok());

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    S2LatLng latlng;

    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                             "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "|2018-04-26|2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e||2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) + "|" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) + "||4.15921|0|-1";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "|" + std::to_string(lat_degrees) +
                 "|24.043028|4.15921|0|";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr||" +
                 std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                 std::to_string(lng_degrees) + "||24.043028|4.15921|0|-1";
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));

    test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|||24.043028|4.15921|0|-1";
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));
}

TEST(latlng_codec_for_aibox_test, decode_from_value)
{
    latlng_codec codec;
    ASSERT_TRUE(codec.set_latlng_indices(0, 1).is_ok());

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    S2LatLng latlng;

    std::string test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees);
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) + "|24.043028";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) + "||";
    ASSERT_TRUE(codec.decode_from_value(test_value, latlng));
    ASSERT_DOUBLE_EQ(latlng.lat().degrees(), lat_degrees);
    ASSERT_DOUBLE_EQ(latlng.lng().degrees(), lng_degrees);

    test_value = "|" + std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees);
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));

    test_value = "|" + std::to_string(lat_degrees);
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));

    test_value = std::to_string(lng_degrees) + "|";
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));

    test_value = "|";
    ASSERT_FALSE(codec.decode_from_value(test_value, latlng));
}

TEST(latlng_codec_encode_test, encode_to_value)
{
    latlng_codec codec;

    double lat_degrees = 12.345;
    double lng_degrees = 67.890;

    std::string value;
    ASSERT_TRUE(codec.set_latlng_indices(0, 1).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("12.345000|67.890000", value);

    ASSERT_TRUE(codec.set_latlng_indices(1, 0).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("67.890000|12.345000", value);

    ASSERT_TRUE(codec.set_latlng_indices(4, 5).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("||||12.345000|67.890000", value);

    ASSERT_TRUE(codec.set_latlng_indices(5, 4).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("||||67.890000|12.345000", value);

    ASSERT_TRUE(codec.set_latlng_indices(0, 3).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("12.345000|||67.890000", value);

    ASSERT_TRUE(codec.set_latlng_indices(3, 1).is_ok());
    ASSERT_TRUE(codec.encode_to_value(lat_degrees, lng_degrees, value));
    ASSERT_EQ("|67.890000||12.345000", value);

    ASSERT_FALSE(codec.encode_to_value(91, lng_degrees, value));
    ASSERT_FALSE(codec.encode_to_value(-91, lng_degrees, value));
    ASSERT_FALSE(codec.encode_to_value(lat_degrees, 181, value));
    ASSERT_FALSE(codec.encode_to_value(lat_degrees, -181, value));
}
} // namespace geo
} // namespace pegasus
