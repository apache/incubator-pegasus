// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <vector>
#include <s2/s2latlng.h>
#include <dsn/utility/strings.h>

namespace dsn {
class error_s;
} // namespace dsn

namespace pegasus {
namespace geo {

class latlng_codec
{
public:
    // Decode latitude and longitude from string type value.
    // Return true when succeed.
    bool decode_from_value(const std::string &value, S2LatLng &latlng) const;

    // Encode latitude and longitude into string type value.
    // Return true when succeed.
    bool encode_to_value(double lat_degrees, double lng_degrees, std::string &value) const;

    // Set latitude and longitude indices in string type value, indices are the ones
    // when the string type value split into list by '|'.
    dsn::error_s set_latlng_indices(uint32_t latitude_index, uint32_t longitude_index);

private:
    // Latitude index and longitude index in sorted order.
    std::vector<int> _sorted_indices;
    // Whether '_sorted_indices' is in latitude-longitude order.
    bool _latlng_order = true;
};

} // namespace geo
} // namespace pegasus
