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

#pragma once

#include <string>
#include <vector>
#include <s2/s2latlng.h>
#include "utils/strings.h"

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
