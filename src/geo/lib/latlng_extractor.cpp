// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "latlng_extractor.h"

namespace pegasus {
namespace geo {

const char *latlng_extractor_for_lbs::name() const { return "latlng_extractor_for_lbs"; }

const char *latlng_extractor_for_lbs::value_sample() const
{
    return "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|160.356396|39.469644|24.0|4.15|0|-1";
}

bool latlng_extractor_for_lbs::extract_from_value(const std::string &value, S2LatLng &latlng) const
{
    std::vector<std::string> data;
    dsn::utils::split_args(value.c_str(), data, '|');
    if (data.size() <= 6) {
        return false;
    }

    std::string lat = data[5];
    std::string lng = data[4];
    latlng = S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr), strtod(lng.c_str(), nullptr));

    return true;
}

} // namespace geo
} // namespace pegasus