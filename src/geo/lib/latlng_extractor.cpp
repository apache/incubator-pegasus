// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/string_conv.h>
#include "latlng_extractor.h"

namespace pegasus {
namespace geo {

void extract_indexs(const std::string &text,
                    const std::vector<int> &indexs,
                    std::vector<std::string> &values,
                    char splitter)
{
    size_t begin_pos = 0;
    size_t end_pos = 0;
    int cur_index = -1;
    for (auto index : indexs) {
        while (cur_index < index) {
            begin_pos = (cur_index == -1 ? 0 : end_pos + 1); // at first time, seek from 0
                                                             // then, seek from end_pos + 1
            end_pos = text.find(splitter, begin_pos);
            if (end_pos == std::string::npos) {
                break;
            }
            cur_index++;
        }

        if (end_pos == std::string::npos) {
            values.emplace_back(text.substr(begin_pos));
            break;
        } else {
            values.emplace_back(text.substr(begin_pos, end_pos - begin_pos));
        }
    }
}

const char *latlng_extractor_for_lbs::name() const { return "latlng_extractor_for_lbs"; }

const char *latlng_extractor_for_lbs::value_sample() const
{
    return "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|160.356396|39.469644|24.0|4.15|0|-1";
}

bool latlng_extractor_for_lbs::extract_from_value(const std::string &value, S2LatLng &latlng) const
{
    std::vector<std::string> data;
    extract_indexs(value, {4, 5}, data, '|');
    if (data.size() != 2) {
        return false;
    }

    std::string lng = data[0];
    std::string lat = data[1];
    double lat_degrees, lng_degrees = 0.0;
    if (!dsn::buf2double(lat, lat_degrees) || !dsn::buf2double(lng, lng_degrees)) {
        return false;
    }
    latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);

    return latlng.is_valid();
}

} // namespace geo
} // namespace pegasus
