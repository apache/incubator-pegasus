// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "latlng_extractor.h"

#include <dsn/utility/error_code.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/string_conv.h>

namespace pegasus {
namespace geo {

void extract_indices(const std::string &line,
                     const std::vector<int> &sorted_indices,
                     std::vector<std::string> &values,
                     char splitter)
{
    size_t begin_pos = 0;
    size_t end_pos = 0;
    int cur_index = -1;
    for (auto index : sorted_indices) {
        while (cur_index < index) {
            begin_pos = (cur_index == -1 ? 0 : end_pos + 1); // at first time, seek from 0
                                                             // then, seek from end_pos + 1
            end_pos = line.find(splitter, begin_pos);
            if (end_pos == std::string::npos) {
                break;
            }
            cur_index++;
        }

        if (end_pos == std::string::npos) {
            values.emplace_back(line.substr(begin_pos));
            break;
        } else {
            values.emplace_back(line.substr(begin_pos, end_pos - begin_pos));
        }
    }
}

bool latlng_extractor::extract_from_value(const std::string &value, S2LatLng &latlng)
{
    std::vector<std::string> data;
    extract_indices(value, _sorted_indices, data, '|');
    if (data.size() != 2) {
        return false;
    }

    std::string &lat = data[_latlng_reversed ? 1 : 0];
    std::string &lng = data[_latlng_reversed ? 0 : 1];
    double lat_degrees = 0.0;
    double lng_degrees = 0.0;
    if (!dsn::buf2double(lat, lat_degrees) || !dsn::buf2double(lng, lng_degrees)) {
        return false;
    }
    latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);

    return latlng.is_valid();
}

dsn::error_s latlng_extractor::set_latlng_indices(std::pair<uint32_t, uint32_t> indices)
{
    if (indices.first == indices.second) {
        return dsn::error_s::make(dsn::ERR_INVALID_PARAMETERS,
                                  "latitude index longitude index should not be equal");
    } else if (indices.first < indices.second) {
        _sorted_indices = {(int)indices.first, (int)indices.second};
        _latlng_reversed = false;
    } else {
        _sorted_indices = {(int)indices.second, (int)indices.first};
        _latlng_reversed = true;
    }
    return dsn::error_s::ok();
}

} // namespace geo
} // namespace pegasus
