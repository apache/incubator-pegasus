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

#include "latlng_codec.h"

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "utils/fmt_logging.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/string_conv.h"

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

bool latlng_codec::decode_from_value(const std::string &value, S2LatLng &latlng) const
{
    CHECK_EQ(_sorted_indices.size(), 2);
    std::vector<std::string> data;
    extract_indices(value, _sorted_indices, data, '|');
    if (data.size() != 2) {
        return false;
    }

    std::string &lat = data[_latlng_order ? 0 : 1];
    std::string &lng = data[_latlng_order ? 1 : 0];
    double lat_degrees = 0.0;
    double lng_degrees = 0.0;
    if (!dsn::buf2double(lat, lat_degrees) || !dsn::buf2double(lng, lng_degrees)) {
        return false;
    }
    latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);

    return latlng.is_valid();
}

bool latlng_codec::encode_to_value(double lat_degrees, double lng_degrees, std::string &value) const
{
    CHECK_EQ(_sorted_indices.size(), 2);
    S2LatLng latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);
    if (!latlng.is_valid()) {
        LOG_ERROR_F("latlng is invalid. lat_degrees={}, lng_degrees={}", lat_degrees, lng_degrees);
        return false;
    }

    value.clear();
    int index = 0;
    for (int i = 0; i <= _sorted_indices[1]; ++i) {
        if (i == _sorted_indices[index]) {
            if ((index == 0 && _latlng_order) || (index == 1 && !_latlng_order)) {
                value += std::to_string(latlng.lat().degrees());
            } else {
                value += std::to_string(latlng.lng().degrees());
            }
            ++index;
        }
        if (i != _sorted_indices[1]) {
            value += '|';
        }
    }
    return true;
}

dsn::error_s latlng_codec::set_latlng_indices(uint32_t latitude_index, uint32_t longitude_index)
{
    if (latitude_index == longitude_index) {
        return dsn::error_s::make(dsn::ERR_INVALID_PARAMETERS,
                                  "latitude_index and longitude_index should not be equal");
    } else if (latitude_index < longitude_index) {
        _sorted_indices = {(int)latitude_index, (int)longitude_index};
        _latlng_order = true;
    } else {
        _sorted_indices = {(int)longitude_index, (int)latitude_index};
        _latlng_order = false;
    }
    return dsn::error_s::ok();
}

} // namespace geo
} // namespace pegasus
