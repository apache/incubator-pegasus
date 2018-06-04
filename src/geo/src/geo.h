// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/tool-api/task_tracker.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/util/units/length-units.h>
#include <pegasus/client.h>

namespace pegasus {

class geo
{
public:
    int
    init(dsn::string_view config_file, dsn::string_view cluster_name, dsn::string_view app_name);

    int search_radial(double lat_degrees,
                      double lng_degrees,
                      double radius_m,
                      int count,
                      int sort_type,
                      std::list<std::pair<std::string, double>> &result);

    void TEST_fill_data_in_rect(S2LatLngRect rect, int count);

private:
    int scan_next(const S2LatLng &center,
                  util::units::Meters radius,
                  int count,
                  std::list<std::pair<std::string, double>> &result,
                  const pegasus_client::pegasus_scanner_wrapper &wrap_scanner);
    int scan_data(const std::string &hash_key,
                  const std::string &start_sort_key,
                  const std::string &stop_sort_key,
                  const S2LatLng &center,
                  util::units::Meters radius,
                  int count,
                  std::list<std::pair<std::string, double>> &result);

private:
    static const int min_level = 12; // edge length at about 2km
    static const int max_level = 16; // edge length at about 150m

    dsn::task_tracker _tracker;
    pegasus::pegasus_client *_client = nullptr;
};

} // namespace pegasus
