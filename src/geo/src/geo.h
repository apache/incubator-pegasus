// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <sstream>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/util/units/length-units.h>
#include <dsn/utility/string_view.h>
#include <dsn/tool-api/task_tracker.h>
#include <pegasus/client.h>

namespace pegasus {

using latlng_extractor = std::function<int(const std::string &value, S2LatLng &latlng)>;

struct SearchResult
{
    double lat_degrees;
    double lng_degrees;
    double distance;
    std::string hashkey;
    std::string sortkey;
    std::string value;

    explicit SearchResult(double lat = 0.0,
                          double lng = 0.0,
                          double dis = 0.0,
                          std::string &&hk = "",
                          std::string &&sk = "",
                          std::string &&v = "")
        : lat_degrees(lat), lng_degrees(lng), distance(dis), hashkey(hk), sortkey(sk), value(v)
    {
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << "[" << hashkey << " : " << sortkey << " => " << value << ", (" << lat_degrees << ", "
           << lng_degrees << "): " << distance << "]";
        return std::move(ss.str());
    }
};

struct SearchResultNearer
{
    inline bool operator()(const SearchResult &l, const SearchResult &r)
    {
        return l.distance < r.distance;
    }
};

class geo
{
public:
    enum class SortType
    {
        random = 0,
        nearest = 1
    };

public:
    geo(dsn::string_view config_file,
        dsn::string_view cluster_name,
        dsn::string_view common_app_name,
        dsn::string_view geo_app_name,
        latlng_extractor &&extractor);

    int set(const std::string &hash_key,
            const std::string &sort_key,
            const std::string &value,
            int timeout_milliseconds = 5000,
            int ttl_seconds = 0,
            pegasus_client::internal_info *info = nullptr);

    int set_geo_data(const std::string &hash_key,
                     const std::string &sort_key,
                     const std::string &value,
                     int timeout_milliseconds = 5000,
                     int ttl_seconds = 0);

    int search_radial(double lat_degrees,
                      double lng_degrees,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_milliseconds,
                      std::list<SearchResult> &result);

    int search_radial(const std::string &hash_key,
                      const std::string &sort_key,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_milliseconds,
                      std::list<SearchResult> &result);

private:
    int search_radial(const S2LatLng &latlng,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_milliseconds,
                      std::list<SearchResult> &result);
    void combine_keys(const std::string &hash_key,
                      const std::string &sort_key,
                      std::string &combine_key);
    int
    extract_keys(const std::string &combine_sort_key, std::string &hash_key, std::string &sort_key);
    std::string get_sort_key(const S2CellId &max_level_cid, const std::string &hash_key);
    int set_common_data(const std::string &hash_key,
                        const std::string &sort_key,
                        const std::string &value,
                        int timeout_milliseconds,
                        int ttl_seconds,
                        pegasus_client::internal_info *info);
    int set_geo_data(const S2LatLng &latlng,
                     const std::string &combine_key,
                     const std::string &value,
                     int timeout_milliseconds,
                     int ttl_seconds);
    int scan_next(const S2LatLng &center,
                  util::units::Meters radius,
                  int count,
                  dsn::task_tracker *tracker,
                  const pegasus_client::pegasus_scanner_wrapper &wrap_scanner,
                  std::vector<SearchResult> &result);
    void scan_data(const std::string &hash_key,
                   const std::string &start_sort_key,
                   const std::string &stop_sort_key,
                   const S2LatLng &center,
                   util::units::Meters radius,
                   int count,
                   dsn::task_tracker *tracker,
                   std::vector<SearchResult> &result);

private:
    // edge length at about 2km, cell id at this level is hash-key in pegasus
    // `min_level` is immutable after geo data has been insert to DB.
    static const int min_level = 12;
    // edge length at about 150m, cell id at this level is prefix of sort-key in pegasus, and
    // convenient for scan operation
    // `max_level` is mutable at any time, and geo-lib users can change it to a appropriate value to
    // improve performance in their scenario.
    static const int max_level = 16;
    static const int max_retry_times = 5;

    latlng_extractor _extractor;
    pegasus_client *_common_data_client = nullptr;
    pegasus_client *_geo_data_client = nullptr;
};

} // namespace pegasus
