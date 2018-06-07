// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <sstream>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/util/units/length-units.h>
#include <dsn/tool-api/task_tracker.h>
#include <pegasus/client.h>
#include <s2/s2cell_union.h>

namespace pegasus {

/// a user define function to extract latitude and longitude from a std::string type value
/// for example, if we have a value format like:
/// "00:00:00:00:01:5e|2018-04-26 23:59:59|2018-04-28
/// 03:00:00|ezp8xchrr|-0.356396|39.469644|24.043028|4.15921|0|-1"
/// we can define the extractor like this:
///    auto extractor = [](const std::string &value, S2LatLng &latlng) {
///        std::vector<std::string> data;
///        dsn::utils::split_args(value.c_str(), data, '|');
///        if (data.size() <= 6) {
///            return pegasus::PERR_INVALID_VALUE;
///        }
///
///        std::string lat = data[4];
///        std::string lng = data[5];
///        latlng = S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr), strtod(lng.c_str(),
///        nullptr));
///
///        return pegasus::PERR_OK;
///    };
using latlng_extractor = std::function<int(const std::string &value, S2LatLng &latlng)>;

/// search result structure when use `search_radial` APIs
struct SearchResult
{
    double lat_degrees; // latitude and longitude extract by `latlng_extractor`, in degree
    double lng_degrees;
    double distance;      // distance from the input and the result, in meter
    std::string hash_key; // the original hash_key, sort_key, and value when data inserted
    std::string sort_key;
    std::string value;

    explicit SearchResult(double lat = 0.0,
                          double lng = 0.0,
                          double dis = 0.0,
                          std::string &&hk = "",
                          std::string &&sk = "",
                          std::string &&v = "")
        : lat_degrees(lat), lng_degrees(lng), distance(dis), hash_key(hk), sort_key(sk), value(v)
    {
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << "[" << hash_key << " : " << sort_key << " => " << value << ", (" << lat_degrees
           << ", " << lng_degrees << "): " << distance << "]";
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

/// geo_client is the class for users to operate geometry data on pegasus
/// geo_client use two separate apps/tables on the same cluster, one for common data, the other for
/// geometry data
/// we use S2Geometry as the underlying library to calculate geometry data, see more:
/// http://s2geometry.io/
class geo_client
{
public:
    enum class SortType
    {
        random = 0,
        nearest = 1 // search results will be sorted by distance from the input point
    };

public:
    /// REQUIRES: app/table `common_app_name` and `geo_app_name` have been created on cluster
    /// `cluster_name`
    geo_client(const char *config_file,
               const char *cluster_name,
               const char *common_app_name,
               const char *geo_app_name,
               latlng_extractor &&extractor);

    ///
    /// \brief set
    ///     store the k-v to the cluster, both app/table `common_app_name` and `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    /// used to decide which partition to put this k-v
    /// \param sort_key
    /// all the k-v under hash_key will be sorted by sort_key.
    /// \param value
    /// the value we want to store.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_extractor
    int set(const std::string &hash_key,
            const std::string &sort_key,
            const std::string &value,
            int timeout_milliseconds = 5000,
            int ttl_seconds = 0,
            pegasus_client::internal_info *info = nullptr);

    ///
    /// \brief set_geo_data
    ///     store the k-v to the cluster, only app/table `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    /// used to decide which partition to put this k-v
    /// \param sort_key
    /// all the k-v under hash_key will be sorted by sort_key.
    /// \param value
    /// the value we want to store.
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param ttl_seconds
    /// time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_extractor
    int set_geo_data(const std::string &hash_key,
                     const std::string &sort_key,
                     const std::string &value,
                     int timeout_milliseconds = 5000,
                     int ttl_seconds = 0);

    ///
    /// \brief search_radial
    ///     search data from app/table `geo_app_name`, the results are `radius_m` meters far from
    ///     the (lat_degrees, lng_degrees).
    /// \param lat_degrees
    /// latitude in degree, range in [-90.0, 90.0]
    /// \param lng_degrees
    /// longitude in degree, range in [-180.0, 180.0]
    /// \param radius_m
    /// the results are limited by its distance from the (lat_degrees, lng_degrees).
    /// \param count
    /// limit results count
    /// \param sort_type
    /// results sorted type
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param result
    /// results container
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    int search_radial(double lat_degrees,
                      double lng_degrees,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_milliseconds,
                      std::list<SearchResult> &result);

    ///
    /// \brief search_radial
    ///     search data from app/table `geo_app_name`, the results are `radius_m` meters far from
    ///     the (lat_degrees, lng_degrees).
    /// \param hash_key
    /// used to decide which partition to get this k-v
    /// \param sort_key
    /// all the k-v under hash_key will be sorted by sort_key.
    /// \param radius_m
    /// the results are limited by its distance from the (lat_degrees, lng_degrees).
    /// \param count
    /// limit results count
    /// \param sort_type
    /// results sorted type
    /// \param timeout_milliseconds
    /// if wait longer than this value, will return time out error
    /// \param result
    /// results container
    /// \return
    /// int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted by latlng_extractor from the
    /// value corresponding to `hash_key` and `sort_key`
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
    void search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap);
    void get_covering_cells(const S2Cap &cap, S2CellUnion &cids);
    void get_result_from_cells(const S2CellUnion &cids,
                               const S2Cap &cap,
                               int count,
                               SortType sort_type,
                               std::list<std::vector<SearchResult>> &results);
    void normalize_result(const std::list<std::vector<SearchResult>> &results,
                          int count,
                          SortType sort_type,
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
    int scan_next(const S2Cap &cap,
                  int count,
                  dsn::task_tracker *tracker,
                  const pegasus_client::pegasus_scanner_wrapper &wrap_scanner,
                  std::vector<SearchResult> &result);
    void scan_data(const std::string &hash_key,
                   const std::string &start_sort_key,
                   const std::string &stop_sort_key,
                   const S2Cap &cap,
                   int count,
                   dsn::task_tracker *tracker,
                   std::vector<SearchResult> &result);

private:
    // cell id at this level is the hash-key in pegasus
    // `_min_level` is immutable after geo_client data has been inserted into DB.
    const int _min_level = 12; // edge length at level 12 is about 2km

    // cell id at this level is the prefix of sort-key in pegasus, and
    // it's convenient for scan operation
    // `_max_level` is mutable at any time, and geo_client-lib users can change it to a appropriate
    // value
    // to improve performance in their scenario.
    int _max_level;

    // max retry times when insert data into db failed
    int _max_retry_times;

    latlng_extractor _extractor;
    pegasus_client *_common_data_client = nullptr;
    pegasus_client *_geo_data_client = nullptr;
};

} // namespace pegasus
