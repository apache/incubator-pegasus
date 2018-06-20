// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <sstream>
#include <s2/s2latlng_rect.h>
#include <s2/util/units/length-units.h>
#include <dsn/tool-api/task_tracker.h>
#include <pegasus/client.h>
#include <s2/s2cell_union.h>
#include "latlng_extractor.h"

namespace pegasus {
namespace geo {

struct SearchResult;
using geo_search_callback_t =
    std::function<void(int error_code, std::list<SearchResult> &&results)>;
using distance_callback_t = std::function<void(int error_code, double distance)>;

/// the search result structure used by `search_radial` APIs
struct SearchResult
{
    double lat_degrees; // latitude and longitude extract by `latlng_extractor`, in degree
    double lng_degrees;
    double distance;      // distance from the input and the result, in meter
    std::string hash_key; // the original hash_key, sort_key, and value when data inserted
    std::string sort_key;
    std::string value;
    std::string cellid;

    explicit SearchResult(double lat = 0.0,
                          double lng = 0.0,
                          double dis = 0.0,
                          std::string &&hk = "",
                          std::string &&sk = "",
                          std::string &&v = "",
                          std::string &&cid = "")
        : lat_degrees(lat),
          lng_degrees(lng),
          distance(dis),
          hash_key(std::move(hk)),
          sort_key(std::move(sk)),
          value(std::move(v)),
          cellid(std::move(cid))
    {
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << "[" << hash_key << " : " << sort_key << " => " << value << ", (" << lat_degrees
           << ", " << lng_degrees << "): " << distance << ", " << cellid << "]";
        return std::move(ss.str());
    }
};

/// geo_client is the class for users to operate geometry data on pegasus
/// geo_client use two separate apps on the same cluster, one for common origin data, the
/// other for geometry data
/// !!!NOTE: API operations on the two separate apps are not atomic!!!
/// we use S2Geometry as the underlying library to calculate geometry data, see more:
/// http://s2geometry.io
class geo_client
{
public:
    enum class SortType
    {
        random = 0,
        asc = 1,
        desc = 2,
    };

public:
    /// REQUIRES: app/table `common_app_name` and `geo_app_name` have been created on cluster
    /// `cluster_name`
    geo_client(const char *config_file,
               const char *cluster_name,
               const char *common_app_name,
               const char *geo_app_name,
               latlng_extractor *extractor);

    ///
    /// \brief set
    ///     store the k-v to the cluster, both app/table `common_app_name` and `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to put this k-v
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param value
    ///     the value we want to store.
    /// \param timeout_milliseconds
    ///     if wait longer than this value, will return time out error
    /// \param ttl_seconds
    ///     time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_extractor
    int set(const std::string &hash_key,
            const std::string &sort_key,
            const std::string &value,
            int timeout_milliseconds = 5000,
            int ttl_seconds = 0,
            pegasus_client::internal_info *info = nullptr);

    void async_set(const std::string &hash_key,
                   const std::string &sort_key,
                   const std::string &value,
                   pegasus_client::async_set_callback_t &&callback = nullptr,
                   int timeout_milliseconds = 5000,
                   int ttl_seconds = 0);

    ///
    /// \brief del
    ///     remove the k-v from the cluster, both app/table `common_app_name` and `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to put this k-v
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param timeout_milliseconds
    ///     if wait longer than this value, will return time out error
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    int del(const std::string &hash_key,
            const std::string &sort_key,
            int timeout_milliseconds = 5000,
            pegasus_client::internal_info *info = nullptr);

    void async_del(const std::string &hash_key,
                   const std::string &sort_key,
                   pegasus_client::async_del_callback_t &&callback = nullptr,
                   int timeout_milliseconds = 5000);

    ///
    /// \brief set_geo_data
    ///     store the k-v to the cluster, only app/table `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to put this k-v
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param value
    ///     the value we want to store.
    /// \param timeout_milliseconds
    ///     if wait longer than this value, will return time out error
    /// \param ttl_seconds
    ///     time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_extractor
    int set_geo_data(const std::string &hash_key,
                     const std::string &sort_key,
                     const std::string &value,
                     int timeout_milliseconds = 5000,
                     int ttl_seconds = 0);

    void async_set_geo_data(const std::string &hash_key,
                            const std::string &sort_key,
                            const std::string &value,
                            pegasus_client::async_set_callback_t &&callback = nullptr,
                            int timeout_milliseconds = 5000,
                            int ttl_seconds = 0);

    ///
    /// \brief search_radial
    ///     search data from app/table `geo_app_name`, the results are `radius_m` meters far from
    ///     the (lat_degrees, lng_degrees).
    /// \param lat_degrees
    ///     latitude in degree, range in [-90.0, 90.0]
    /// \param lng_degrees
    ///     longitude in degree, range in [-180.0, 180.0]
    /// \param radius_m
    ///     the results are limited by its distance from the (lat_degrees, lng_degrees).
    /// \param count
    ///     limit results count
    /// \param sort_type
    ///     results sorted type
    /// \param timeout_milliseconds
    ///     if wait longer than this value, will return time out error
    /// \param result
    ///     results container
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    int search_radial(double lat_degrees,
                      double lng_degrees,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_milliseconds,
                      std::list<SearchResult> &result);

    void async_search_radial(double lat_degrees,
                             double lng_degrees,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_milliseconds,
                             geo_search_callback_t &&callback);

    ///
    /// \brief search_radial
    ///     search data from app/table `geo_app_name`, the results are `radius_m` meters far from
    ///     the (lat_degrees, lng_degrees).
    /// \param hash_key
    ///     used to decide which partition to get this k-v
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param radius_m
    ///     the results are limited by its distance from the (lat_degrees, lng_degrees).
    /// \param count
    ///     limit results count
    /// \param sort_type
    ///     results sorted type
    /// \param timeout_milliseconds
    ///     if wait longer than this value, will return time out error
    /// \param result
    ///     results container
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
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

    void async_search_radial(const std::string &hash_key,
                             const std::string &sort_key,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_milliseconds,
                             geo_search_callback_t &&callback);

    ///
    /// \brief distance
    ///     get the distance of the two given keys
    /// \param hash_key1, hash_key2
    ///     used to decide which partition to get this k-v
    /// \param sort_key1, sort_key2
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param distance
    ///     the returned distance of the two given keys.
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    int distance(const std::string &hash_key1,
                 const std::string &sort_key1,
                 const std::string &hash_key2,
                 const std::string &sort_key2,
                 int timeout_milliseconds,
                 double &distance);

    void async_distance(const std::string &hash_key1,
                        const std::string &sort_key1,
                        const std::string &hash_key2,
                        const std::string &sort_key2,
                        int timeout_milliseconds,
                        distance_callback_t &&callback);

    const char *get_error_string(int error_code) const
    {
        return _common_data_client->get_error_string(error_code);
    }

private:
    friend class geo_client_test;

    enum class DataType
    {
        common = 0,
        geo = 1
    };

    using update_callback_t = std::function<void(
        int error_code, pegasus_client::internal_info &&info, DataType data_type)>;
    using scan_all_area_callback_t =
        std::function<void(std::list<std::vector<SearchResult>> &&results)>;
    using scan_one_area_callback = std::function<void()>;

    // generate hash_key and sort_key in geo database from hash_key and sort_key in common data
    // database
    // geo hash_key is the prefix of cell id which is calculated from value by `_extractor`, its
    // length is associated with `_min_level`
    // geo sort_key is composed with the postfix of the same cell id and origin hash_key and
    // sort_key
    bool generate_geo_keys(const std::string &hash_key,
                           const std::string &sort_key,
                           const std::string &value,
                           std::string &geo_hash_key,
                           std::string &geo_sort_key);

    bool restore_origin_keys(const std::string &geo_sort_key,
                             std::string &origin_hash_key,
                             std::string &origin_sort_key);

    void async_set_common_data(const std::string &hash_key,
                               const std::string &sort_key,
                               const std::string &value,
                               update_callback_t &&callback,
                               int timeout_milliseconds,
                               int ttl_seconds);

    void async_set_geo_data(const std::string &hash_key,
                            const std::string &sort_key,
                            const std::string &value,
                            update_callback_t &&callback = nullptr,
                            int timeout_milliseconds = 5000,
                            int ttl_seconds = 0);

    void async_del_common_data(const std::string &hash_key,
                               const std::string &sort_key,
                               update_callback_t &&callback,
                               int timeout_milliseconds);

    void async_del_geo_data(const std::string &hash_key,
                            const std::string &sort_key,
                            update_callback_t &&callback,
                            int timeout_milliseconds);

    void async_search_radial(const S2LatLng &latlng,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_milliseconds,
                             geo_search_callback_t &&callback);

    // generate a cap by center point and radius
    void gen_search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap);

    // generate cell ids covered by the cap on a pre-defined level
    void gen_cells_covered_by_cap(const S2Cap &cap, S2CellUnion &cids);

    // search data covered by `cap` in all `cids`
    void async_get_result_from_cells(const S2CellUnion &cids,
                                     const S2Cap &cap,
                                     int count,
                                     SortType sort_type,
                                     scan_all_area_callback_t &&callback);

    // normalize the result by count, sort type, ...
    void normalize_result(const std::list<std::vector<SearchResult>> &results,
                          int count,
                          SortType sort_type,
                          std::list<SearchResult> &result);

    // generate sort key of `max_level_cid` under `hash_key`
    std::string gen_sort_key(const S2CellId &max_level_cid, const std::string &hash_key);
    // generate start sort key of `max_level_cid` under `hash_key`
    std::string gen_start_sort_key(const S2CellId &max_level_cid, const std::string &hash_key);
    // generate stop sort key of `max_level_cid` under `hash_key`
    std::string gen_stop_sort_key(const S2CellId &max_level_cid, const std::string &hash_key);

    void start_scan(const std::string &hash_key,
                    const std::string &start_sort_key,
                    const std::string &stop_sort_key,
                    const S2Cap &cap,
                    int count,
                    scan_one_area_callback cb,
                    std::vector<SearchResult> &result);

    void do_scan(pegasus_client::pegasus_scanner_wrapper scanner_wrapper,
                 const S2Cap &cap,
                 int count,
                 scan_one_area_callback cb,
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
    int _max_level = 16;

    std::shared_ptr<const latlng_extractor> _extractor = nullptr;
    pegasus_client *_common_data_client = nullptr;
    pegasus_client *_geo_data_client = nullptr;
};

} // namespace geo
} // namespace pegasus
