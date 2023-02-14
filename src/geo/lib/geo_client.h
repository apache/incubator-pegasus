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

#include <sstream>
#include <s2/s2latlng_rect.h>
#include <s2/s2cell_union.h>
#include <s2/util/units/length-units.h>
#include "runtime/task/task_tracker.h"
#include <pegasus/client.h>
#include "latlng_codec.h"

namespace dsn {
class error_s;
} // namespace dsn

namespace pegasus {
namespace geo {

struct SearchResult;
using geo_search_callback_t =
    std::function<void(int error_code, std::list<SearchResult> &&results)>;
using distance_callback_t = std::function<void(int error_code, double distance)>;
using get_latlng_callback_t =
    std::function<void(int error_code, int id, double lat_degrees, double lng_degrees)>;

/// the search result structure used by `search_radial` APIs
struct SearchResult
{
    double lat_degrees; // latitude and longitude extract by `latlng_codec`, in degree
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
        return ss.str();
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
               const char *geo_app_name);

    ~geo_client() { _tracker.wait_outstanding_tasks(); }

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
    /// \param timeout_ms
    ///     if wait longer than this value, will return time out error
    /// \param ttl_seconds
    ///     time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_codec
    int set(const std::string &hash_key,
            const std::string &sort_key,
            const std::string &value,
            int timeout_ms = 5000,
            int ttl_seconds = 0,
            pegasus_client::internal_info *info = nullptr);

    void async_set(const std::string &hash_key,
                   const std::string &sort_key,
                   const std::string &value,
                   pegasus_client::async_set_callback_t &&callback = nullptr,
                   int timeout_ms = 5000,
                   int ttl_seconds = 0);

    void async_set(const std::string &hash_key,
                   const std::string &sort_key,
                   double lat_degrees,
                   double lng_degrees,
                   pegasus_client::async_set_callback_t &&callback = nullptr,
                   int timeout_ms = 5000,
                   int ttl_seconds = 0);

    ///
    /// \brief get
    ///     get latitude and longitude of key pair from the cluster
    ///     key pair is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to get value
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key
    /// \param lat_degrees
    ///     latitude in degree of this key
    /// \param lng_degrees
    ///     longitude in degree of this key
    /// \param timeout_ms
    ///     if wait longer than this value, will return timeout error
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: value of this key can be correctly extracted to latitude and longitude by
    /// latlng_codec
    int get(const std::string &hash_key,
            const std::string &sort_key,
            double &lat_degrees,
            double &lng_degrees,
            int timeout_ms = 5000);

    ///
    /// \brief async_get
    ///     get latitude and longitude of key pair from the cluster asynchronous
    ///     key pair is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to get value
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key
    /// \param id
    ///     to distinguish different calls
    /// \param callback
    ///     callback function either success or not
    /// \param timeout_ms
    ///     if wait longer than this value, will return timeout error
    ///
    /// REQUIRES: value of this key can be correctly extracted to latitude and longitude by
    /// latlng_codec
    void async_get(const std::string &hash_key,
                   const std::string &sort_key,
                   int id,
                   get_latlng_callback_t &&callback = nullptr,
                   int timeout_ms = 5000);

    ///
    /// \brief del
    ///     remove the k-v from the cluster, both app/table `common_app_name` and `geo_app_name`
    ///     key is composed of hash_key and sort_key.
    /// \param hash_key
    ///     used to decide which partition to put this k-v
    /// \param sort_key
    ///     all the k-v under hash_key will be sorted by sort_key.
    /// \param keep_common_data
    ///     only delete geo data, keep common data.
    /// \param timeout_ms
    ///     if wait longer than this value, will return time out error
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    int del(const std::string &hash_key,
            const std::string &sort_key,
            int timeout_ms = 5000,
            pegasus_client::internal_info *info = nullptr);

    void async_del(const std::string &hash_key,
                   const std::string &sort_key,
                   bool keep_common_data,
                   pegasus_client::async_del_callback_t &&callback = nullptr,
                   int timeout_ms = 5000);

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
    /// \param timeout_ms
    ///     if wait longer than this value, will return time out error
    /// \param ttl_seconds
    ///     time to live of this value, if expired, will return not found; 0 means no ttl
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted from `value` by latlng_codec
    int set_geo_data(const std::string &hash_key,
                     const std::string &sort_key,
                     const std::string &value,
                     int timeout_ms = 5000,
                     int ttl_seconds = 0);

    void async_set_geo_data(const std::string &hash_key,
                            const std::string &sort_key,
                            const std::string &value,
                            pegasus_client::async_set_callback_t &&callback = nullptr,
                            int timeout_ms = 5000,
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
    /// \param timeout_ms
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
                      int timeout_ms,
                      std::list<SearchResult> &result);

    void async_search_radial(double lat_degrees,
                             double lng_degrees,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_ms,
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
    /// \param timeout_ms
    ///     if wait longer than this value, will return time out error
    /// \param result
    ///     results container
    /// \return
    ///     int, the error indicates whether or not the operation is succeeded.
    /// this error can be converted to a string using get_error_string()
    ///
    /// REQUIRES: latitude and longitude can be correctly extracted by latlng_codec from the
    /// value corresponding to `hash_key` and `sort_key`
    int search_radial(const std::string &hash_key,
                      const std::string &sort_key,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      int timeout_ms,
                      std::list<SearchResult> &result);

    void async_search_radial(const std::string &hash_key,
                             const std::string &sort_key,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_ms,
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
                 int timeout_ms,
                 double &distance);

    void async_distance(const std::string &hash_key1,
                        const std::string &sort_key1,
                        const std::string &hash_key2,
                        const std::string &sort_key2,
                        int timeout_ms,
                        distance_callback_t &&callback);

    const char *get_error_string(int error_code) const
    {
        return _common_data_client->get_error_string(error_code);
    }

    dsn::error_s set_max_level(int level);

    // For test.
    const latlng_codec &get_codec() const { return _codec; }

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
        std::function<void(std::list<std::list<SearchResult>> &&results)>;
    using scan_one_area_callback_t = std::function<void()>;

    // generate hash_key and sort_key in geo database from hash_key and sort_key in common data
    // database
    // geo hash_key is the prefix of cell id which is calculated from value by `_codec`, its
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
                               int timeout_ms,
                               int ttl_seconds);

    void async_set_geo_data(const std::string &hash_key,
                            const std::string &sort_key,
                            const std::string &value,
                            update_callback_t &&callback = nullptr,
                            int timeout_ms = 5000,
                            int ttl_seconds = 0);

    void async_del_common_data(const std::string &hash_key,
                               const std::string &sort_key,
                               update_callback_t &&callback,
                               int timeout_ms);

    void async_del_geo_data(const std::string &geo_hash_key,
                            const std::string &geo_sort_key,
                            update_callback_t &&callback,
                            int timeout_ms);

    void async_search_radial(const S2LatLng &latlng,
                             double radius_m,
                             int count,
                             SortType sort_type,
                             int timeout_ms,
                             geo_search_callback_t &&callback);

    // generate a cap by center point and radius
    void gen_search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap);

    // generate cell ids covered by the cap on a pre-defined level
    void gen_cells_covered_by_cap(const S2Cap &cap, S2CellUnion &cids);

    // search data covered by `cap` in all `cids`
    void async_get_result_from_cells(const S2CellUnion &cids,
                                     std::shared_ptr<S2Cap> cap_ptr,
                                     int count,
                                     SortType sort_type,
                                     int timeout_ms,
                                     scan_all_area_callback_t &&callback);

    // normalize the result by count, sort type, ...
    void normalize_result(std::list<std::list<SearchResult>> &&results,
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
                    std::string &&start_sort_key,
                    std::string &&stop_sort_key,
                    std::shared_ptr<S2Cap> cap_ptr,
                    int count,
                    int timeout_ms,
                    scan_one_area_callback_t &&callback,
                    std::list<SearchResult> &result);

    void do_scan(pegasus_client::pegasus_scanner_wrapper scanner_wrapper,
                 std::shared_ptr<S2Cap> cap_ptr,
                 int count,
                 scan_one_area_callback_t &&callback,
                 std::list<SearchResult> &result);

private:
    dsn::task_tracker _tracker;

    latlng_codec _codec;
    pegasus_client *_common_data_client = nullptr;
    pegasus_client *_geo_data_client = nullptr;
};

} // namespace geo
} // namespace pegasus
