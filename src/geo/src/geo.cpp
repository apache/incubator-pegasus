// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo.h"

#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>
#include <s2/s2earth.h>
#include <s2/s2testing.h>
#include <s2/s2region_coverer.h>
#include <s2/s2cap.h>
#include <base/pegasus_key_schema.h>

namespace pegasus {

DEFINE_TASK_CODE(LPC_SCAN_DATA, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

geo::geo(dsn::string_view config_file,
         dsn::string_view cluster_name,
         dsn::string_view common_app_name,
         dsn::string_view geo_app_name,
         latlng_extractor &&extractor)
{
    bool ok = pegasus_client_factory::initialize(config_file.data());
    dassert(ok, "init pegasus client factory failed");

    _common_data_client =
        pegasus_client_factory::get_client(cluster_name.data(), common_app_name.data());
    dassert(_common_data_client != nullptr, "init pegasus _common_data_client failed");

    _geo_data_client = pegasus_client_factory::get_client(cluster_name.data(), geo_app_name.data());
    dassert(_geo_data_client != nullptr, "init pegasus _geo_data_client failed");

    _extractor = extractor;
}

int geo::set(const std::string &hash_key,
             const std::string &sort_key,
             const std::string &value,
             int timeout_milliseconds,
             int ttl_seconds,
             pegasus_client::internal_info *info)
{
    // TODO 异步并行set
    int ret =
        set_common_data(hash_key, sort_key, value, timeout_milliseconds / 2, ttl_seconds, info);
    if (ret != PERR_OK) {
        derror_f("set_common_data failed. hash_key={} sort_key={}", hash_key, sort_key);
        return ret;
    }

    ret = set_geo_data(hash_key, sort_key, value, timeout_milliseconds / 2, ttl_seconds);
    if (ret != PERR_OK) {
        derror_f("set_geo_data failed. hash_key={} sort_key={}", hash_key, sort_key);
        return ret;
    }

    return PERR_OK;
}

int geo::set_geo_data(const std::string &hash_key,
                      const std::string &sort_key,
                      const std::string &value,
                      int timeout_milliseconds,
                      int ttl_seconds)
{
    S2LatLng latlng;
    int ret = _extractor(value, latlng);
    if (ret != PERR_OK) {
        derror_f("_extractor failed. value={}", value);
        return ret;
    }

    std::string combine_key;
    combine_keys(hash_key, sort_key, combine_key);

    ret = set_geo_data(latlng, combine_key, value, timeout_milliseconds / 2, ttl_seconds);
    if (ret != PERR_OK) {
        derror_f("set_geo_data failed. hash_key={}, sort_key={}", hash_key, sort_key);
        return ret;
    }

    return ret;
}

int geo::search_radial(double lat_degrees,
                       double lng_degrees,
                       double radius_m,
                       int count,
                       SortType sort_type,
                       int timeout_milliseconds,
                       std::list<SearchResult> &result)
{
    util::units::Meters radius((float)radius_m);
    return search_radial(S2LatLng::FromDegrees(lat_degrees, lng_degrees),
                         radius_m,
                         count,
                         sort_type,
                         timeout_milliseconds,
                         result);
}

int geo::search_radial(const std::string &hash_key,
                       const std::string &sort_key,
                       double radius_m,
                       int count,
                       SortType sort_type,
                       int timeout_milliseconds,
                       std::list<SearchResult> &result)
{
    std::string value;
    int ret =
        _common_data_client->get(hash_key, sort_key, value, (int)(timeout_milliseconds * 0.2));
    if (ret != pegasus::PERR_OK) {
        derror_f("get failed, error={}", _common_data_client->get_error_string(ret));
        return ret;
    }

    S2LatLng latlng;
    ret = _extractor(value, latlng);
    if (ret != PERR_OK) {
        derror_f("_extractor failed. value={}", value);
        return ret;
    }

    return search_radial(
        latlng, radius_m, count, sort_type, (int)(timeout_milliseconds * 0.8), result);
}

int geo::search_radial(const S2LatLng &latlng,
                       double radius_m,
                       int count,
                       SortType sort_type,
                       int timeout_milliseconds,
                       std::list<SearchResult> &result)
{
    S2Cap cap;
    search_cap(latlng, radius_m, cap);

    S2CellUnion cids;
    get_covering_cells(cap, cids);

    // each scan result store in a separate vector, we will combine all the results finally
    std::list<std::vector<SearchResult>> results;
    get_result_from_cells(cids, cap, count, sort_type, results);

    normalize_result(results, count, sort_type, result);

    return PERR_OK;
}

void geo::search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap)
{
    // construct a cap by center point and radius
    util::units::Meters radius((float)radius_m);
    cap = S2Cap(latlng.ToPoint(), S2Earth::ToAngle(radius));
}

void geo::get_covering_cells(const S2Cap &cap, S2CellUnion &cids)
{
    // calculate all the cells covered by the cap at `min_level`
    S2RegionCoverer rc;
    rc.mutable_options()->set_fixed_level(min_level);
    cids = rc.GetCovering(cap);
}

void geo::get_result_from_cells(const S2CellUnion &cids,
                                const S2Cap &cap,
                                int count,
                                SortType sort_type,
                                std::list<std::vector<SearchResult>> &results)
{
    int single_scan_count = count;
    if (sort_type == SortType::nearest) {
        single_scan_count = -1; // scan all data to make full sort
    }

    // scan each cell
    dsn::task_tracker tracker;
    for (const auto &cid : cids) {
        if (cap.Contains(S2Cell(cid))) {
            // for the full contained cell, scan all data in this cell(at `min_level`)
            results.emplace_back(std::vector<SearchResult>());
            scan_data(cid.ToString(), "", "", cap, single_scan_count, &tracker, results.back());
        } else {
            // for the partial contained cell, scan cells covered by the cap at `max_level` which is
            // more accurate than the ones at `min_level`
            std::string hash_key = cid.parent(min_level).ToString();
            std::pair<std::string, std::string> start_stop_keys;
            S2CellId pre;
            for (S2CellId cur = cid.child_begin(max_level); cur != cid.child_end(max_level);
                 cur = cur.next()) {
                if (cap.MayIntersect(S2Cell(cur))) {
                    // only cells whose any vertex is contained by the cap is needed
                    if (!pre.is_valid()) {
                        // `cur` is the very first cell in Hilbert curve and contained by the cap
                        pre = cur;
                        start_stop_keys.first = get_sort_key(pre, hash_key);
                    } else {
                        if (pre.next() != cur) {
                            // `pre` is the last cell in Hilbert curve and contained by the cap
                            // `cur` is a new start cell in Hilbert curve and contained by the cap
                            start_stop_keys.second = get_sort_key(pre, hash_key);
                            results.emplace_back(std::vector<SearchResult>());
                            scan_data(hash_key,
                                      start_stop_keys.first,
                                      start_stop_keys.second,
                                      cap,
                                      single_scan_count,
                                      &tracker,
                                      results.back());

                            start_stop_keys.first = get_sort_key(cur, hash_key);
                            start_stop_keys.second.clear();
                        }
                        pre = cur;
                    }
                }
            }

            // edge case: when the cell is the last one in Hilbert curve in current `min_level` cell
            if (start_stop_keys.second.empty()) {
                start_stop_keys.second = start_stop_keys.first;
                results.emplace_back(std::vector<SearchResult>());
                scan_data(hash_key,
                          start_stop_keys.first,
                          start_stop_keys.second,
                          cap,
                          single_scan_count,
                          &tracker,
                          results.back());
            }
        }
    }
    tracker.cancel_outstanding_tasks();
}

void geo::normalize_result(const std::list<std::vector<SearchResult>> &results,
                           int count,
                           SortType sort_type,
                           std::list<SearchResult> &result)
{
    for (auto &r : results) {
        result.insert(result.end(), r.begin(), r.end());
        if (sort_type == SortType::random && count > 0 && result.size() >= count) {
            break;
        }
    }
    if (sort_type == SortType::nearest) {
        std::priority_queue<SearchResult, std::vector<SearchResult>, SearchResultNearer>
            nearest_result;
        for (const auto &r : result) {
            nearest_result.emplace(r);
            if (nearest_result.size() > count) {
                nearest_result.pop();
            }
        }

        result.clear();
        while (!nearest_result.empty()) {
            result.emplace_front(nearest_result.top());
            nearest_result.pop();
        }
    } else if (count > 0) {
        result.resize((size_t)count);
    }
}

void geo::combine_keys(const std::string &hash_key,
                       const std::string &sort_key,
                       std::string &combine_key)
{
    dsn::blob blob_combine_key;
    pegasus_generate_key(blob_combine_key, hash_key, sort_key);
    combine_key = std::move(blob_combine_key.to_string());
}

int geo::extract_keys(const std::string &combine_sort_key,
                      std::string &hash_key,
                      std::string &sort_key)
{
    // combine_sort_key: [0,3]{30-min_level}:combine_keys
    unsigned int leaf_cell_length = 30 - min_level + 1;
    if (combine_sort_key.length() <= leaf_cell_length) {
        return PERR_INVALID_VALUE;
    }

    auto combine_key_len = static_cast<unsigned int>(combine_sort_key.length() - leaf_cell_length);
    pegasus_restore_key(
        dsn::blob(combine_sort_key.c_str(), leaf_cell_length, combine_key_len), hash_key, sort_key);

    return PERR_OK;
}

std::string geo::get_sort_key(const S2CellId &max_level_cid, const std::string &hash_key)
{
    return max_level_cid.ToString().substr(hash_key.length());
}

int geo::set_common_data(const std::string &hash_key,
                         const std::string &sort_key,
                         const std::string &value,
                         int timeout_milliseconds,
                         int ttl_seconds,
                         pegasus_client::internal_info *info)
{
    int ret;
    unsigned int retry_times = 0;
    do {
        if (retry_times > 0) {
            dwarn_f("retry set data. sleep {}ms", retry_times * 10);
            usleep(retry_times * 10 * 1000);
        }
        ret = _common_data_client->set(
            hash_key, sort_key, value, timeout_milliseconds, ttl_seconds, info);
    } while (ret != PERR_OK && retry_times++ < max_retry_times);

    return ret;
}

int geo::set_geo_data(const S2LatLng &latlng,
                      const std::string &combine_key,
                      const std::string &value,
                      int timeout_milliseconds,
                      int ttl_seconds)
{
    // leaf cell
    S2CellId leaf_cell_id = S2Cell(latlng).id();

    // convert to a parent level cell
    S2CellId parent_cell_id = leaf_cell_id.parent(min_level);

    std::string hash_key(parent_cell_id.ToString()); // [0,5]{1}/[0,3]{min_level}
    std::string sort_key(leaf_cell_id.ToString().substr(hash_key.length()) + ":" +
                         combine_key); // [0,3]{30-min_level}:combine_keys

    int ret;
    unsigned int retry_times = 0;
    do {
        if (retry_times > 0) {
            dwarn_f("retry set geo data. sleep {}ms", retry_times * 10);
            usleep(retry_times * 10 * 1000);
        }
        ret = _geo_data_client->set(hash_key, sort_key, value, timeout_milliseconds, ttl_seconds);
        if (ret != PERR_OK) {
            derror_f("set data failed. error={}", _geo_data_client->get_error_string(ret));
        }

    } while (ret != PERR_OK && retry_times++ < max_retry_times);

    return ret;
}

int geo::scan_next(const S2Cap &cap,
                   int count,
                   dsn::task_tracker *tracker,
                   const pegasus_client::pegasus_scanner_wrapper &wrap_scanner,
                   std::vector<SearchResult> &result)
{
    wrap_scanner->async_next(
        [this, cap, count, tracker, wrap_scanner, &result](int ret,
                                                           std::string &&hash_key,
                                                           std::string &&sort_key,
                                                           std::string &&scan_value,
                                                           pegasus_client::internal_info &&info) {
            if (ret == PERR_SCAN_COMPLETE) {
                return;
            }

            if (ret != PERR_OK) {
                derror_f("async_next failed. error={}", _geo_data_client->get_error_string(ret));
                return;
            }

            S2LatLng latlng;
            if (_extractor(scan_value, latlng) != PERR_OK) {
                derror_f("_extractor failed. scan_value={}", scan_value);
                return;
            }

            util::units::Meters distance = S2Earth::GetDistance(S2LatLng(cap.center()), latlng);
            if (distance <= S2Earth::ToDistance(cap.radius())) {
                std::string origin_hash_key, origin_sort_key;
                if (extract_keys(sort_key, origin_hash_key, origin_sort_key) != PERR_OK) {
                    derror_f("extract_keys failed. sort_key={}", sort_key);
                    return;
                }

                result.emplace_back(SearchResult(latlng.lat().degrees(),
                                                 latlng.lng().degrees(),
                                                 distance.value(),
                                                 std::move(origin_hash_key),
                                                 std::move(origin_sort_key),
                                                 std::move(scan_value)));
            }

            if (count == -1 || result.size() < count) {
                scan_next(cap, count, tracker, wrap_scanner, result);
            }
        });

    return PERR_OK;
}

void geo::scan_data(const std::string &hash_key,
                    const std::string &start_sort_key,
                    const std::string &stop_sort_key,
                    const S2Cap &cap,
                    int count,
                    dsn::task_tracker *tracker,
                    std::vector<SearchResult> &result)
{
    dsn::tasking::enqueue(
        LPC_SCAN_DATA,
        tracker,
        [this, hash_key, start_sort_key, stop_sort_key, cap, count, tracker, &result]() {
            pegasus_client::scan_options options;
            options.start_inclusive = true;
            options.stop_inclusive = true;
            _geo_data_client->async_get_scanner(
                hash_key,
                start_sort_key,
                stop_sort_key,
                options,
                [this, cap, count, tracker, &result](int ret,
                                                     pegasus_client::pegasus_scanner *scanner) {
                    if (ret == PERR_OK) {
                        pegasus_client::pegasus_scanner_wrapper wrap_scanner =
                            scanner->get_smart_wrapper();
                        scan_next(cap, count, tracker, wrap_scanner, result);
                    }
                });

        });
}

} // namespace pegasus