// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo_client.h"

#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>
#include <s2/s2earth.h>
#include <s2/s2testing.h>
#include <s2/s2region_coverer.h>
#include <s2/s2cap.h>
#include <base/pegasus_key_schema.h>

namespace pegasus {
namespace geo {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_GEO)
DEFINE_TASK_CODE(LPC_GEO_SCAN_DATA, TASK_PRIORITY_COMMON, THREAD_POOL_GEO)

geo_client::geo_client(const char *config_file,
                       const char *cluster_name,
                       const char *common_app_name,
                       const char *geo_app_name,
                       latlng_extractor &&extractor)
{
    bool ok = pegasus_client_factory::initialize(config_file);
    dassert(ok, "init pegasus client factory failed");

    _common_data_client = pegasus_client_factory::get_client(cluster_name, common_app_name);
    dassert(_common_data_client != nullptr, "init pegasus _common_data_client failed");

    _geo_data_client = pegasus_client_factory::get_client(cluster_name, geo_app_name);
    dassert(_geo_data_client != nullptr, "init pegasus _geo_data_client failed");

    _extractor = extractor;

    // default: 16. edge length at level 16 is about 150m
    _max_level = (int32_t)dsn_config_get_value_uint64(
        "geo_client.lib", "max_level", 16, "max cell level for scan");
}

int geo_client::set(const std::string &hash_key,
                    const std::string &sort_key,
                    const std::string &value,
                    int timeout_milliseconds,
                    int ttl_seconds,
                    pegasus_client::internal_info *info)
{
    int ret = PERR_OK;
    std::atomic<int> set_count(2);
    dsn::utils::notify_event set_completed;
    auto async_set_callback =
        [&](int ec_, pegasus_client::internal_info &&info_, DataType data_type_) {
            if (data_type_ == DataType::common) {
                if (info != nullptr) {
                    *info = std::move(info_);
                }
            }

            if (ec_ != PERR_OK) {
                derror_f("set {} data failed. hash_key={}, sort_key={}",
                         data_type_ == DataType::common ? "common" : "geo",
                         hash_key,
                         sort_key);
                ret = ec_;
            }

            set_count.fetch_sub(1);
            if (set_count.load() == 0) {
                set_completed.notify();
            }
        };

    async_set_common_data(
        hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
    async_set_geo_data(
        hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);

    set_completed.wait();

    return ret;
}

void geo_client::async_set(const std::string &hash_key,
                           const std::string &sort_key,
                           const std::string &value,
                           pegasus_client::async_set_callback_t &&callback,
                           int timeout_milliseconds,
                           int ttl_seconds)
{
    std::shared_ptr<int> ret(new int(PERR_OK));
    std::shared_ptr<std::atomic<int32_t>> set_count(new std::atomic<int32_t>(2));
    auto async_set_callback = [ hash_key, sort_key, set_count, ret, cb = std::move(callback) ](
        int ec_, pegasus_client::internal_info &&info_, DataType data_type_)
    {
        pegasus_client::internal_info info;
        if (data_type_ == DataType::common) {
            info = std::move(info_);
        }

        if (ec_ != PERR_OK) {
            derror_f("set {} data failed. hash_key={}, sort_key={}",
                     data_type_ == DataType::common ? "common" : "geo",
                     hash_key,
                     sort_key);
            *ret = ec_;
        }

        if (set_count->fetch_sub(1) == 1) {
            cb(*ret, std::move(info));
        }
    };

    async_set_common_data(
        hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
    async_set_geo_data(
        hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
}

int geo_client::set_geo_data(const std::string &hash_key,
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

    dsn::utils::notify_event set_completed;
    auto async_set_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            derror_f("set geo data failed. hash_key={}, sort_key={}", hash_key, sort_key);
        }
        set_completed.notify();
    };
    async_set_geo_data(
        latlng, combine_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);

    set_completed.wait();
    return ret;
}

void geo_client::async_set_geo_data(const std::string &hash_key,
                                    const std::string &sort_key,
                                    const std::string &value,
                                    pegasus_client::async_set_callback_t &&callback,
                                    int timeout_milliseconds,
                                    int ttl_seconds)
{
    async_set_geo_data(
        hash_key,
        sort_key,
        value,
        [cb = std::move(callback)](int error_code,
                                   pegasus_client::internal_info &&info,
                                   DataType data_type) { cb(error_code, std::move(info)); },
        timeout_milliseconds,
        ttl_seconds);
}

int geo_client::search_radial(double lat_degrees,
                              double lng_degrees,
                              double radius_m,
                              int count,
                              SortType sort_type,
                              int timeout_milliseconds,
                              std::list<SearchResult> &result)
{
    int ret = PERR_OK;
    util::units::Meters radius((float)radius_m);
    dsn::utils::notify_event search_completed;
    async_search_radial(S2LatLng::FromDegrees(lat_degrees, lng_degrees),
                        radius_m,
                        count,
                        sort_type,
                        timeout_milliseconds,
                        [&](int error_code, std::list<SearchResult> &&r) {
                            if (PERR_OK == error_code) {
                                result = std::move(r);
                            }
                            ret = error_code;
                            search_completed.notify();
                        });
    search_completed.wait();
    return ret;
}

void geo_client::async_search_radial(double lat_degrees,
                                     double lng_degrees,
                                     double radius_m,
                                     int count,
                                     SortType sort_type,
                                     int timeout_milliseconds,
                                     geo_search_callback_t &&callback)
{
    util::units::Meters radius((float)radius_m);
    async_search_radial(S2LatLng::FromDegrees(lat_degrees, lng_degrees),
                        radius_m,
                        count,
                        sort_type,
                        timeout_milliseconds,
                        std::move(callback));
}

int geo_client::search_radial(const std::string &hash_key,
                              const std::string &sort_key,
                              double radius_m,
                              int count,
                              SortType sort_type,
                              int timeout_milliseconds,
                              std::list<SearchResult> &result)
{
    std::string value;
    int ret = _common_data_client->get(
        hash_key, sort_key, value, (int)(timeout_milliseconds * 0.2)); // TODO timeout control
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

    dsn::utils::notify_event search_completed;
    async_search_radial(latlng,
                        radius_m,
                        count,
                        sort_type,
                        (int)(timeout_milliseconds * 0.8), // TODO timeout control
                        [&](int error_code, std::list<SearchResult> &&r) {
                            if (error_code == PERR_OK) {
                                result = std::move(r);
                                search_completed.notify();
                            }
                        });
    search_completed.wait();

    return PERR_OK;
}

void geo_client::async_search_radial(const std::string &hash_key,
                                     const std::string &sort_key,
                                     double radius_m,
                                     int count,
                                     SortType sort_type,
                                     int timeout_milliseconds,
                                     geo_search_callback_t &&callback)
{
    _common_data_client->async_get(
        hash_key,
        sort_key,
        [ this, radius_m, count, sort_type, timeout_milliseconds, cb = std::move(callback) ](
            int error_code, std::string &&value, pegasus_client::internal_info &&info) mutable {
            if (error_code != PERR_OK) {
                derror_f("get failed, error={}", _common_data_client->get_error_string(error_code));
                cb(error_code, {});
                return;
            }

            S2LatLng latlng;
            int ret = _extractor(value, latlng);
            if (ret != PERR_OK) {
                derror_f("_extractor failed. value={}", value);
                cb(error_code, {});
                return;
            }

            async_search_radial(latlng,
                                radius_m,
                                count,
                                sort_type,
                                (int)(timeout_milliseconds * 0.8), // TODO timeout control
                                std::move(cb));
        },
        (int)(timeout_milliseconds * 0.2)); // TODO timeout control
}

void geo_client::async_search_radial(const S2LatLng &latlng,
                                     double radius_m,
                                     int count,
                                     SortType sort_type,
                                     int timeout_milliseconds,
                                     geo_search_callback_t &&callback)
{
    S2Cap cap;
    search_cap(latlng, radius_m, cap);

    S2CellUnion cids;
    get_covering_cells(cap, cids);

    // each scan result store in a separate vector, we will combine all the results finally
    async_get_result_from_cells(cids,
                                cap,
                                count,
                                sort_type,
                                [ this, count, sort_type, cb = std::move(callback) ](
                                    std::list<std::vector<SearchResult>> && results) {
                                    std::list<SearchResult> result;
                                    normalize_result(std::move(results), count, sort_type, result);
                                    cb(PERR_OK, std::move(result));
                                });
}

void geo_client::search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap)
{
    // construct a cap by center point and radius
    util::units::Meters radius((float)radius_m);
    cap = S2Cap(latlng.ToPoint(), S2Earth::ToAngle(radius));
}

void geo_client::get_covering_cells(const S2Cap &cap, S2CellUnion &cids)
{
    // calculate all the cells covered by the cap at `_min_level`
    S2RegionCoverer rc;
    rc.mutable_options()->set_fixed_level(_min_level);
    cids = rc.GetCovering(cap);
}

void geo_client::async_get_result_from_cells(const S2CellUnion &cids,
                                             const S2Cap &cap,
                                             int count,
                                             SortType sort_type,
                                             geo_search_callback_origin_t &&callback)
{
    int single_scan_count = count;
    if (sort_type == SortType::asc || sort_type == SortType::desc) {
        single_scan_count = -1; // scan all data to make full sort
    }

    // scan each cell
    std::shared_ptr<std::list<std::vector<SearchResult>>> results(
        new std::list<std::vector<SearchResult>>());
    std::shared_ptr<std::atomic<bool>> send_finish(new std::atomic<bool>(false));
    std::shared_ptr<std::atomic<int>> scan_count(new std::atomic<int>(0));
    auto scan_finish_cb = [ results, send_finish, scan_count, cb = std::move(callback) ]() mutable
    {
        if (scan_count->fetch_sub(1) == 1 && send_finish->load()) {
            cb(std::move(*results.get()));
        }
    };

    for (const auto &cid : cids) {
        if (cap.Contains(S2Cell(cid))) {
            // for the full contained cell, scan all data in this cell(at `_min_level`)
            results->emplace_back(std::vector<SearchResult>());
            scan_count->fetch_add(1);
            start_scan(
                cid.ToString(), "", "", cap, single_scan_count, scan_finish_cb, results->back());
        } else {
            // for the partial contained cell, scan cells covered by the cap at `_max_level` which
            // is more accurate than the ones at `_min_level`
            std::string hash_key = cid.parent(_min_level).ToString();
            std::pair<std::string, std::string> start_stop_keys;
            S2CellId pre;
            for (S2CellId cur = cid.child_begin(_max_level); cur != cid.child_end(_max_level);
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
                            start_stop_keys.second = get_sort_key(pre, hash_key) + "z";
                            results->emplace_back(std::vector<SearchResult>());
                            scan_count->fetch_add(1);
                            start_scan(hash_key,
                                       start_stop_keys.first,
                                       start_stop_keys.second,
                                       cap,
                                       single_scan_count,
                                       scan_finish_cb,
                                       results->back());

                            start_stop_keys.first = get_sort_key(cur, hash_key);
                            start_stop_keys.second.clear();
                        }
                        pre = cur;
                    }
                }
            }

            // edge case: when the cell is the last one in Hilbert curve in current `_min_level`
            // cell
            dassert(!start_stop_keys.first.empty(), "");
            if (start_stop_keys.second.empty()) {
                start_stop_keys.second = get_sort_key(pre, hash_key) + "z";
                results->emplace_back(std::vector<SearchResult>());
                scan_count->fetch_add(1);
                start_scan(hash_key,
                           start_stop_keys.first,
                           start_stop_keys.second,
                           cap,
                           single_scan_count,
                           scan_finish_cb,
                           results->back());
            }
        }
    }
    send_finish->store(true);
}

void geo_client::normalize_result(std::list<std::vector<SearchResult>> &&results,
                                  int count,
                                  SortType sort_type,
                                  std::list<SearchResult> &result)
{
    result.clear();
    for (auto &r : results) {
        result.insert(result.end(), r.begin(), r.end());
        if (sort_type == SortType::random && count > 0 && result.size() >= count) {
            break;
        }
    }
    if (sort_type == SortType::asc) {
        auto top_n_result =
            std::priority_queue<SearchResult, std::vector<SearchResult>, SearchResultNearer>();
        get_top_n(top_n_result, count, result);
    } else if (sort_type == SortType::desc) {
        auto top_n_result =
            std::priority_queue<SearchResult, std::vector<SearchResult>, SearchResultFarther>();
        get_top_n(top_n_result, count, result);
    } else if (count > 0 && result.size() > count) {
        result.resize((size_t)count);
    }
}

void geo_client::combine_keys(const std::string &hash_key,
                              const std::string &sort_key,
                              std::string &combine_key)
{
    dsn::blob blob_combine_key;
    pegasus_generate_key(blob_combine_key, hash_key, sort_key);
    combine_key = std::move(blob_combine_key.to_string());
}

int geo_client::extract_keys(const std::string &combined_sort_key,
                             std::string &hash_key,
                             std::string &sort_key)
{
    // combined_sort_key: [0,3]{30-_min_level}:combine_keys
    unsigned int leaf_cell_length = 30 - _min_level + 1;
    if (combined_sort_key.length() <= leaf_cell_length) {
        return PERR_INVALID_VALUE;
    }

    auto combine_key_len = static_cast<unsigned int>(combined_sort_key.length() - leaf_cell_length);
    pegasus_restore_key(dsn::blob(combined_sort_key.c_str(), leaf_cell_length, combine_key_len),
                        hash_key,
                        sort_key);

    return PERR_OK;
}

std::string geo_client::get_sort_key(const S2CellId &max_level_cid, const std::string &hash_key)
{
    return std::move(max_level_cid.ToString().substr(hash_key.length()));
}

void geo_client::async_set_common_data(const std::string &hash_key,
                                       const std::string &sort_key,
                                       const std::string &value,
                                       geo_set_callback_t &&callback,
                                       int timeout_milliseconds,
                                       int ttl_seconds)
{
    _common_data_client->async_set(
        hash_key,
        sort_key,
        value,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::common);
        },
        timeout_milliseconds,
        ttl_seconds);
}

void geo_client::async_set_geo_data(const std::string &hash_key,
                                    const std::string &sort_key,
                                    const std::string &value,
                                    geo_set_callback_t &&callback,
                                    int timeout_milliseconds,
                                    int ttl_seconds)
{
    S2LatLng latlng;
    int ret = _extractor(value, latlng);
    if (ret != PERR_OK) {
        derror_f("_extractor failed. value={}", value);
        if (callback != nullptr) {
            callback(PERR_GEO_DECODE_VALUE_ERROR, pegasus_client::internal_info(), DataType::geo);
        }
        return;
    }

    std::string combine_key;
    combine_keys(hash_key, sort_key, combine_key);

    async_set_geo_data(
        latlng,
        combine_key,
        value,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::geo);
        },
        timeout_milliseconds,
        ttl_seconds);
}

void geo_client::async_set_geo_data(const S2LatLng &latlng,
                                    const std::string &combine_key,
                                    const std::string &value,
                                    pegasus_client::async_set_callback_t &&callback,
                                    int timeout_milliseconds,
                                    int ttl_seconds)
{
    // leaf cell
    S2CellId leaf_cell_id = S2Cell(latlng).id();

    // convert to a parent level cell
    S2CellId parent_cell_id = leaf_cell_id.parent(_min_level);

    std::string hash_key(parent_cell_id.ToString()); // [0,5]{1}/[0,3]{_min_level}
    std::string sort_key(leaf_cell_id.ToString().substr(hash_key.length()) + ":" +
                         combine_key); // [0,3]{30-_min_level}:combine_keys

    _geo_data_client->async_set(
        hash_key, sort_key, value, std::move(callback), timeout_milliseconds, ttl_seconds);
}

void geo_client::start_scan(const std::string &hash_key,
                            const std::string &start_sort_key,
                            const std::string &stop_sort_key,
                            const S2Cap &cap,
                            int count,
                            scan_finish_callback cb,
                            std::vector<SearchResult> &result)
{
    dsn::tasking::enqueue(
        LPC_GEO_SCAN_DATA,
        nullptr,
        [this, hash_key, start_sort_key, stop_sort_key, cap, count, cb, &result]() {
            pegasus_client::scan_options options;
            options.start_inclusive = true;
            options.stop_inclusive = true;
            pegasus_client::pegasus_scanner *scanner = nullptr;
            int ret = _geo_data_client->get_scanner(
                hash_key, start_sort_key, stop_sort_key, options, scanner);
            if (ret == PERR_OK) {
                do_scan(scanner, cap, count, cb, result);
            }
        });
}

void geo_client::do_scan(pegasus_client::pegasus_scanner *scanner,
                         const S2Cap &cap,
                         int count,
                         scan_finish_callback cb,
                         std::vector<SearchResult> &result)
{
    scanner->async_next(
        [this, cap, count, scanner, cb, &result](int ret,
                                                 std::string &&hash_key,
                                                 std::string &&sort_key,
                                                 std::string &&scan_value,
                                                 pegasus_client::internal_info &&info) {
            if (ret == PERR_SCAN_COMPLETE) {
                cb();
                return;
            }

            if (ret != PERR_OK) {
                derror_f("async_next failed. error={}", _geo_data_client->get_error_string(ret));
                cb();
                return;
            }

            S2LatLng latlng;
            if (_extractor(scan_value, latlng) != PERR_OK) {
                derror_f("_extractor failed. scan_value={}", scan_value);
                cb();
                return;
            }

            util::units::Meters distance = S2Earth::GetDistance(S2LatLng(cap.center()), latlng);
            if (distance <= S2Earth::ToDistance(cap.radius())) {
                std::string origin_hash_key, origin_sort_key;
                if (extract_keys(sort_key, origin_hash_key, origin_sort_key) != PERR_OK) {
                    derror_f("extract_keys failed. sort_key={}", sort_key);
                    cb();
                    return;
                }

                result.emplace_back(SearchResult(latlng.lat().degrees(),
                                                 latlng.lng().degrees(),
                                                 distance.value(),
                                                 std::move(origin_hash_key),
                                                 std::move(origin_sort_key),
                                                 std::move(scan_value)));
            }

            if (count != -1 && result.size() >= count) {
                cb();
                return;
            }

            do_scan(scanner, cap, count, cb, result);
        });
}

} // namespace geo
} // namespace pegasus
