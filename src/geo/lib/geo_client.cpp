// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo_client.h"

#include <s2/s2earth.h>
#include <s2/s2region_coverer.h>
#include <s2/s2cap.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>
#include <base/pegasus_key_schema.h>
#include <base/pegasus_utils.h>

namespace pegasus {
namespace geo {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_GEO)
DEFINE_TASK_CODE(LPC_GEO_SCAN_DATA, TASK_PRIORITY_COMMON, THREAD_POOL_GEO)

struct SearchResultNearer
{
    inline bool operator()(const SearchResult &l, const SearchResult &r)
    {
        return l.distance < r.distance;
    }
};

struct SearchResultFarther
{
    inline bool operator()(const SearchResult &l, const SearchResult &r)
    {
        return l.distance > r.distance;
    }
};

geo_client::geo_client(const char *config_file,
                       const char *cluster_name,
                       const char *common_app_name,
                       const char *geo_app_name,
                       latlng_extractor *extractor)
{
    bool ok = pegasus_client_factory::initialize(config_file);
    dassert(ok, "init pegasus client factory failed");

    _common_data_client = pegasus_client_factory::get_client(cluster_name, common_app_name);
    dassert(_common_data_client != nullptr, "init pegasus _common_data_client failed");

    _geo_data_client = pegasus_client_factory::get_client(cluster_name, geo_app_name);
    dassert(_geo_data_client != nullptr, "init pegasus _geo_data_client failed");

    _extractor.reset(extractor);

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
    dsn::utils::notify_event set_completed;
    auto async_set_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            derror_f("set data failed. hash_key={}, sort_key={}", hash_key, sort_key);
            ret = ec_;
        }
        if (info != nullptr) {
            *info = std::move(info_);
        }
        set_completed.notify();
    };
    async_set(hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
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
    async_del(
        hash_key,
        sort_key,
        [this,
         hash_key,
         sort_key,
         value,
         timeout_milliseconds,
         ttl_seconds,
         cb = std::move(callback)](int ec_, pegasus_client::internal_info &&info_) {
            if (ec_ != PERR_OK) {
                cb(ec_, std::move(info_));
                return;
            }

            std::shared_ptr<int> ret(new int(PERR_OK));
            std::shared_ptr<std::atomic<int32_t>> set_count(new std::atomic<int32_t>(2));
            std::shared_ptr<pegasus_client::internal_info> info(
                new pegasus_client::internal_info());
            auto async_set_callback =
                [=](int ec_, pegasus_client::internal_info &&info_, DataType data_type_) {
                    if (data_type_ == DataType::common) {
                        *info = std::move(info_);
                    }

                    if (ec_ != PERR_OK) {
                        derror_f("set {} data failed. hash_key={}, sort_key={}",
                                 data_type_ == DataType::common ? "common" : "geo",
                                 hash_key,
                                 sort_key);
                        *ret = ec_;
                    }

                    if (set_count->fetch_sub(1) == 1) {
                        if (cb != nullptr) {
                            cb(*ret, std::move(*info));
                        }
                    }
                };

            async_set_common_data(
                hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
            async_set_geo_data(
                hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
        },
        timeout_milliseconds);
}

int geo_client::del(const std::string &hash_key,
                    const std::string &sort_key,
                    int timeout_milliseconds,
                    pegasus_client::internal_info *info)
{
    int ret = PERR_OK;
    dsn::utils::notify_event del_completed;
    auto async_del_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            derror_f("del data failed. hash_key={}, sort_key={}", hash_key, sort_key);
            ret = ec_;
        }
        if (info != nullptr) {
            *info = std::move(info_);
        }
        del_completed.notify();
    };
    async_del(hash_key, sort_key, async_del_callback, timeout_milliseconds);
    del_completed.wait();

    return ret;
}

void geo_client::async_del(const std::string &hash_key,
                           const std::string &sort_key,
                           pegasus_client::async_del_callback_t &&callback,
                           int timeout_milliseconds)
{
    _common_data_client->async_get(
        hash_key,
        sort_key,
        [this, hash_key, sort_key, timeout_milliseconds, cb = std::move(callback)](
            int ec_, std::string &&value_, pegasus::pegasus_client::internal_info &&info_) {
            if (ec_ == PERR_NOT_FOUND) {
                if (cb != nullptr) {
                    cb(PERR_OK, std::move(info_));
                }
                return;
            }

            if (ec_ != PERR_OK) {
                if (cb != nullptr) {
                    cb(ec_, std::move(info_));
                }
                return;
            }

            std::string geo_hash_key;
            std::string geo_sort_key;
            if (!generate_geo_keys(hash_key, sort_key, value_, geo_hash_key, geo_sort_key)) {
                cb(PERR_GEO_DECODE_VALUE_ERROR, pegasus_client::internal_info());
                return;
            }

            std::shared_ptr<int> ret(new int(PERR_OK));
            std::shared_ptr<std::atomic<int32_t>> del_count(new std::atomic<int32_t>(2));
            auto async_del_callback =
                [=](int ec__, pegasus_client::internal_info &&, DataType data_type_) mutable {
                    if (ec__ != PERR_OK) {
                        derror_f("del {} data failed. hash_key={}, sort_key={}",
                                 data_type_ == DataType::common ? "common" : "geo",
                                 hash_key,
                                 sort_key);
                        *ret = ec__;
                    }

                    if (del_count->fetch_sub(1) == 1) {
                        cb(*ret, std::move(info_));
                    }
                };

            async_del_common_data(hash_key, sort_key, async_del_callback, timeout_milliseconds);
            async_del_geo_data(
                geo_hash_key, geo_sort_key, async_del_callback, timeout_milliseconds);
        },
        timeout_milliseconds);
}

int geo_client::set_geo_data(const std::string &hash_key,
                             const std::string &sort_key,
                             const std::string &value,
                             int timeout_milliseconds,
                             int ttl_seconds)
{
    int ret = PERR_OK;
    dsn::utils::notify_event set_completed;
    auto async_set_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            ret = ec_;
            derror_f("set geo data failed. hash_key={}, sort_key={}", hash_key, sort_key);
        }
        set_completed.notify();
    };
    async_set_geo_data(
        hash_key, sort_key, value, async_set_callback, timeout_milliseconds, ttl_seconds);
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
        [cb = std::move(callback)](int ec_, pegasus_client::internal_info &&info_, DataType) {
            if (cb != nullptr) {
                cb(ec_, std::move(info_));
            }
        },
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
    S2LatLng latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);
    if (!latlng.is_valid()) {
        derror_f("latlng is invalid. lat_degrees={}, lng_degrees={}", lat_degrees, lng_degrees);
        return PERR_GEO_INVALID_LATLNG_ERROR;
    }
    dsn::utils::notify_event search_completed;
    async_search_radial(latlng,
                        radius_m,
                        count,
                        sort_type,
                        timeout_milliseconds,
                        [&](int ec_, std::list<SearchResult> &&result_) {
                            if (PERR_OK == ec_) {
                                result = std::move(result_);
                            }
                            ret = ec_;
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
    S2LatLng latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);
    if (!latlng.is_valid()) {
        derror_f("latlng is invalid. lat_degrees={}, lng_degrees={}", lat_degrees, lng_degrees);
        callback(PERR_GEO_INVALID_LATLNG_ERROR, {});
    }

    async_search_radial(
        latlng, radius_m, count, sort_type, timeout_milliseconds, std::move(callback));
}

int geo_client::search_radial(const std::string &hash_key,
                              const std::string &sort_key,
                              double radius_m,
                              int count,
                              SortType sort_type,
                              int timeout_milliseconds,
                              std::list<SearchResult> &result)
{
    int ret = PERR_OK;
    dsn::utils::notify_event search_completed;
    async_search_radial(hash_key,
                        sort_key,
                        radius_m,
                        count,
                        sort_type,
                        timeout_milliseconds,
                        [&](int ec_, std::list<SearchResult> &&result_) {
                            if (ec_ != PERR_OK) {
                                ret = ec_;
                            }
                            result = std::move(result_);
                            search_completed.notify();
                        });
    search_completed.wait();

    return ret;
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
        [this,
         hash_key,
         sort_key,
         radius_m,
         count,
         sort_type,
         timeout_milliseconds,
         cb = std::move(callback)](
            int ec_, std::string &&value_, pegasus_client::internal_info &&) mutable {
            if (ec_ != PERR_OK) {
                derror_f("get failed. hash_key={}, sort_key={}, error={}",
                         hash_key,
                         sort_key,
                         get_error_string(ec_));
                cb(ec_, {});
                return;
            }

            S2LatLng latlng;
            if (!_extractor->extract_from_value(value_, latlng)) {
                derror_f("extract_from_value failed. hash_key={}, sort_key={}, value={}",
                         hash_key,
                         sort_key,
                         value_);
                cb(ec_, {});
                return;
            }

            async_search_radial(latlng,
                                radius_m,
                                count,
                                sort_type,
                                (int)ceil(timeout_milliseconds * 0.8),
                                std::move(cb));
        },
        (int)ceil(timeout_milliseconds * 0.2));
}

void geo_client::async_search_radial(const S2LatLng &latlng,
                                     double radius_m,
                                     int count,
                                     SortType sort_type,
                                     int timeout_milliseconds,
                                     geo_search_callback_t &&callback)
{
    // generate a cap
    S2Cap cap;
    gen_search_cap(latlng, radius_m, cap);

    // generate cell ids
    S2CellUnion cids;
    gen_cells_covered_by_cap(cap, cids);

    // search data in the cell ids
    async_get_result_from_cells(cids,
                                cap,
                                count,
                                sort_type,
                                [this, count, sort_type, cb = std::move(callback)](
                                    std::list<std::vector<SearchResult>> &&results_) {
                                    std::list<SearchResult> result;
                                    normalize_result(results_, count, sort_type, result);
                                    cb(PERR_OK, std::move(result));
                                });
}

void geo_client::gen_search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap)
{
    util::units::Meters radius((float)radius_m);
    cap = S2Cap(latlng.ToPoint(), S2Earth::ToAngle(radius));
}

void geo_client::gen_cells_covered_by_cap(const S2Cap &cap, S2CellUnion &cids)
{
    S2RegionCoverer rc;
    rc.mutable_options()->set_fixed_level(_min_level);
    cids = rc.GetCovering(cap);
}

void geo_client::async_get_result_from_cells(const S2CellUnion &cids,
                                             const S2Cap &cap,
                                             int count,
                                             SortType sort_type,
                                             scan_all_area_callback_t &&callback)
{
    int single_scan_count = count;
    if (sort_type == SortType::asc || sort_type == SortType::desc) {
        single_scan_count = -1; // scan all data to make full sort
    }

    // scan all cell ids
    std::shared_ptr<std::list<std::vector<SearchResult>>> results(
        new std::list<std::vector<SearchResult>>());
    std::shared_ptr<std::atomic<bool>> send_finish(new std::atomic<bool>(false));
    std::shared_ptr<std::atomic<int>> scan_count(new std::atomic<int>(0));
    auto single_scan_finish_callback =
        [send_finish, scan_count, results, cb = std::move(callback)]() {
            // NOTE: make sure fetch_sub is at first of the if expression to make it always execute
            if (scan_count->fetch_sub(1) == 1 && send_finish->load()) {
                cb(std::move(*results.get()));
            }
        };

    std::shared_ptr<dsn::task_tracker> tracker(new dsn::task_tracker);
    for (const auto &cid : cids) {
        if (cap.Contains(S2Cell(cid))) {
            // for the full contained cell, scan all data in this cell(which is at the `_min_level`)
            results->emplace_back(std::vector<SearchResult>());
            scan_count->fetch_add(1);
            start_scan(cid.ToString(),
                       "",
                       "",
                       cap,
                       single_scan_count,
                       single_scan_finish_callback,
                       results->back());
        } else {
            // for the partial contained cell, scan cells covered by the cap at the `_max_level`
            // which is more accurate than the ones at `_min_level`, but it will cost more time on
            // calculating here.
            std::string hash_key = cid.parent(_min_level).ToString();
            std::pair<std::string, std::string> start_stop_sort_keys;
            S2CellId pre;
            // traverse all sub cell ids of `cid` on `_max_level` along the Hilbert curve, to find
            // the needed ones.
            for (S2CellId cur = cid.child_begin(_max_level); cur != cid.child_end(_max_level);
                 cur = cur.next()) {
                if (cap.MayIntersect(S2Cell(cur))) {
                    // only cells whose any vertex is contained by the cap is needed
                    if (!pre.is_valid()) {
                        // `cur` is the very first cell in Hilbert curve contained by the cap
                        pre = cur;
                        start_stop_sort_keys.first = gen_start_sort_key(pre, hash_key);
                    } else {
                        if (pre.next() != cur) {
                            // `pre` is the last cell in Hilbert curve contained by the cap
                            // `cur` is a new start cell in Hilbert curve contained by the cap
                            start_stop_sort_keys.second = gen_stop_sort_key(pre, hash_key);
                            results->emplace_back(std::vector<SearchResult>());
                            scan_count->fetch_add(1);
                            start_scan(hash_key,
                                       start_stop_sort_keys.first,
                                       start_stop_sort_keys.second,
                                       cap,
                                       single_scan_count,
                                       single_scan_finish_callback,
                                       results->back());

                            start_stop_sort_keys.first = gen_start_sort_key(cur, hash_key);
                            start_stop_sort_keys.second.clear();
                        }
                        pre = cur;
                    }
                }
            }

            dassert(!start_stop_sort_keys.first.empty(), "");
            // the last sub slice of current `cid` on `_max_level` in Hilbert curve covered by `cap`
            if (start_stop_sort_keys.second.empty()) {
                start_stop_sort_keys.second = gen_stop_sort_key(pre, hash_key);
                results->emplace_back(std::vector<SearchResult>());
                scan_count->fetch_add(1);
                start_scan(hash_key,
                           start_stop_sort_keys.first,
                           start_stop_sort_keys.second,
                           cap,
                           single_scan_count,
                           single_scan_finish_callback,
                           results->back());
            }
        }
    }
    // when all scan rpc have received before send_finish is set to true, the callback will never be
    // called, so we add 2 lines tricky code as follows
    scan_count->fetch_add(1);
    send_finish->store(true);
    single_scan_finish_callback();
}

void geo_client::normalize_result(const std::list<std::vector<SearchResult>> &results,
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
        result = utils::top_n<SearchResult, SearchResultNearer>(result, count).to();
    } else if (sort_type == SortType::desc) {
        result = utils::top_n<SearchResult, SearchResultFarther>(result, count).to();
    } else if (count > 0 && result.size() > count) {
        result.resize((size_t)count);
    }
}

bool geo_client::generate_geo_keys(const std::string &hash_key,
                                   const std::string &sort_key,
                                   const std::string &value,
                                   std::string &geo_hash_key,
                                   std::string &geo_sort_key)
{
    // extract latitude and longitude from value
    S2LatLng latlng;
    if (!_extractor->extract_from_value(value, latlng)) {
        derror_f("extract_from_value failed. value={}", value);
        return false;
    }

    // generate hash key
    S2CellId leaf_cell_id = S2Cell(latlng).id();
    S2CellId parent_cell_id = leaf_cell_id.parent(_min_level);
    geo_hash_key = parent_cell_id.ToString(); // [0,5]{1}/[0,3]{_min_level}

    // generate sort key
    dsn::blob sort_key_postfix;
    pegasus_generate_key(sort_key_postfix, hash_key, sort_key);
    geo_sort_key = leaf_cell_id.ToString().substr(geo_hash_key.length()) + ":" +
                   sort_key_postfix.to_string(); // [0,3]{30-_min_level}:combine_keys

    return true;
}

bool geo_client::restore_origin_keys(const std::string &geo_sort_key,
                                     std::string &origin_hash_key,
                                     std::string &origin_sort_key)
{
    // geo_sort_key: [0,3]{30-_min_level}:combine_keys
    int cid_prefix_len = 30 - _min_level + 1;
    if (geo_sort_key.length() <= cid_prefix_len) {
        return false;
    }

    auto origin_keys_len = static_cast<unsigned int>(geo_sort_key.length() - cid_prefix_len);
    pegasus_restore_key(dsn::blob(geo_sort_key.c_str(), cid_prefix_len, origin_keys_len),
                        origin_hash_key,
                        origin_sort_key);

    return true;
}

std::string geo_client::gen_sort_key(const S2CellId &max_level_cid, const std::string &hash_key)
{
    return max_level_cid.ToString().substr(hash_key.length());
}

std::string geo_client::gen_start_sort_key(const S2CellId &max_level_cid,
                                           const std::string &hash_key)
{
    return gen_sort_key(max_level_cid, hash_key);
}

std::string geo_client::gen_stop_sort_key(const S2CellId &max_level_cid,
                                          const std::string &hash_key)
{
    return gen_sort_key(max_level_cid, hash_key) + "z";
}

void geo_client::async_set_common_data(const std::string &hash_key,
                                       const std::string &sort_key,
                                       const std::string &value,
                                       update_callback_t &&callback,
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
                                    update_callback_t &&callback,
                                    int timeout_milliseconds,
                                    int ttl_seconds)
{
    std::string geo_hash_key;
    std::string geo_sort_key;
    if (!generate_geo_keys(hash_key, sort_key, value, geo_hash_key, geo_sort_key)) {
        callback(PERR_GEO_DECODE_VALUE_ERROR, pegasus_client::internal_info(), DataType::geo);
        return;
    }

    _geo_data_client->async_set(
        geo_hash_key,
        geo_sort_key,
        value,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::geo);
        },
        timeout_milliseconds,
        ttl_seconds);
}

void geo_client::async_del_common_data(const std::string &hash_key,
                                       const std::string &sort_key,
                                       update_callback_t &&callback,
                                       int timeout_milliseconds)
{
    _common_data_client->async_del(
        hash_key,
        sort_key,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::common);
        },
        timeout_milliseconds);
}

void geo_client::async_del_geo_data(const std::string &geo_hash_key,
                                    const std::string &geo_sort_key,
                                    update_callback_t &&callback,
                                    int timeout_milliseconds)
{
    _geo_data_client->async_del(
        geo_hash_key,
        geo_sort_key,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::geo);
        },
        timeout_milliseconds);
}

void geo_client::start_scan(const std::string &hash_key,
                            const std::string &start_sort_key,
                            const std::string &stop_sort_key,
                            const S2Cap &cap,
                            int count,
                            scan_one_area_callback &&callback,
                            std::vector<SearchResult> &result)
{
    dsn::tasking::enqueue(LPC_GEO_SCAN_DATA,
                          &_tracker,
                          [this,
                           hash_key,
                           start_sort_key,
                           stop_sort_key,
                           cap,
                           count,
                           cb = std::move(callback),
                           &result]() mutable {
                              pegasus_client::scan_options options;
                              options.start_inclusive = true;
                              options.stop_inclusive = true;
                              pegasus_client::pegasus_scanner *scanner = nullptr;
                              int ret = _geo_data_client->get_scanner(
                                  hash_key, start_sort_key, stop_sort_key, options, scanner);
                              if (ret == PERR_OK) {
                                  pegasus_client::pegasus_scanner_wrapper scanner_wrapper =
                                      scanner->get_smart_wrapper();
                                  do_scan(scanner_wrapper, cap, count, std::move(cb), result);
                              }
                          });
}

void geo_client::do_scan(pegasus_client::pegasus_scanner_wrapper scanner_wrapper,
                         const S2Cap &cap,
                         int count,
                         scan_one_area_callback &&callback,
                         std::vector<SearchResult> &result)
{
    scanner_wrapper->async_next(
        [this, cap, count, scanner_wrapper, cb = std::move(callback), &result](
            int ret,
            std::string &&geo_hash_key,
            std::string &&geo_sort_key,
            std::string &&value,
            pegasus_client::internal_info &&info) mutable {
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
            if (!_extractor->extract_from_value(value, latlng)) {
                derror_f("extract_from_value failed. value={}", value);
                cb();
                return;
            }

            double distance = S2Earth::GetDistanceMeters(S2LatLng(cap.center()), latlng);
            if (distance <= S2Earth::ToMeters(cap.radius())) {
                std::string origin_hash_key, origin_sort_key;
                if (!restore_origin_keys(geo_sort_key, origin_hash_key, origin_sort_key)) {
                    derror_f("restore_origin_keys failed. geo_sort_key={}", geo_sort_key);
                    cb();
                    return;
                }

                result.emplace_back(SearchResult(latlng.lat().degrees(),
                                                 latlng.lng().degrees(),
                                                 distance,
                                                 std::move(origin_hash_key),
                                                 std::move(origin_sort_key),
                                                 std::move(value)));
            }

            if (count != -1 && result.size() >= count) {
                cb();
                return;
            }

            do_scan(scanner_wrapper, cap, count, std::move(cb), result);
        });
}

int geo_client::distance(const std::string &hash_key1,
                         const std::string &sort_key1,
                         const std::string &hash_key2,
                         const std::string &sort_key2,
                         int timeout_milliseconds,
                         double &distance)
{
    int ret = PERR_OK;
    dsn::utils::notify_event get_completed;
    auto async_calculate_callback = [&](int ec_, double &&distance_) {
        if (ec_ != PERR_OK) {
            derror_f("get distance failed.");
            ret = ec_;
        }
        distance = distance_;
        get_completed.notify();
    };
    async_distance(
        hash_key1, sort_key1, hash_key2, sort_key2, timeout_milliseconds, async_calculate_callback);
    get_completed.wait();

    return ret;
}

void geo_client::async_distance(const std::string &hash_key1,
                                const std::string &sort_key1,
                                const std::string &hash_key2,
                                const std::string &sort_key2,
                                int timeout_milliseconds,
                                distance_callback_t &&callback)
{
    std::shared_ptr<int> ret(new int(PERR_OK));
    std::shared_ptr<std::mutex> mutex(new std::mutex());
    std::shared_ptr<std::vector<S2LatLng>> get_result(new std::vector<S2LatLng>());
    auto async_get_callback = [=, cb = std::move(callback)](
                                  int ec_, std::string &&value_, pegasus_client::internal_info &&) {
        if (ec_ != PERR_OK) {
            derror_f("get data failed.");
            *ret = ec_;
        }

        S2LatLng latlng;
        if (!_extractor->extract_from_value(value_, latlng)) {
            derror_f("extract_from_value failed. value={}", value_);
            *ret = PERR_GEO_DECODE_VALUE_ERROR;
        }

        std::lock_guard<std::mutex> lock(*mutex);
        get_result->push_back(latlng);
        if (get_result->size() == 2) {
            if (*ret == PERR_OK) {
                double distance = S2Earth::GetDistanceMeters((*get_result)[0], (*get_result)[1]);
                cb(*ret, distance);
            } else {
                cb(*ret, std::numeric_limits<double>::max());
            }
        }
    };

    _common_data_client->async_get(hash_key1, sort_key1, async_get_callback, timeout_milliseconds);
    _common_data_client->async_get(hash_key2, sort_key2, async_get_callback, timeout_milliseconds);
}

} // namespace geo
} // namespace pegasus
