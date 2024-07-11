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

#include "geo_client.h"

#include <fmt/core.h>
#include <math.h>
#include <pegasus/error.h>
#include <s2/s1angle.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <s2/util/units/length-units.h>
#include <stddef.h>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <type_traits>
#include <vector>

#include "base/pegasus_key_schema.h"
#include "base/pegasus_utils.h"
#include "geo/lib/latlng_codec.h"
#include "pegasus/client.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

DSN_DEFINE_int32(geo_client.lib,
                 min_level,
                 12,
                 "Min cell level for a scan. Cell id at this level is the hash-key in Pegasus. "
                 "min_level is immutable after geo_client data has been inserted into DB. "
                 "Edge length at level 12 is about 2 km");
DSN_DEFINE_int32(geo_client.lib,
                 max_level,
                 16,
                 "Max cell level for a scan. Cell id at this level is the prefix of sort-key "
                 "in Pegasus, and it's convenient for scan operation. max_level is mutable "
                 "at any time, and geo_client-lib users can change it to a appropriate "
                 "value to improve performance in their scenario. Edge length at level 16 "
                 "is about 150 m");
DSN_DEFINE_group_validator(min_max_level, [](std::string &message) -> bool {
    if (FLAGS_min_level >= FLAGS_max_level) {
        message = fmt::format("[geo_client.lib].min_level({}) should be < "
                              "[geo_client.lib].max_level({})",
                              FLAGS_min_level,
                              FLAGS_max_level);
        return false;
    }
    return true;
});
DSN_DEFINE_uint32(geo_client.lib, latitude_index, 5, "latitude index in value");
DSN_DEFINE_uint32(geo_client.lib, longitude_index, 4, "longitude index in value");

namespace pegasus {
namespace geo {

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
                       const char *geo_app_name)
{
    bool ok = pegasus_client_factory::initialize(config_file);
    CHECK(ok, "init pegasus client factory failed");

    _common_data_client = pegasus_client_factory::get_client(cluster_name, common_app_name);
    CHECK_NOTNULL(_common_data_client, "init pegasus _common_data_client failed");

    _geo_data_client = pegasus_client_factory::get_client(cluster_name, geo_app_name);
    CHECK_NOTNULL(_geo_data_client, "init pegasus _geo_data_client failed");

    dsn::error_s s = _codec.set_latlng_indices(FLAGS_latitude_index, FLAGS_longitude_index);
    CHECK_OK(s, "set_latlng_indices({}, {}) failed", FLAGS_latitude_index, FLAGS_longitude_index);
}

dsn::error_s geo_client::set_max_level(int level)
{
    if (level <= FLAGS_min_level) {
        return FMT_ERR(dsn::ERR_INVALID_PARAMETERS,
                       "level({}) must be larger than FLAGS_min_level({})",
                       level,
                       FLAGS_min_level);
    }

    FLAGS_max_level = level;
    return dsn::error_s::ok();
}

int geo_client::set(const std::string &hash_key,
                    const std::string &sort_key,
                    const std::string &value,
                    int timeout_ms,
                    int ttl_seconds,
                    pegasus_client::internal_info *info)
{
    int ret = PERR_OK;
    dsn::utils::notify_event set_completed;
    auto async_set_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            LOG_ERROR("set data failed. hash_key={}, sort_key={}, error={}",
                      utils::redact_sensitive_string(hash_key),
                      utils::redact_sensitive_string(sort_key),
                      get_error_string(ec_));
            ret = ec_;
        }
        if (info != nullptr) {
            *info = std::move(info_);
        }
        set_completed.notify();
    };
    async_set(hash_key, sort_key, value, async_set_callback, timeout_ms, ttl_seconds);
    set_completed.wait();

    return ret;
}

void geo_client::async_set(const std::string &hash_key,
                           const std::string &sort_key,
                           const std::string &value,
                           pegasus_client::async_set_callback_t &&callback,
                           int timeout_ms,
                           int ttl_seconds)
{
    async_del(
        hash_key,
        sort_key,
        true,
        [this, hash_key, sort_key, value, timeout_ms, ttl_seconds, cb = std::move(callback)](
            int ec_, pegasus_client::internal_info &&info_) {
            if (ec_ != PERR_OK) {
                cb(ec_, std::move(info_));
                return;
            }

            std::shared_ptr<int> ret = std::make_shared<int>(PERR_OK);
            std::shared_ptr<std::atomic<int32_t>> set_count =
                std::make_shared<std::atomic<int32_t>>(2);
            std::shared_ptr<pegasus_client::internal_info> info =
                std::make_shared<pegasus_client::internal_info>();
            auto async_set_callback =
                [=](int ec_, pegasus_client::internal_info &&info_, DataType data_type_) {
                    if (data_type_ == DataType::common) {
                        *info = std::move(info_);
                    }

                    if (ec_ != PERR_OK) {
                        LOG_ERROR("set {} data failed. hash_key={}, sort_key={}, error={}",
                                  data_type_ == DataType::common ? "common" : "geo",
                                  utils::redact_sensitive_string(hash_key),
                                  utils::redact_sensitive_string(sort_key),
                                  get_error_string(ec_));
                        *ret = ec_;
                    }

                    if (set_count->fetch_sub(1) == 1) {
                        if (cb != nullptr) {
                            cb(*ret, std::move(*info));
                        }
                    }
                };

            async_set_common_data(
                hash_key, sort_key, value, async_set_callback, timeout_ms, ttl_seconds);
            async_set_geo_data(
                hash_key, sort_key, value, async_set_callback, timeout_ms, ttl_seconds);
        },
        timeout_ms);
}

void geo_client::async_set(const std::string &hash_key,
                           const std::string &sort_key,
                           double lat_degrees,
                           double lng_degrees,
                           pegasus_client::async_set_callback_t &&callback,
                           int timeout_ms,
                           int ttl_seconds)
{
    std::string value;
    if (!_codec.encode_to_value(lat_degrees, lng_degrees, value)) {
        callback(PERR_GEO_INVALID_LATLNG_ERROR, {});
        return;
    }

    async_set(hash_key, sort_key, value, std::move(callback), timeout_ms, ttl_seconds);
}

int geo_client::get(const std::string &hash_key,
                    const std::string &sort_key,
                    double &lat_degrees,
                    double &lng_degrees,
                    int timeout_ms)
{
    int ret = PERR_OK;
    dsn::utils::notify_event get_completed;
    auto get_latlng_callback = [&](int ec_, int id_, double lat_degrees_, double lng_degrees_) {
        if (ec_ == PERR_OK) {
            lat_degrees = lat_degrees_;
            lng_degrees = lng_degrees_;
        } else {
            LOG_WARNING("get data failed. hash_key={}, sort_key={}, error={}",
                        utils::redact_sensitive_string(hash_key),
                        utils::redact_sensitive_string(sort_key),
                        get_error_string(ec_));
        }
        ret = ec_;
        get_completed.notify();
    };
    async_get(hash_key, sort_key, 0, get_latlng_callback, timeout_ms);
    get_completed.wait();

    return ret;
}

void geo_client::async_get(const std::string &hash_key,
                           const std::string &sort_key,
                           int id,
                           get_latlng_callback_t &&callback,
                           int timeout_ms)
{
    _common_data_client->async_get(
        hash_key,
        sort_key,
        [this, &hash_key, &sort_key, id, cb = std::move(callback)](
            int ec_, std::string &&value_, pegasus_client::internal_info &&info_) {
            if (ec_ != PERR_OK) {
                cb(ec_, id, 0, 0);
                return;
            }
            S2LatLng latlng;
            if (!_codec.decode_from_value(value_, latlng)) {
                LOG_ERROR("decode_from_value failed. hash_key={}, sort_key={}, value={}",
                          utils::redact_sensitive_string(hash_key),
                          utils::redact_sensitive_string(sort_key),
                          value_);
                cb(PERR_GEO_DECODE_VALUE_ERROR, id, 0, 0);
                return;
            }
            cb(ec_, id, latlng.lat().degrees(), latlng.lng().degrees());
        },
        timeout_ms);
}

int geo_client::del(const std::string &hash_key,
                    const std::string &sort_key,
                    int timeout_ms,
                    pegasus_client::internal_info *info)
{
    int ret = PERR_OK;
    dsn::utils::notify_event del_completed;
    auto async_del_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            LOG_ERROR("del data failed. hash_key={}, sort_key={}, error={}",
                      utils::redact_sensitive_string(hash_key),
                      utils::redact_sensitive_string(sort_key),
                      get_error_string(ec_));
            ret = ec_;
        }
        if (info != nullptr) {
            *info = std::move(info_);
        }
        del_completed.notify();
    };
    async_del(hash_key, sort_key, false, async_del_callback, timeout_ms);
    del_completed.wait();

    return ret;
}

void geo_client::async_del(const std::string &hash_key,
                           const std::string &sort_key,
                           bool keep_common_data,
                           pegasus_client::async_del_callback_t &&callback,
                           int timeout_ms)
{
    _common_data_client->async_get(
        hash_key,
        sort_key,
        [this, hash_key, sort_key, keep_common_data, timeout_ms, cb = std::move(callback)](
            int ec_, std::string &&value_, pegasus_client::internal_info &&info_) {
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

            bool keep_geo_data = false;
            std::string geo_hash_key;
            std::string geo_sort_key;
            if (!generate_geo_keys(hash_key, sort_key, value_, geo_hash_key, geo_sort_key)) {
                keep_geo_data = true;
                LOG_WARNING("generate_geo_keys failed");
            }

            std::shared_ptr<int> ret = std::make_shared<int>(PERR_OK);
            std::shared_ptr<std::atomic<int32_t>> del_count =
                std::make_shared<std::atomic<int32_t>>(2);
            if (keep_common_data) {
                del_count->fetch_sub(1);
            }
            if (keep_geo_data) {
                del_count->fetch_sub(1);
            }
            if (del_count->load() == 0) {
                cb(PERR_OK, pegasus_client::internal_info());
                return;
            }

            auto async_del_callback =
                [=](int ec__, pegasus_client::internal_info &&, DataType data_type_) mutable {
                    if (ec__ != PERR_OK) {
                        LOG_ERROR("del {} data failed. hash_key={}, sort_key={}, error={}",
                                  data_type_ == DataType::common ? "common" : "geo",
                                  utils::redact_sensitive_string(hash_key),
                                  utils::redact_sensitive_string(sort_key),
                                  get_error_string(ec_));
                        *ret = ec__;
                    }

                    if (del_count->fetch_sub(1) == 1) {
                        cb(*ret, std::move(info_));
                    }
                };

            if (!keep_common_data) {
                async_del_common_data(hash_key, sort_key, async_del_callback, timeout_ms);
            }
            if (!keep_geo_data) {
                async_del_geo_data(geo_hash_key, geo_sort_key, async_del_callback, timeout_ms);
            }
        },
        timeout_ms);
}

int geo_client::set_geo_data(const std::string &hash_key,
                             const std::string &sort_key,
                             const std::string &value,
                             int timeout_ms,
                             int ttl_seconds)
{
    int ret = PERR_OK;
    dsn::utils::notify_event set_completed;
    auto async_set_callback = [&](int ec_, pegasus_client::internal_info &&info_) {
        if (ec_ != PERR_OK) {
            ret = ec_;
            LOG_ERROR("set geo data failed. hash_key={}, sort_key={}, error={}",
                      utils::redact_sensitive_string(hash_key),
                      utils::redact_sensitive_string(sort_key),
                      get_error_string(ec_));
        }
        set_completed.notify();
    };
    async_set_geo_data(hash_key, sort_key, value, async_set_callback, timeout_ms, ttl_seconds);
    set_completed.wait();
    return ret;
}

void geo_client::async_set_geo_data(const std::string &hash_key,
                                    const std::string &sort_key,
                                    const std::string &value,
                                    pegasus_client::async_set_callback_t &&callback,
                                    int timeout_ms,
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
        timeout_ms,
        ttl_seconds);
}

int geo_client::search_radial(double lat_degrees,
                              double lng_degrees,
                              double radius_m,
                              int count,
                              SortType sort_type,
                              int timeout_ms,
                              std::list<SearchResult> &result)
{
    int ret = PERR_OK;
    S2LatLng latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);
    if (!latlng.is_valid()) {
        LOG_ERROR("latlng is invalid. lat_degrees={}, lng_degrees={}", lat_degrees, lng_degrees);
        return PERR_GEO_INVALID_LATLNG_ERROR;
    }
    dsn::utils::notify_event search_completed;
    async_search_radial(latlng,
                        radius_m,
                        count,
                        sort_type,
                        timeout_ms,
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
                                     int timeout_ms,
                                     geo_search_callback_t &&callback)
{
    S2LatLng latlng = S2LatLng::FromDegrees(lat_degrees, lng_degrees);
    if (!latlng.is_valid()) {
        LOG_ERROR("latlng is invalid. lat_degrees={}, lng_degrees={}", lat_degrees, lng_degrees);
        callback(PERR_GEO_INVALID_LATLNG_ERROR, {});
    }

    async_search_radial(latlng, radius_m, count, sort_type, timeout_ms, std::move(callback));
}

int geo_client::search_radial(const std::string &hash_key,
                              const std::string &sort_key,
                              double radius_m,
                              int count,
                              SortType sort_type,
                              int timeout_ms,
                              std::list<SearchResult> &result)
{
    int ret = PERR_OK;
    dsn::utils::notify_event search_completed;
    async_search_radial(hash_key,
                        sort_key,
                        radius_m,
                        count,
                        sort_type,
                        timeout_ms,
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
                                     int timeout_ms,
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
         timeout_ms,
         cb = std::move(callback)](
            int ec_, std::string &&value_, pegasus_client::internal_info &&) mutable {
            if (ec_ != PERR_OK) {
                LOG_ERROR("get failed. hash_key={}, sort_key={}, error={}",
                          utils::redact_sensitive_string(hash_key),
                          utils::redact_sensitive_string(sort_key),
                          get_error_string(ec_));
                cb(ec_, {});
                return;
            }

            S2LatLng latlng;
            if (!_codec.decode_from_value(value_, latlng)) {
                LOG_ERROR("decode_from_value failed. hash_key={}, sort_key={}, value={}",
                          utils::redact_sensitive_string(hash_key),
                          utils::redact_sensitive_string(sort_key),
                          value_);
                cb(ec_, {});
                return;
            }

            async_search_radial(
                latlng, radius_m, count, sort_type, (int)ceil(timeout_ms * 0.8), std::move(cb));
        },
        (int)ceil(timeout_ms * 0.2));
}

void geo_client::async_search_radial(const S2LatLng &latlng,
                                     double radius_m,
                                     int count,
                                     SortType sort_type,
                                     int timeout_ms,
                                     geo_search_callback_t &&callback)
{
    // generate a cap
    std::shared_ptr<S2Cap> cap_ptr = std::make_shared<S2Cap>();
    gen_search_cap(latlng, radius_m, *cap_ptr);

    // generate cell ids
    S2CellUnion cids;
    gen_cells_covered_by_cap(*cap_ptr, cids);

    // search data in the cell ids
    async_get_result_from_cells(cids,
                                cap_ptr,
                                count,
                                sort_type,
                                timeout_ms,
                                [this, count, sort_type, cb = std::move(callback)](
                                    std::list<std::list<SearchResult>> &&results_) {
                                    std::list<SearchResult> result;
                                    normalize_result(std::move(results_), count, sort_type, result);
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
    rc.mutable_options()->set_fixed_level(FLAGS_min_level);
    cids = rc.GetCovering(cap);
}

void geo_client::async_get_result_from_cells(const S2CellUnion &cids,
                                             std::shared_ptr<S2Cap> cap_ptr,
                                             int count,
                                             SortType sort_type,
                                             int timeout_ms,
                                             scan_all_area_callback_t &&callback)
{
    int single_scan_count = count;
    if (sort_type == SortType::asc || sort_type == SortType::desc) {
        single_scan_count = -1; // scan all data to make full sort
    }

    // scan all cell ids
    std::shared_ptr<std::list<std::list<SearchResult>>> results =
        std::make_shared<std::list<std::list<SearchResult>>>();
    std::shared_ptr<std::atomic<bool>> send_finish = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<std::atomic<int>> scan_count = std::make_shared<std::atomic<int>>(0);
    auto single_scan_finish_callback =
        [send_finish, scan_count, results, cb = std::move(callback)]() {
            // NOTE: make sure fetch_sub is at first of the if expression to make it always execute
            if (scan_count->fetch_sub(1) == 1 && send_finish->load()) {
                cb(std::move(*results.get()));
            }
        };

    for (const auto &cid : cids) {
        if (cap_ptr->Contains(S2Cell(cid))) {
            // for the full contained cell, scan all data in this cell(which is at the
            // FLAGS_min_level)
            results->emplace_back(std::list<SearchResult>());
            scan_count->fetch_add(1);
            start_scan(cid.ToString(),
                       "",
                       "",
                       cap_ptr,
                       single_scan_count,
                       timeout_ms,
                       single_scan_finish_callback,
                       results->back());
        } else {
            // for the partial contained cell, scan cells covered by the cap at the FLAGS_max_level
            // which is more accurate than the ones at FLAGS_min_level, but it will cost more time
            // on calculating here.
            std::string hash_key = cid.parent(FLAGS_min_level).ToString();
            std::pair<std::string, std::string> start_stop_sort_keys;
            S2CellId pre;
            // traverse all sub cell ids of `cid` on FLAGS_max_level along the Hilbert curve, to
            // find the needed ones.
            for (S2CellId cur = cid.child_begin(FLAGS_max_level);
                 cur != cid.child_end(FLAGS_max_level);
                 cur = cur.next()) {
                if (cap_ptr->MayIntersect(S2Cell(cur))) {
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
                            results->emplace_back(std::list<SearchResult>());
                            scan_count->fetch_add(1);
                            start_scan(hash_key,
                                       std::move(start_stop_sort_keys.first),
                                       std::move(start_stop_sort_keys.second),
                                       cap_ptr,
                                       single_scan_count,
                                       timeout_ms,
                                       single_scan_finish_callback,
                                       results->back());

                            start_stop_sort_keys.first = gen_start_sort_key(cur, hash_key);
                            start_stop_sort_keys.second.clear();
                        }
                        pre = cur;
                    }
                }
            }

            CHECK(!start_stop_sort_keys.first.empty(), "");
            // the last sub slice of current `cid` on FLAGS_max_level in Hilbert curve covered by
            // `cap`
            if (start_stop_sort_keys.second.empty()) {
                start_stop_sort_keys.second = gen_stop_sort_key(pre, hash_key);
                results->emplace_back(std::list<SearchResult>());
                scan_count->fetch_add(1);
                start_scan(hash_key,
                           std::move(start_stop_sort_keys.first),
                           std::move(start_stop_sort_keys.second),
                           cap_ptr,
                           single_scan_count,
                           timeout_ms,
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

void geo_client::normalize_result(std::list<std::list<SearchResult>> &&results,
                                  int count,
                                  SortType sort_type,
                                  std::list<SearchResult> &result)
{
    result.clear();
    for (auto &r : results) {
        result.splice(result.end(), r);
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
    if (!_codec.decode_from_value(value, latlng)) {
        LOG_ERROR("decode_from_value failed. hash_key={}, sort_key={}, value={}",
                  utils::redact_sensitive_string(hash_key),
                  utils::redact_sensitive_string(sort_key),
                  value);
        return false;
    }

    // generate hash key
    S2CellId leaf_cell_id = S2Cell(latlng).id();
    S2CellId parent_cell_id = leaf_cell_id.parent(FLAGS_min_level);
    geo_hash_key = parent_cell_id.ToString(); // [0,5]{1}/[0,3]{FLAGS_min_level}

    // generate sort key
    dsn::blob sort_key_postfix;
    pegasus_generate_key(sort_key_postfix, hash_key, sort_key);
    geo_sort_key = leaf_cell_id.ToString().substr(geo_hash_key.length()) + ":" +
                   sort_key_postfix.to_string(); // [0,3]{30-FLAGS_min_level}:combine_keys

    return true;
}

bool geo_client::restore_origin_keys(const std::string &geo_sort_key,
                                     std::string &origin_hash_key,
                                     std::string &origin_sort_key)
{
    // geo_sort_key: [0,3]{30-FLAGS_min_level}:combine_keys
    int cid_prefix_len = 30 - FLAGS_min_level + 1; // '1' is for ':' in geo_sort_key
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
                                       int timeout_ms,
                                       int ttl_seconds)
{
    _common_data_client->async_set(
        hash_key,
        sort_key,
        value,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::common);
        },
        timeout_ms,
        ttl_seconds);
}

void geo_client::async_set_geo_data(const std::string &hash_key,
                                    const std::string &sort_key,
                                    const std::string &value,
                                    update_callback_t &&callback,
                                    int timeout_ms,
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
        timeout_ms,
        ttl_seconds);
}

void geo_client::async_del_common_data(const std::string &hash_key,
                                       const std::string &sort_key,
                                       update_callback_t &&callback,
                                       int timeout_ms)
{
    _common_data_client->async_del(
        hash_key,
        sort_key,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::common);
        },
        timeout_ms);
}

void geo_client::async_del_geo_data(const std::string &geo_hash_key,
                                    const std::string &geo_sort_key,
                                    update_callback_t &&callback,
                                    int timeout_ms)
{
    _geo_data_client->async_del(
        geo_hash_key,
        geo_sort_key,
        [cb = std::move(callback)](int error_code, pegasus_client::internal_info &&info) {
            cb(error_code, std::move(info), DataType::geo);
        },
        timeout_ms);
}

void geo_client::start_scan(const std::string &hash_key,
                            std::string &&start_sort_key,
                            std::string &&stop_sort_key,
                            std::shared_ptr<S2Cap> cap_ptr,
                            int count,
                            int timeout_ms,
                            scan_one_area_callback_t &&callback,
                            std::list<SearchResult> &result)
{
    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    options.batch_size = 1000;
    options.timeout_ms = timeout_ms;

    _geo_data_client->async_get_scanner(
        hash_key,
        start_sort_key,
        stop_sort_key,
        options,
        [this, cap_ptr, count, cb = std::move(callback), &result](
            int error_code, pegasus_client::pegasus_scanner *hash_scanner) mutable {
            if (error_code == PERR_OK) {
                do_scan(hash_scanner->get_smart_wrapper(), cap_ptr, count, std::move(cb), result);
            } else {
                cb();
            }
        });
}

void geo_client::do_scan(pegasus_client::pegasus_scanner_wrapper scanner_wrapper,
                         std::shared_ptr<S2Cap> cap_ptr,
                         int count,
                         scan_one_area_callback_t &&callback,
                         std::list<SearchResult> &result)
{
    scanner_wrapper->async_next(
        [this, cap_ptr, count, scanner_wrapper, cb = std::move(callback), &result](
            int ret,
            std::string &&geo_hash_key,
            std::string &&geo_sort_key,
            std::string &&value,
            pegasus_client::internal_info &&info,
            uint32_t expire_ts_seconds,
            int32_t kv_count) mutable {
            if (ret == PERR_SCAN_COMPLETE) {
                cb();
                return;
            }

            if (ret != PERR_OK) {
                LOG_ERROR("async_next failed. error={}", get_error_string(ret));
                cb();
                return;
            }

            S2LatLng latlng;
            if (!_codec.decode_from_value(value, latlng)) {
                LOG_ERROR("decode_from_value failed. value={}", value);
                cb();
                return;
            }

            double distance = S2Earth::GetDistanceMeters(S2LatLng(cap_ptr->center()), latlng);
            if (distance <= S2Earth::ToMeters(cap_ptr->radius())) {
                std::string origin_hash_key, origin_sort_key;
                if (!restore_origin_keys(geo_sort_key, origin_hash_key, origin_sort_key)) {
                    LOG_ERROR("restore_origin_keys failed. geo_sort_key={}",
                              utils::redact_sensitive_string(geo_sort_key));
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

            do_scan(scanner_wrapper, cap_ptr, count, std::move(cb), result);
        });
}

int geo_client::distance(const std::string &hash_key1,
                         const std::string &sort_key1,
                         const std::string &hash_key2,
                         const std::string &sort_key2,
                         int timeout_ms,
                         double &distance)
{
    int ret = PERR_OK;
    dsn::utils::notify_event get_completed;
    auto async_calculate_callback = [&](int ec_, double &&distance_) {
        if (ec_ != PERR_OK) {
            LOG_ERROR(
                "get distance failed. hash_key1={}, sort_key1={}, hash_key2={}, sort_key2={}, "
                "error={}",
                utils::redact_sensitive_string(hash_key1),
                utils::redact_sensitive_string(sort_key1),
                utils::redact_sensitive_string(hash_key2),
                utils::redact_sensitive_string(sort_key2),
                get_error_string(ec_));
            ret = ec_;
        }
        distance = distance_;
        get_completed.notify();
    };
    async_distance(
        hash_key1, sort_key1, hash_key2, sort_key2, timeout_ms, async_calculate_callback);
    get_completed.wait();

    return ret;
}

void geo_client::async_distance(const std::string &hash_key1,
                                const std::string &sort_key1,
                                const std::string &hash_key2,
                                const std::string &sort_key2,
                                int timeout_ms,
                                distance_callback_t &&callback)
{
    std::shared_ptr<int> ret = std::make_shared<int>(PERR_OK);
    std::shared_ptr<std::mutex> mutex = std::make_shared<std::mutex>();
    std::shared_ptr<std::vector<S2LatLng>> get_result = std::make_shared<std::vector<S2LatLng>>();
    auto async_get_callback = [=, cb = std::move(callback)](
                                  int ec_, std::string &&value_, pegasus_client::internal_info &&) {
        if (ec_ != PERR_OK) {
            LOG_ERROR("get data failed. hash_key1={}, sort_key1={}, hash_key2={}, sort_key2={}, "
                      "error={}",
                      utils::redact_sensitive_string(hash_key1),
                      utils::redact_sensitive_string(sort_key1),
                      utils::redact_sensitive_string(hash_key2),
                      utils::redact_sensitive_string(sort_key2),
                      get_error_string(ec_));
            *ret = ec_;
        }

        S2LatLng latlng;
        if (!_codec.decode_from_value(value_, latlng)) {
            LOG_ERROR("decode_from_value failed. value={}", value_);
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

    _common_data_client->async_get(hash_key1, sort_key1, async_get_callback, timeout_ms);
    _common_data_client->async_get(hash_key2, sort_key2, async_get_callback, timeout_ms);
}

} // namespace geo
} // namespace pegasus
