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

namespace pegasus {

DEFINE_TASK_CODE(LPC_SCAN_DATA, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

int geo::init(dsn::string_view config_file,
              dsn::string_view cluster_name,
              dsn::string_view common_app_name,
              dsn::string_view geo_app_name)
{
    bool ok = pegasus_client_factory::initialize(config_file.data());
    dassert(ok, "init pegasus client factory failed");

    _common_data_client =
        pegasus_client_factory::get_client(cluster_name.data(), common_app_name.data());
    dassert(_common_data_client != nullptr, "init pegasus _common_data_client failed");

    _geo_data_client = pegasus_client_factory::get_client(cluster_name.data(), geo_app_name.data());
    dassert(_geo_data_client != nullptr, "init pegasus _geo_data_client failed");

    return PERR_OK;
}

int geo::set_with_geo(const std::string &hashkey,
                      const std::string &sortkey,
                      const std::string &value,
                      int timeout_milliseconds,
                      int ttl_seconds,
                      pegasus_client::internal_info *info)
{
    // TODO 异步并行set
    int ret = set_common_data(hashkey, sortkey, value, timeout_milliseconds / 2, ttl_seconds, info);
    if (ret != PERR_OK) {
        derror_f("set_common_data failed. hashkey={} sortkey={}", hashkey, sortkey);
        return ret;
    }

    S2LatLng latlng;
    ret = extract_latlng(value, latlng);
    if (ret != PERR_OK) {
        derror_f("extract_latlng failed. value={}", value);
        return ret;
    }

    ret = set_geo_data(latlng, hashkey, value, timeout_milliseconds / 2, ttl_seconds);
    if (ret != PERR_OK) {
        derror_f("set_geo_data failed. hashkey={} sortkey={}", hashkey, sortkey);
        return ret;
    }

    return PERR_OK;
}

int geo::search_radial(double lat_degrees,
                       double lng_degrees,
                       double radius_m,
                       int count,
                       SortType sort_type,
                       std::vector<std::pair<std::string, double>> &result)
{
    util::units::Meters radius((float)radius_m);
    S2Cap cap(S2LatLng::FromDegrees(lat_degrees, lng_degrees).ToPoint(), S2Earth::ToAngle(radius));

    int single_scan_count = count;
    if (sort_type == SortType::nearest) {
        single_scan_count = -1;
    }

    std::list<std::vector<std::pair<std::string, double>>> results;

    // region cover
    S2RegionCoverer rc;
    rc.mutable_options()->set_fixed_level(min_level);
    S2CellUnion cell_union = rc.GetCovering(cap);
    for (const auto &ci : cell_union) {
        if (cap.Contains(S2Cell(ci))) {
            results.emplace_back(std::vector<std::pair<std::string, double>>());
            int ret = scan_data(ci.ToString(),
                                "",
                                "",
                                S2LatLng(cap.center()),
                                radius,
                                single_scan_count,
                                *results.rbegin());
            if (ret != pegasus::PERR_OK) {
                derror_f("scan_data failed, error={}", _geo_data_client->get_error_string(ret));
                return -1;
            }
        } else {
            std::string hash_key = ci.parent(min_level).ToString();
            std::vector<S2CellId> cell_ids;
            for (S2CellId max_level_cid = ci.child_begin(max_level);
                 max_level_cid != ci.child_end(max_level);
                 max_level_cid = max_level_cid.next()) {
                if (cap.MayIntersect(S2Cell(max_level_cid))) {
                    cell_ids.push_back(max_level_cid);
                }
            }

            std::pair<std::string, std::string> begin_end;
            std::list<std::pair<std::string, std::string>> begin_ends;

            S2CellId pre;
            for (auto &cur : cell_ids) {
                if (!pre.is_valid()) {
                    pre = cur;
                    begin_end.first = pre.ToString().substr(hash_key.length());
                } else {
                    if (pre.next() != cur) {
                        begin_end.second = pre.ToString().substr(hash_key.length());
                        begin_ends.push_back(begin_end);
                        begin_end.first = cur.ToString().substr(hash_key.length());
                    }
                    pre = cur;
                }
            }
            for (auto &be : begin_ends) {
                results.emplace_back(std::vector<std::pair<std::string, double>>());
                int ret = scan_data(hash_key,
                                    be.first,
                                    be.second,
                                    S2LatLng(cap.center()),
                                    radius,
                                    single_scan_count,
                                    *results.rbegin());
                if (ret != pegasus::PERR_OK) {
                    derror_f("scan_data failed, error={}", _geo_data_client->get_error_string(ret));
                    return -1;
                }
            }
        }
    }
    _tracker.cancel_outstanding_tasks();

    // TODO need optimize
    for (auto &r : results) {
        result.insert(result.end(), r.begin(), r.end());
    }
    if (sort_type == SortType::nearest) {
        std::sort(result.begin(),
                  result.end(),
                  [](const std::pair<std::string, double> &l,
                     const std::pair<std::string, double> &r) { return l.second <= r.second; });
    }

    if (count > 0) {
        result.resize((size_t)count);
    }

    return PERR_OK;
}

int geo::extract_latlng(const std::string &value, S2LatLng &latlng)
{
    std::vector<std::string> data;
    dsn::utils::split_args(value.c_str(), data, '|');
    if (data.size() <= 6) {
        return PERR_INVALID_VALUE;
    }

    std::string lat = data[4];
    std::string lng = data[5];
    latlng = S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr), strtod(lng.c_str(), nullptr));

    // TODO should add more check for strtod

    return PERR_OK;
}

int geo::set_common_data(const std::string &hashkey,
                         const std::string &sortkey,
                         const std::string &value,
                         int timeout_milliseconds,
                         int ttl_seconds,
                         pegasus_client::internal_info *info)
{
    int ret = PERR_OK;
    int retry_times = 0;
    do {
        if (retry_times > 0) {
            dwarn_f("retry set data. sleep {}ms", retry_times * 10);
            usleep(retry_times * 10 * 1000);
        }
        ret = _common_data_client->set(
            hashkey, sortkey, value, timeout_milliseconds, ttl_seconds, info);
    } while (ret != PERR_OK && retry_times++ < max_retry_times);

    return PERR_OK;
}

int geo::set_geo_data(const S2LatLng &latlng,
                      const std::string &key,
                      const std::string &value,
                      int timeout_milliseconds,
                      int ttl_seconds)
{
    // leaf cell
    S2CellId leaf_cell_id = S2Cell(latlng).id();

    // convert to a parent level cell
    S2CellId parent_cell_id = leaf_cell_id.parent(min_level);

    std::string hash_key(parent_cell_id.ToString()); // regex: [0,5]{1}/[0,3]{min_level}
    std::string sort_key(leaf_cell_id.ToString().substr(hash_key.length()) + ":" +
                         key); // [0,3]{30-min_level}

    int ret = PERR_OK;
    int retry_times = 0;
    do {
        if (retry_times > 0) {
            dwarn_f("retry set geo data. sleep {}ms", retry_times * 10);
            usleep(retry_times * 10 * 1000);
        }
        ret = _geo_data_client->set(hash_key, sort_key, value, timeout_milliseconds, ttl_seconds);
        if (ret != PERR_OK) {
            derror("set data failed. error=%s", _geo_data_client->get_error_string(ret));
        }

    } while (ret != PERR_OK && retry_times++ < max_retry_times);

    return ret;
}

int geo::scan_next(const S2LatLng &center,
                   util::units::Meters radius,
                   int count,
                   std::vector<std::pair<std::string, double>> &result,
                   const pegasus_client::pegasus_scanner_wrapper &wrap_scanner)
{
    wrap_scanner->async_next(
        [this, center, radius, count, &result, wrap_scanner](int ret,
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
            if (extract_latlng(scan_value, latlng) != PERR_OK) {
                derror_f("extract_latlng failed. scan_value={}", scan_value);
                return;
            }

            util::units::Meters distance = S2Earth::GetDistance(center, latlng);
            if (distance.value() <= radius.value()) {
                result.emplace_back(
                    std::pair<std::string, double>(std::move(scan_value), distance.value()));
            }

            if (count == -1 || result.size() < count) {
                scan_next(center, radius, count, result, wrap_scanner);
            }
        });

    return 0;
}

int geo::scan_data(const std::string &hash_key,
                   const std::string &start_sort_key,
                   const std::string &stop_sort_key,
                   const S2LatLng &center,
                   util::units::Meters radius,
                   int count,
                   std::vector<std::pair<std::string, double>> &result)
{
    dsn::tasking::enqueue(
        LPC_SCAN_DATA,
        &_tracker,
        [this, hash_key, start_sort_key, stop_sort_key, center, radius, count, &result]() {
            pegasus_client::scan_options options;
            options.start_inclusive = true;
            options.stop_inclusive = true;
            _geo_data_client->async_get_scanner(
                hash_key,
                start_sort_key,
                stop_sort_key,
                options,
                [this, center, radius, count, &result](int ret,
                                                       pegasus_client::pegasus_scanner *scanner) {
                    if (ret == PERR_OK) {
                        pegasus_client::pegasus_scanner_wrapper wrap_scanner =
                            scanner->get_smart_wrapper();
                        scan_next(center, radius, count, result, wrap_scanner);
                    }
                });

        });
    return PERR_OK;
}

} // namespace pegasus