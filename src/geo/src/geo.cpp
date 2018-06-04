// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo.h"

#include <dsn/service_api_cpp.h>
#include <s2/s2earth.h>
#include <s2/s2testing.h>
#include <s2/s2region_coverer.h>
#include <s2/s2cap.h>

namespace pegasus {

DEFINE_TASK_CODE(LPC_SCAN_DATA, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

int geo::init(dsn::string_view config_file,
              dsn::string_view cluster_name,
              dsn::string_view app_name)
{
    bool ok = pegasus_client_factory::initialize(config_file.data());
    dassert(ok, "init pegasus client factory failed");

    _client = pegasus_client_factory::get_client(cluster_name.data(), app_name.data());
    dassert(_client != nullptr, "init pegasus client failed");

    return PERR_OK;
}

int geo::search_radial(double lat_degrees,
                       double lng_degrees,
                       double radius_m,
                       int count,
                       int sort_type,
                       std::list<std::pair<std::string, double>> &result)
{
    util::units::Meters radius((float)radius_m);
    S2Cap cap(S2LatLng::FromDegrees(lat_degrees, lng_degrees).ToPoint(), S2Earth::ToAngle(radius));

    // region cover
    S2RegionCoverer rc;
    rc.mutable_options()->set_fixed_level(min_level);
    S2CellUnion cell_union = rc.GetCovering(cap);
    for (const auto &ci : cell_union) {
        if (cap.Contains(S2Cell(ci))) {
            int ret =
                scan_data(ci.ToString(), "", "", S2LatLng(cap.center()), radius, count, result);
            if (ret != pegasus::PERR_OK) {
                std::cerr << "ERROR: scan_data failed, error=" << _client->get_error_string(ret)
                          << std::endl;
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
                int ret = scan_data(
                    hash_key, be.first, be.second, S2LatLng(cap.center()), radius, count, result);
                if (ret != pegasus::PERR_OK) {
                    std::cerr << "ERROR: scan_data failed, error=" << _client->get_error_string(ret)
                              << std::endl;
                    return -1;
                }
            }
        }
    }
    _tracker.cancel_outstanding_tasks();

    if (count > 0) {
        result.resize((size_t)count);
    }

    return PERR_OK;
}

void geo::TEST_fill_data_in_rect(S2LatLngRect rect, int count)
{
    for (int i = 0; i < count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(rect));

        // leaf cell
        S2Cell cell(latlng);
        S2CellId cell_id = cell.id();

        // convert to a parent level cell
        S2CellId parent_cell_id = cell_id.parent(min_level);
        S2Cell parent_cell(parent_cell_id);

        std::string hash_key(parent_cell_id.ToString());
        std::string sort_key(cell_id.ToString().substr(hash_key.length()) + ":" +
                             std::to_string(i));
        std::string value(latlng.ToStringInDegrees() + ":" + std::to_string(i));

        int ret = _client->set(hash_key, sort_key, value);
        if (ret != pegasus::PERR_OK) {
            derror("set data failed. error=%s", _client->get_error_string(ret));
        }
    }
}

int geo::scan_next(const S2LatLng &center,
                   util::units::Meters radius,
                   int count,
                   std::list<std::pair<std::string, double>> &result,
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
                derror("async_next failed. error=%s", _client->get_error_string(ret));
                return;
            }

            size_t pos1 = scan_value.find(',', 0);
            size_t pos2 = scan_value.find(':', 0);
            std::string lat = scan_value.substr(0, pos1);
            std::string lng = scan_value.substr(pos1 + 1, pos2 - pos1 - 1);
            util::units::Meters distance = S2Earth::GetDistance(
                center,
                S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr), strtod(lng.c_str(), nullptr)));
            if (distance.value() <= radius.value()) {
                result.push_back(std::move(std::make_pair(scan_value, distance.value())));
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
                   std::list<std::pair<std::string, double>> &result)
{
    dsn::tasking::enqueue(
        LPC_SCAN_DATA,
        &_tracker,
        [this, hash_key, start_sort_key, stop_sort_key, center, radius, count, &result]() {
            pegasus_client::scan_options options;
            options.start_inclusive = true;
            options.stop_inclusive = true;
            _client->async_get_scanner(
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