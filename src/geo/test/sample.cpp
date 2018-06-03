// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <iostream>
#include <iomanip>
#include <list>

#include <s2/s2latlng.h>
#include <s2/s2cell.h>
#include <s2/s2earth.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2cap.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2testing.h>

#include <pegasus/client.h>

using namespace pegasus;

static const int data_count = 10000;
static const int test_count = 1;
static const int min_level = 12; // edge length at about
static const int max_level = 16; // edge length at about

pegasus_client *client = nullptr;

int scan_data(const std::string &hash_key,
              const std::string &start_sort_key,
              const std::string &stop_sort_key,
              const S2LatLng center,
              util::units::Meters radius,
              std::list<std::string> &datas)
{
    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;

    client->async_get_scanner(
        hash_key,
        start_sort_key,
        stop_sort_key,
        options,
        [center, radius, &datas](int ret, pegasus_client::pegasus_scanner *scanner) {
            if (ret == PERR_OK) {
                pegasus_client::pegasus_scanner_wrapper wrap_scanner = scanner->get_smart_wrapper();
                wrap_scanner->async_next(
                    [center, radius, &datas](int ret,
                                             std::string &&hash_key,
                                             std::string &&sort_key,
                                             std::string &&scan_value,
                                             pegasus::pegasus_client::internal_info &&info) {
                        if (ret == pegasus::PERR_OK) {
                            if (radius.value() > 0) {
                                size_t pos1 = scan_value.find(',', 0);
                                size_t pos2 = scan_value.find(':', 0);
                                std::string lat = scan_value.substr(0, pos1);
                                std::string lng = scan_value.substr(pos1 + 1, pos2 - pos1 - 1);
                                util::units::Meters meters = S2Earth::GetDistance(
                                    center,
                                    S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr),
                                                          strtod(lng.c_str(), nullptr)));
                                if (meters.value() <= radius.value()) {
                                    datas.push_back(std::move(scan_value));
                                }
                            } else {
                                datas.push_back(std::move(scan_value));
                            }
                        } else if (ret == pegasus::PERR_SCAN_COMPLETE) {
                        } else {
                        }
                    });
            }
        });
    return PERR_OK;
}

// ./pegasus_geo_test onebox temp
int main(int argc, char **argv)
{
    if (!pegasus_client_factory::initialize("config.ini")) {
        std::cerr << "ERROR: init pegasus failed" << std::endl;
        return -1;
    }

    if (argc != 3) {
        std::cerr << "USAGE: " << argv[0] << "<cluster-name> <app-name>" << std::endl;
        return -1;
    }

    client = pegasus_client_factory::get_client(argv[1], argv[2]);

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));
    for (int i = 0; i < data_count; ++i) {
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

        int ret = client->set(hash_key, sort_key, value);
        if (ret != PERR_OK) {
            std::cerr << "ERROR: set failed, error=" << client->get_error_string(ret) << std::endl;
            return -1;
        }
    }

    for (int i = 0; i < test_count; ++i) {
        util::units::Meters radius(5000.0);
        S2Cap cap(S2Testing::SamplePoint(rect), S2Earth::ToAngle(radius));

        std::list<std::string> datas;
        // region cover
        S2RegionCoverer rc;
        rc.mutable_options()->set_fixed_level(min_level);
        S2CellUnion cell_union = rc.GetCovering(cap);
        for (const auto &ci : cell_union) {
            if (cap.Contains(S2Cell(ci))) {
                std::cout << "full: " << ci.level() << ": " << ci << std::endl;
                int ret = scan_data(ci.ToString(), "", "", S2LatLng(cap.center()), radius, datas);
                if (ret != PERR_OK) {
                    std::cerr << "ERROR: scan_data failed, error=" << client->get_error_string(ret)
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
                        hash_key, be.first, be.second, S2LatLng(cap.center()), radius, datas);
                    if (ret != PERR_OK) {
                        std::cerr << "ERROR: scan_data failed, error="
                                  << client->get_error_string(ret) << std::endl;
                        return -1;
                    }
                }
            }
        }
    }

    return 0;
}
