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

    pegasus_client *client = pegasus_client_factory::get_client(argv[1], argv[2]);

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));
    for (int i = 0; i < 10; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(rect));

        // leaf cell
        S2Cell cell(latlng);

        // leaf(level-30) cell id
        S2CellId cell_id = cell.id();

        // convert to a parent level cell
        S2CellId parent_cell_id = cell_id.parent(10); // level 10 error about 10 km

        S2Cell parent_cell(parent_cell_id);

        std::string hash_key(parent_cell_id.ToString());
        std::string sort_key(cell_id.ToString() + ":" + std::to_string(i));
        std::string value(latlng.ToStringInDegrees() + ":" + std::to_string(i));

        std::cout << " hash_key: " << hash_key
                  << " sort_key: " << sort_key
                  << " value: " << value << std::endl;

        int ret = client->set(hash_key, sort_key, value);
        if (ret != PERR_OK) {
            fprintf(stderr, "ERROR: set failed, error=%s\n", client->get_error_string(ret));
            return -1;
        }
    }

/*    // region cover
    S2RegionCoverer rc;
    //  rc.mutable_options()->set_min_level(7);
    //  rc.mutable_options()->set_max_level(24);
    rc.mutable_options()->set_min_level(14);
    rc.mutable_options()->set_max_level(14);
    rc.mutable_options()->set_max_cells(30);

    S2CellUnion cell_union;
    // cap
    S2Cap cap(S2LatLng::FromDegrees(40.039752, 116.332557).ToPoint(),
              S2Earth::ToAngle(util::units::Meters(5000.0)));

    int min_level = 10;
    int max_level = 14;
    rc.mutable_options()->set_min_level(min_level);
    rc.mutable_options()->set_max_level(max_level);
    S2CellUnion cell_union1 = rc.GetCovering(cap); // GetInteriorCovering
    std::cout << "cap: " << cap << std::endl;
    std::cout << "cell_union11: " << cell_union1.size()
              << ", normal: " << cell_union1.IsNormalized() << std::endl;
    std::vector<S2CellId> cell_ids;
    for (auto &ci : cell_union1) {
        if (ci.level() != min_level && ci.level() != max_level) {
            for (S2CellId c = ci.child_begin(max_level); c != ci.child_end(max_level);
                 c = c.next()) {
                cell_ids.push_back(c);
            }
        } else {
            std::cout << ci.level() << ": " << ci.id() << std::endl;
            cell_ids.push_back(ci);
        }
    }

    std::cout << "cell_ids11.size: " << cell_ids.size() << std::endl;
    //  for (auto ci : cell_ids) {
    //      if (ci.level() != max_level) {
    //          std::cout << ci.level() << ": " << ci.id() << std::endl;
    //      }
    //  }

    std::cout << "===================" << std::endl;
    rc.mutable_options()->set_fixed_level(10);
    S2CellUnion cell_union11 = rc.GetCovering(cap);
    std::cout << "size: " << cell_union11.size() << std::endl;
    for (auto &ci : cell_union11) {
        if (cap.Contains(S2Cell(ci))) {
            std::cout << "full: " << ci.level() << ": " << ci << std::endl;
        } else {
            std::vector<S2CellId> cell_ids11;
            for (S2CellId c = ci.child_begin(18); c != ci.child_end(18); c = c.next()) {
                if (cap.MayIntersect(S2Cell(c))) {
                    cell_ids11.push_back(c);
                }
            }

            std::pair<std::string, std::string> begin_end;
            std::list<std::pair<std::string, std::string>> begin_ends;

            S2CellId pre;
            for (auto &cur : cell_ids11) {
                if (!pre.is_valid()) {
                    pre = cur;
                    begin_end.first = pre.ToString();
                } else {
                    if (pre.next() != cur) {
                        begin_end.second = pre.ToString();
                        begin_ends.push_back(begin_end);
                        begin_end.first = cur.ToString();
                    }
                    pre = cur;
                }
                // std::cout << cur.level() << ": " << cur << std::endl;
            }
            std::cout << "begin_ends size: " << begin_ends.size() << std::endl;
            for (auto be : begin_ends) {
                std::cout << be.first << "->" << be.second << std::endl;
            }
        }
    }

    util::units::Meters meters = S2Earth::GetDistance(S2LatLng::FromDegrees(40.041362, 116.329419),
                                                      S2LatLng::FromDegrees(40.036918, 116.32929));
    std::cout << "meters: " << meters << std::endl;*/

    return 0;
}
