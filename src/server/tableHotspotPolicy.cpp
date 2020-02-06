#pragma once

#include "tableHotspotPolicy.h"

namespace pegasus {
namespace server {

hotspot_calculator::hotspot_calculator(const std::string &app_name, const int &partition_num)
    : data_stores(partition_num),
      app_name(app_name),
      _hotpot_point_value(partition_num),
      _hotpot_points(partition_num)
{
}

void hotspot_calculator::aggregate(std::vector<row_data> &partitions)
{
    for (int i = 0; i < partitions.size(); i++) {
        while (this->data_stores.at(i).size() > MAX_STORE_SIZE - 1) {
            this->data_stores.at(i).pop();
        }
        this->data_stores[i].emplace(data_store(partitions[i], this->app_name));
    }
}

void hotspot_calculator::init_perf_counter()
{
    char counter_name[1024];
    char counter_desc[1024];
    for (int i = 0; i < this->_hotpot_points.size(); i++) {
        string paritition_desc = this->app_name + std::to_string(i);
        sprintf(counter_name, "app.stat.hotspots.%s", paritition_desc.c_str());
        sprintf(counter_desc, "statistic the hotspots of app %s", paritition_desc.c_str());
        _hotpot_points[i].init_app_counter(
            "app.pegasus", counter_name, COUNTER_TYPE_NUMBER, counter_desc);
    }
}

void hotspot_calculator::get_hotpot_point_value(std::vector<double> &result)
{
    result.assign(this->_hotpot_point_value.begin(), this->_hotpot_point_value.end());
}

void hotspot_calculator::set_result_to_falcon()
{
    if (_hotpot_points.size() != _hotpot_point_value.size()) {
        ddebug("partittion counts error, please check");
        return;
    }
    for (int i = 0; i < _hotpot_points.size(); i++)
        _hotpot_points.at(i)->set(_hotpot_point_value[i]);
}

void hotspot_calculator::start_alg()
{
    _policy = new Algo1();
    std::cout << "Start Algo1 0" << std::endl;
    _policy->detect_hotspot_policy(&(this->data_stores), &(this->_hotpot_point_value));
}
}
}
