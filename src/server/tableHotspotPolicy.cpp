#include "tableHotspotPolicy.h"

namespace pegasus {
namespace server {

Hotpot_calculator::Hotpot_calculator(const std::string &name, const int &partition_num)
    : data_stores(partition_num), app_name(name), _hotpot_points(partition_num)
{
}

void Hotpot_calculator::aggregate(std::vector<row_data> partitions)
{
    for (int i = 0; i < partitions.size(); i++) {
        while (this->data_stores[i].size() > MAX_STORE_SIZE) {
            this->data_stores[i].pop();
        }
        data_store temp(partitions[i], this->app_name);
        this->data_stores[i].emplace(temp);
    }
}

void Hotpot_calculator::init_perf_counter()
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

void Hotpot_calculator::get_hotpot_point_value(std::vector<double> &result){
    result.assign(this->_hotpot_point_value.begin(),this->_hotpot_point_value.end());
}

void Hotpot_calculator::set_result_to_falcon(){
    if (_hotpot_points.size()!=_hotpot_point_value.size()){
        ddebug("partittion counts error, please check");
        return ;
    }
    for (int i=0;i<_hotpot_points.size();i++)
        _hotpot_points.at(i)->set(_hotpot_point_value[i]);
}

void Hotpot_calculator::start_alg()
{
    _policy = new Algo1();
    _policy->detect_hotspot_policy(&(this->data_stores), &(this->_hotpot_point_value));
}
}
}
