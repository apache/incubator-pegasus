#include "TableHotspotPolicy.h"

namespace pegasus {
namespace server {

Hotpot_calculator::Hotpot_calculator(const std::string &name, const int &app_size)
    : data_stores(app_size), _app_name(name)
{
}

void Hotpot_calculator::aggregate(std::vector<row_data> partitions)
{
    for (int i = 0; i < partitions.size(); i++) {
        while (data_stores[i].size() > MAX_STORE_SIZE)
            data_stores[i].pop();
        data_store temp;
        temp.aggregate(partitions[i]);
        data_stores[i].emplace(temp);
    }
}

void Hotpot_calculator::start_alg()
{
    _policy = new Algo1(&data_stores, &_hotpot_points);
    _policy->detect_hotspot_policy();
}
}
}
