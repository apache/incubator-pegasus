#include "data_store.h"
#include <algorithm>
#include <dsn/perf_counter/perf_counter.h>

#define MAX_STORE_SIZE 100

namespace pegasus {
namespace server {
class Hotspot_policy
{
public:
    virtual void detect_hotspot_policy(std::vector<std::queue<data_store>> *data_stores,
                                       std::vector<dsn::perf_counter_wrapper> *hot_points) = 0;
};

class Algo1 : public Hotspot_policy
{
public:
    void detect_hotspot_policy(std::vector<std::queue<data_store>> *data_stores,
                               std::vector<dsn::perf_counter_wrapper> *hot_points)
    {
        std::vector<std::queue<data_store>> temp = *data_stores;
        std::vector<data_store> anly_data;
        double min_total_qps = 1.0, min_total_cu = 1.0;
        int index = 0;
        while (index != temp.size()) {
            anly_data.push_back(temp[index].back());
            min_total_qps = std::min(min_total_qps, std::max(anly_data[index].total_qps, 1.0));
            min_total_cu = std::min(min_total_cu, std::max(anly_data[index].total_cu, 1.0));
            index++;
        }
        for (int i = 0; i < anly_data.size(); i++) {
            hot_points->at(i)->set(anly_data[i].total_qps / min_total_qps);
        }
        return;
    }
};

class Hotpot_calculator
{
public:
    std::vector<std::queue<data_store>> data_stores;
    Hotpot_calculator(const std::string &name, const int &app_size);
    void aggregate(std::vector<row_data> partitions);
    void start_alg();
    void init_perf_counter();
    const std::string app_name;

private:
    Hotspot_policy *_policy;
    std::vector<::dsn::perf_counter_wrapper> _hotpot_points;
};
}
}
