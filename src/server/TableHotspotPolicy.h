#include "data_store.h"
#include <algorithm>
#include <dsn/perf_counter/perf_counter.h>

#define MAX_STORE_SIZE 100

namespace pegasus {
namespace server {
class Hotspot_policy
{
public:
    Hotspot_policy(std::vector<std::queue<data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : _data_stores(data_stores), _hot_points(hot_points)
    {
    }
    virtual void detect_hotspot_policy() = 0;

protected:
    std::vector<std::queue<data_store>> *_data_stores;
    std::vector<dsn::perf_counter> *_hot_points;
};

class Algo1 : public Hotspot_policy
{
public:
    explicit Algo1(std::vector<std::queue<data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : Hotspot_policy(data_stores, hot_points){};
    void detect_hotspot_policy()
    {
        ::dsn::perf_counter_wrapper hotspot_points_couter;
        std::vector<std::queue<data_store>> temp = *_data_stores;
        std::vector<double> hotspot_points;
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
            hotspot_points.push_back(anly_data[i].total_qps / min_total_qps);
            // hotspot_points[i] = anly_data[i].total_cu / min_total_cu;
            ddebug("hotspot_points %lf ", hotspot_points[hotspot_points.size() - 1]);
        }
        return;
    }
};

class Algo2 : public Hotspot_policy
{
public:
    explicit Algo2(std::vector<std::queue<data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : Hotspot_policy(data_stores, hot_points){};
    void detect_hotspot_policy() { return; }
};

class Hotpot_calculator
{
public:
    std::vector<std::queue<data_store>> data_stores;
    Hotpot_calculator(const std::string &name, const int &app_size);
    void aggregate(std::vector<row_data> partitions);
    void start_alg();
    const std::string app_name;

private:
    Hotspot_policy *_policy;
    std::vector<dsn::perf_counter> _hotpot_points;
};
}
}
