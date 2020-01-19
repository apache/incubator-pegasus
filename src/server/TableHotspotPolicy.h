#include "data_store.h"
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
        std::vector<std::queue<data_store>> anly_data = *_data_stores;
        for (int i = 0; i < (_data_stores->size()); i++) {
            ddebug("partition_name %s total_get_qps %lf total_recent_read_cu %lf ",
                   anly_data[i].back().app_name.c_str(),
                   anly_data[i].back().total_qps,
                   anly_data[i].back().total_cu);
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
