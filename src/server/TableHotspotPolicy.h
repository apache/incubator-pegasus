#include "data_store.h"
#include <dsn/perf_counter/perf_counter.h>

#define MAX_STORE_SIZE 100

namespace pegasus {
namespace server {
class Hotspot_policy
{
public:
    Hotspot_policy(std::vector<std::queue<Data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : _data_stores(data_stores), _hot_points(hot_points)
    {
    }
    virtual void detect_hotspot_policy() = 0;

protected:
    std::vector<std::queue<Data_store>> *_data_stores;
    std::vector<dsn::perf_counter> *_hot_points;
};

class Algo1 : public Hotspot_policy
{
public:
    explicit Algo1(std::vector<std::queue<Data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : Hotspot_policy(data_stores, hot_points){};
    void detect_hotspot_policy()
    {
        for (int i = 0; i < (_data_stores->size()); i++)
            (*_hot_points)[i].set((*_data_stores)[i].front().total_multi_get_qps);
        return;
    }
};

class Algo2 : public Hotspot_policy
{
public:
    explicit Algo2(std::vector<std::queue<Data_store>> *data_stores,
                   std::vector<dsn::perf_counter> *hot_points)
        : Hotspot_policy(data_stores, hot_points){};
    void detect_hotspot_policy() { return; }
};

class Hotpot_calculator
{
public:
    std::vector<std::queue<Data_store>> data_stores;
    Hotpot_calculator(const std::string &name, const int &app_size);
    void aggregate(std::vector<row_data> partitions);
    void start_alg();

private:
    std::string _name;
    Hotspot_policy *_policy;
    std::vector<dsn::perf_counter> _hotpot_points;
};
}
}
