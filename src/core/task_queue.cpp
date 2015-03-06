# include <rdsn/internal/task_queue.h>
# include "task_engine.h"
# include <rdsn/internal/perf_counters.h>
# include <cstdio>
# define __TITLE__ "task_queue"

namespace rdsn {

task_queue::task_queue(task_worker_pool* pool, int index, task_queue* inner_provider) : _pool(pool), _controller(nullptr)
{
    char num[30];
    sprintf(num, "%u", index);
    _name = pool->spec().name + '.';
    _name.append(num);
    _qps_counter = rdsn::utils::perf_counters::instance().get_counter((_name + std::string(".qps")).c_str(), COUNTER_TYPE_RATE, true);
}

//void task_queue::on_dequeue(int count)
//{
//    _qps_counter->add((unsigned long long)count);
//    _pool->on_dequeue(count);
//}

}