#include "shell/commands.h"

namespace pegasus {
namespace server {

class data_store
{
public:
    data_store(){};
    std::string store_name;
    double total_get_qps = 0;
    double total_multi_get_qps = 0;
    double total_put_qps = 0;
    double total_multi_put_qps = 0;
    double total_remove_qps = 0;
    double total_multi_remove_qps = 0;
    double total_incr_qps = 0;
    double total_check_and_set_qps = 0;
    double total_check_and_mutate_qps = 0;
    double total_scan_qps = 0;
    double total_recent_read_cu = 0;
    double total_recent_write_cu = 0;
    std::string name;

    void aggregate(const row_data &row)
    {
        total_get_qps = row.get_qps;
        total_multi_get_qps = row.multi_get_qps;
        total_put_qps = row.put_qps;
        total_multi_put_qps = row.multi_put_qps;
        total_remove_qps = row.remove_qps;
        total_multi_remove_qps = row.multi_remove_qps;
        total_incr_qps = row.incr_qps;
        total_check_and_set_qps = row.check_and_set_qps;
        total_check_and_mutate_qps = row.check_and_mutate_qps;
        total_scan_qps = row.scan_qps;
        total_recent_read_cu = row.recent_read_cu;
        total_recent_write_cu = row.recent_write_cu;
        name = row.row_name;
    }

    void merge(const data_store &sub_data_store)
    {
        total_get_qps += sub_data_store.total_get_qps;
        total_multi_get_qps += sub_data_store.total_multi_get_qps;
        total_put_qps += sub_data_store.total_put_qps;
        total_multi_put_qps += sub_data_store.total_multi_put_qps;
        total_remove_qps += sub_data_store.total_remove_qps;
        total_multi_remove_qps += sub_data_store.total_multi_remove_qps;
        total_incr_qps += sub_data_store.total_incr_qps;
        total_check_and_set_qps += sub_data_store.total_check_and_set_qps;
        total_check_and_mutate_qps += sub_data_store.total_check_and_mutate_qps;
        total_scan_qps += sub_data_store.total_scan_qps;
        total_recent_read_cu += sub_data_store.total_recent_read_cu;
        total_recent_write_cu += sub_data_store.total_recent_write_cu;
    }
};
}
}
