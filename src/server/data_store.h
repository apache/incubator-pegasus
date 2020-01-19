#include "shell/commands.h"

namespace pegasus {
namespace server {

class data_store
{
public:
    data_store(const row_data &row, const std::string app_name_out)
        :app_name(app_name_out),total_qps(row.get_total_qps()),total_cu(row.get_total_cu()),partition_name(row.row_name)
    {
    };
    const std::string app_name;
    const double total_qps;
    const double total_cu;
    const std::string partition_name;
};
}
}
