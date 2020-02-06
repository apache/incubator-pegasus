#include "shell/commands.h"

namespace pegasus {
namespace server {

class data_store
{
public:
    data_store(const row_data &row, const std::string app_name_out)
        : _app_name(app_name_out),
          _total_qps(row.get_total_qps()),
          _total_cu(row.get_total_cu()),
          _partition_name(row.row_name){};
    data_store() {}
    std::string _app_name;
    double _total_qps;
    double _total_cu;
    std::string _partition_name;
};
}
}
