

# pragma once

# include <dsn/service_api_cpp.h>
# include <unordered_map>
# include <string>
# include <vector>

namespace dsn 
{
    namespace dist 
    {
        class machine_pool_mgr
        {
        public:
            machine_pool_mgr(const std::string& cluster_config_file);

            error_code get_machine(
                        int count,
                        const std::vector<std::string>& forbidden_list,
                        /*out*/ std::vector<std::string>& assign_list
                        );

            void return_machine(
                const std::vector<std::string>& machine_list
                );

        private:
            struct machine_info
            {
                double workload;
            };

            std::unordered_map<std::string, machine_info> _machines;
        };
    }
}