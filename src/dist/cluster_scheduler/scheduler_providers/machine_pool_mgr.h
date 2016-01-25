# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/cpp/auto_codes.h>
# include <unordered_map>
# include <algorithm>
# include <string>
# include <vector>



namespace dsn
{
    namespace dist
    {

        //error code
        DEFINE_ERR_CODE(ERR_RESOURCE_NOT_ENOUGH)
        class machine_pool_mgr
        {
        public:
            machine_pool_mgr();

            /*
            * Each string in the forbidden_list is a hostname of a machine.
            * Each string in the assign_list is of the format username@hostname.
            * The error_code of ERR_OK indicates a successful get,
            * while an ERR_RESOURCE_NOT_ENOUGH indicate there are not enough machines.
            */
            error_code get_machine(
                int count,
                const std::vector<std::string>& forbidden_list,
                /*out*/ std::vector<std::string>& assign_list
                );

            /*each string in the machine_list is a hostname of a machine*/
            void return_machine(
                const std::vector<std::string>& machine_list
                );

        private:
            struct machine_workload
            {
                int instance;
            };

            struct machine_identity
            {
                std::string host_name;
                std::string user_name;
            };
            struct machine_info
            {
                machine_workload workload;
                machine_identity id;
                bool friend operator < (const machine_info &a, const machine_info &b)
                {
                    return a.workload.instance < b.workload.instance;
                }
            };

            std::unordered_map<std::string, machine_info> _machines;

            ::dsn::service::zlock _lock;


            error_code parse_cluster_config_file(
                const std::string& cluster_config_file,
                /*out*/ std::vector<machine_identity> &machine_id
                );
        };
    }
}
