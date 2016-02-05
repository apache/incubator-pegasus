#include "machine_pool_mgr.h"
#include <dsn/internal/task.h>
#include <fstream>

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "machine_pool"

namespace dsn
{
    namespace dist
    {
        machine_pool_mgr::machine_pool_mgr(const char* sec)
        {
            ::dsn::service::zauto_lock l(_lock);
            std::string cluster_config_file = dsn_config_get_value_string(sec, "machine", "nodes", "the location of the file which lists the host name and user name of all the available machines");
            std::vector<std::string> machine_id;
            error_code err = parse_cluster_config_file(cluster_config_file, machine_id);
            dassert(ERR_OK == err, "unable to load the cluster config file, pls check your config file again.");
            for (auto& i : machine_id)
            {
                machine_info machine;
                machine.workload.instance_count = 0;
                machine.identity = i;
                _machines[i] = machine;
            }
        }

        error_code machine_pool_mgr::get_machine(const alloca_options& opt, /* out */ std::vector<std::string>& assign_list)
        {
            ::dsn::service::zauto_lock l(_lock);

            if (opt.slot_count <= 0)
            {
                return ERR_INVALID_PARAMETERS;
            }

            std::set<std::string> forbidden_machines;
            for (auto& i : opt.forbidden_machines)
            {
                forbidden_machines.insert(i);
            }

            std::vector<machine_info> candidates;
            for (auto& i : _machines)
            {
                if (forbidden_machines.find(i.first) != forbidden_machines.end())
                {
                    continue;
                }
                candidates.push_back(i.second);
            }

            sort(candidates.begin(), candidates.end());
            assign_list.clear();
            do {
                for (size_t i = 0; i < candidates.size() && assign_list.size() < opt.slot_count; i++)
                {
                    assign_list.push_back(candidates[i].identity);
                }
            } while (opt.allow_same_machine_slots && assign_list.size() < opt.slot_count);

            if (!opt.allow_partial_allocation && assign_list.size() < opt.slot_count)
            {
                assign_list.clear();
                return ERR_RESOURCE_NOT_ENOUGH;
            }

            for (auto& i : assign_list)
            {
                _machines[i].workload.instance_count++;
            }
            return ERR_OK;
        }

        void machine_pool_mgr::return_machine(const std::vector<std::string>& machine_list)
        {
            ::dsn::service::zauto_lock l(_lock);
            for (auto& i: machine_list)
            {
                if (_machines.find(i) != _machines.end())
                {
                    _machines[i].workload.instance_count -= 1;
                }
            }
        }

        error_code machine_pool_mgr::parse_cluster_config_file(const std::string& cluster_config_file, std::vector<std::string> &machine_id)
        {
            FILE* fd = fopen(cluster_config_file.c_str(), "r");
            if (fd == nullptr)
            {
                return ERR_FILE_OPERATION_FAILED;
            }
            char str[256];

            str[255] = '\0';
            while (1 == fscanf(fd, "%255s", str))
            {
                    machine_id.push_back(std::string(str));
            }
            fclose(fd);
            return ERR_OK;
        }
    }
}
