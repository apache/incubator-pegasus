#include "machine_pool_mgr.h"

#include <fstream>

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "machine_pool"

namespace dsn
{
    namespace dist
    {
        machine_pool_mgr::machine_pool_mgr()
        {
            ::dsn::service::zauto_lock l(_lock);
            std::string cluster_config_file = dsn_config_get_value_string("apps.server", "node_list_path", "nodes", "the location of the file which lists the host name and user name of all the available machines");
            std::vector<machine_identity> machine_id;
            error_code err = parse_cluster_config_file(cluster_config_file, machine_id);
            dassert(ERR_OK == err, "unable to load the cluster config file, pls check your config file again.");
            for (std::vector<machine_identity>::iterator i = machine_id.begin(); i != machine_id.end(); i++)
            {
                machine_info machine;
                machine.workload.instance = 0;
                machine.id = *i;
                _machines[i->host_name] = machine;
            }
        }

        error_code machine_pool_mgr::get_machine(int count, const std::vector<std::string>& forbidden_list, std::vector<std::string>& assign_list)
        {
            ::dsn::service::zauto_lock l(_lock);

            if (count <= 0)
            {
                return ERR_INVALID_PARAMETERS;
            }

            if (_machines.size() < count + forbidden_list.size())
            {
                //machines not enough
                return ERR_RESOURCE_NOT_ENOUGH;
            }

            std::set<std::string> forbidden_machines;
            for (std::vector<std::string>::const_iterator i = forbidden_list.begin(); i != forbidden_list.end(); i++)
            {
                forbidden_machines.insert(*i);
            }

            std::vector<machine_info> candidates;
            for (std::unordered_map<std::string, machine_info>::iterator i = _machines.begin(); i != _machines.end(); i++)
            {
                if (forbidden_machines.find(i->first) != forbidden_machines.end())
                {
                    continue;
                }
                candidates.push_back(i->second);
            }

            sort(candidates.begin(), candidates.end());
            assign_list.clear();
            for (int i = 0; i < count; i++)
            {
                assign_list.push_back(candidates[i].id.user_name + "@" + candidates[i].id.host_name);
                _machines[candidates[i].id.host_name].workload.instance += 1;
            }
        }

        void machine_pool_mgr::return_machine(const std::vector<std::string>& machine_list)
        {
            ::dsn::service::zauto_lock l(_lock);
            for (std::vector<std::string>::const_iterator i = machine_list.begin(); i != machine_list.end(); i++)
            {
                if (_machines.find(*i) != _machines.end())
                {
                    _machines[*i].workload.instance -= 1;
                }
            }
        }

        error_code machine_pool_mgr::parse_cluster_config_file(const std::string& cluster_config_file, std::vector<machine_identity> &machine_id)
        {
            FILE* fd = fopen(cluster_config_file.c_str(), "r");
            if (fd == nullptr)
            {
                return ERR_FILE_OPERATION_FAILED;
            }
            char str[256];
            char host_name[128];
            char user_name[128];

            str[255] = host_name[127] = user_name[127] = '\0';
            while (1 == fscanf(fd, "%255s", str))
            {
                if (2 == sscanf(str, "%127[^@]@%127s", user_name, host_name))
                {
                    machine_identity id;
                    id.host_name = host_name;
                    id.user_name = user_name;
                    machine_id.push_back(id);
                }
            }
            fclose(fd);
            return ERR_OK;
        }
    }
}
