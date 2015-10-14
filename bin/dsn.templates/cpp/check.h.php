<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>

# include <dsn/tool/global_checker.h>

<?=$_PROG->get_cpp_namespace_begin()?> 

class <?=$_PROG->name?>_checker 
    : public ::dsn::tools::checker
{
public:
    <?=$_PROG->name?>_checker(const char* name, dsn_app_info* info, int count)
          : ::dsn::tools::checker(name, info, count)
    {
        for (auto& app : _apps)
        {
            // TODO: identify your own type of service apps
            //if (0 == strcmp(app.second.type, "meta"))
            //{
            //    _meta_servers.push_back((meta_service_app*)app.second.app_context_ptr);
            //}
        }
    }

    virtual void check() override
    {
        // nothing to check
        //if (_meta_servers.size() == 0)
        //    return;

        // check all invariances
        /*
        auto meta = meta_leader();
        if (!meta) return;

        for (auto& r : _replica_servers)
        {
            if (!r->is_started())
                continue;

            auto ep = r->primary_address();
            if (!meta->_service->_failure_detector->is_worker_connected(ep))
            {
                dassert(!r->_stub->is_connected(), "when meta server says a replica is dead, it must be dead");
            }
        }
        */
    }

private:
    //std::vector<meta_service_app*>        _meta_servers;
};

<?=$_PROG->get_cpp_namespace_end()?>
