<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include "<?=$file_prefix?>.client.h"
# include "<?=$file_prefix?>.client.perf.h"
# include "<?=$file_prefix?>.server.h"

<?=$_PROG->get_cpp_namespace_begin()?>

// client app example
class <?=$_PROG->name?>_client_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
{
public:
    <?=$_PROG->name?>_client_app(::dsn::service_app_spec* s) 
        : ::dsn::service::service_app(s) 
    {
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_client = nullptr;
<?php } ?>
    }
    
    ~<?=$_PROG->name?>_client_app() 
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector<::dsn::end_point> meta_servers;
        auto cf = ::dsn::service::system::config();
        ::dsn::replication::replication_app_client_base::load_meta_servers(cf, meta_servers);
        
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_client = new <?=$svc->name?>_client(meta_servers, argv[1]);
<?php } ?>
        _timer = ::dsn::service::tasking::enqueue(<?=$_PROG->get_test_task_code()?>, this, &<?=$_PROG->name?>_client_app::on_test_timer, 0, 0, 1000);
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        _timer->cancel(true);
<?php foreach ($_PROG->services as $svc) { ?> 
        if (_<?=$svc->name?>_client != nullptr)
        {
            delete _<?=$svc->name?>_client;
            _<?=$svc->name?>_client = nullptr;
        }
<?php } ?>
    }

    void on_test_timer()
    {
<?php
foreach ($_PROG->services as $svc)
{
    echo "        // test for service '". $svc->name ."'". PHP_EOL;
    foreach ($svc->functions as $f)
{?>
        {
            <?=$f->get_first_param()->get_cpp_type()?> req;
<?php if ($f->is_one_way()) { ?>
            _<?=$svc->name?>_client-><?=$f->name?>(req);
<?php } else { ?>
            //sync:
            <?=$f->get_cpp_return_type()?> resp;
            auto err = _<?=$svc->name?>_client-><?=$f->name?>(req, resp);
            std::cout << "call <?=$f->get_rpc_code()?> end, return " << err.to_string() << std::endl;
            //async: 
            //_<?=$svc->name?>_client->begin_<?=$f->name?>(req);
<?php } ?>           
        }
<?php }    
}
?>
    }

private:
    ::dsn::task_ptr _timer;
    ::dsn::end_point _server;
    
<?php foreach ($_PROG->services as $svc) { ?>
    <?=$svc->name?>_client *_<?=$svc->name?>_client;
<?php } ?>
};

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_perf_test_client_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
{
public:
    <?=$svc->name?>_perf_test_client_app(::dsn::service_app_spec* s)
        : ::dsn::service::service_app(s)
    {
        _<?=$svc->name?>_client = nullptr;
    }

    ~<?=$svc->name?>_perf_test_client_app()
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector<::dsn::end_point> meta_servers;
        auto cf = ::dsn::service::system::config();
        ::dsn::replication::replication_app_client_base::load_meta_servers(cf, meta_servers);

        _<?=$svc->name?>_client = new <?=$svc->name?>_perf_test_client(meta_servers, argv[1]);
        _<?=$svc->name?>_client->start_test();
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        if (_<?=$svc->name?>_client != nullptr)
        {
            delete _<?=$svc->name?>_client;
            _<?=$svc->name?>_client = nullptr;
        }
    }
    
private:
    <?=$svc->name?>_perf_test_client *_<?=$svc->name?>_client;
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
