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
class <?=$_PROG->name?>_client_app : 
    public ::dsn::service_app,
    public virtual ::dsn::clientlet
{
public:
    <?=$_PROG->name?>_client_app() {}
    
    ~<?=$_PROG->name?>_client_app() 
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector< ::dsn::rpc_address> meta_servers;
        ::dsn::replication::replication_app_client_base::load_meta_servers(meta_servers);
        
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_client.reset(new <?=$svc->name?>_client(meta_servers, argv[1]));
<?php } ?>
        _timer = ::dsn::tasking::enqueue_timer(<?=$_PROG->get_test_task_code()?>, this, [this]{on_test_timer();}, std::chrono::seconds(1));
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        _timer->cancel(true);
<?php foreach ($_PROG->services as $svc) { ?> 
        _<?=$svc->name?>_client.reset();
<?php } ?>
    }

    void on_test_timer()
    {
<?php foreach ($_PROG->services as $svc) { ?>
        // test for service <?=$svc->name.PHP_EOL?>
<?php foreach ($svc->functions as $f) { ?>
        {
            <?=$f->get_first_param()->get_cpp_type()?> req;
<?php if ($f->is_one_way()) { ?>
            _<?=$svc->name?>_client-><?=$f->name?>(req);
<?php } else { ?>
            //sync:
            error_code err;
            <?=$f->get_cpp_return_type()?> resp;
            std::tie(err, resp) = _<?=$svc->name?>_client-><?=$f->name?>_sync(req);
            std::cout << "call <?=$f->get_rpc_code()?> end, return " << err.to_string() << std::endl;
            //async: 
            //_<?=$svc->name?>_client-><?=$f->name?>(req, empty_callback);
<?php } ?>           
        }
<?php } ?>
<?php } ?>
    }

private:
    ::dsn::task_ptr _timer;
    ::dsn::rpc_address _server;
    
<?php foreach ($_PROG->services as $svc) { ?>
    std::unique_ptr<<?=$svc->name?>_client> _<?=$svc->name?>_client;
<?php } ?>
};

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_perf_test_client_app : 
    public ::dsn::service_app,
    public virtual ::dsn::clientlet
{
public:
    <?=$svc->name?>_perf_test_client_app() {}

    ~<?=$svc->name?>_perf_test_client_app()
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector< ::dsn::rpc_address> meta_servers;
        ::dsn::replication::replication_app_client_base::load_meta_servers(meta_servers);

        _<?=$svc->name?>_client.reset(new <?=$svc->name?>_perf_test_client(meta_servers, argv[1]));
        _<?=$svc->name?>_client->start_test();
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        _<?=$svc->name?>_client.reset();
    }
    
private:
    std::unique_ptr<<?=$svc->name?>_perf_test_client> _<?=$svc->name?>_client;
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
