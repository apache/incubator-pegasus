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

// server app example
class <?=$_PROG->name?>_server_app : 
    public ::dsn::service_app
{
public:
    <?=$_PROG->name?>_server_app() {}

    virtual ::dsn::error_code start(int argc, char** argv)
    {
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_svc.open_service();
<?php } ?>
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_svc.close_service();
<?php } ?>
    }

private:
<?php foreach ($_PROG->services as $svc) { ?>
    <?=$svc->name?>_service _<?=$svc->name?>_svc;
<?php } ?>
};

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
        if (argc < 3)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_client = std::make_unique<<?=$svc->name?>_client>(_server);
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
<?php
foreach ($_PROG->services as $svc)
{
    echo "        // test for service '". $svc->name ."'". PHP_EOL;
    foreach ($svc->functions as $f)
{?>
        {
<?php if ($f->is_one_way()) { ?>
            _<?=$svc->name?>_client-><?=$f->name?>(req);
<?php } else { ?>
            //sync:
            auto result = _<?=$svc->name?>_client-><?=$f->name?>({}});
            std::cout << "call <?=$f->get_rpc_code()?> end, return " << result.first.to_string() << std::endl;
            //async: 
            //_<?=$svc->name?>_client->begin_<?=$f->name?>({});
<?php } ?>           
        }
<?php }    
}
?>
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
    <?=$svc->name?>_perf_test_client_app()
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

        _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));

        _<?=$svc->name?>_client = new <?=$svc->name?>_perf_test_client(_server);
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
    ::dsn::rpc_address _server;
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
