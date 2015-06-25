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
class <?=$_PROG->name?>_server_app : public ::dsn::service::service_app
{
public:
    <?=$_PROG->name?>_server_app(::dsn::service_app_spec* s) 
        : ::dsn::service::service_app(s) {}

    virtual ::dsn::error_code start(int argc, char** argv)
    {
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_svc.open_service();
<?php } ?>
        return ::dsn::ERR_SUCCESS;
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
        if (argc < 3)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server = ::dsn::end_point(argv[1], (uint16_t)atoi(argv[2]));
<?php foreach ($_PROG->services as $svc) { ?>
        _<?=$svc->name?>_client = new <?=$svc->name?>_client(_server);
<?php } ?>
        _timer = ::dsn::service::tasking::enqueue(<?=$_PROG->get_test_task_code()?>, this, &<?=$_PROG->name?>_client_app::on_test_timer, 0, 0, 1000);
        return ::dsn::ERR_SUCCESS;
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
    echo "\t\t// test for service '". $svc->name ."'". PHP_EOL;
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

        _server = ::dsn::end_point(argv[1], (uint16_t)atoi(argv[2]));

        _<?=$svc->name?>_client = new <?=$svc->name?>_perf_test_client(_server);
        _<?=$svc->name?>_client->start_test();
        return ::dsn::ERR_SUCCESS;
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
    ::dsn::end_point _server;
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
