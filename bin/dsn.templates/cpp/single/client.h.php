<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include "<?=$file_prefix?>.code.definition.h"
# include <iostream>


<?=$_PROG->get_cpp_namespace_begin()?>

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_client 
    : public virtual ::dsn::servicelet
{
public:
    <?=$svc->name?>_client(const dsn_address_t& server) { _server = server; }
    <?=$svc->name?>_client() { _server = dsn_address_invalid; }
    virtual ~<?=$svc->name?>_client() {}

<?php foreach ($svc->functions as $f) { ?>

    // ---------- call <?=$f->get_rpc_code()?> ------------
<?php    if ($f->is_one_way()) {?>
    void <?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
        int hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        ::dsn::rpc::call_one_way_typed(p_server_addr ? *p_server_addr : _server, 
            <?=$f->get_rpc_code()?>, <?=$f->get_first_param()->name?>, hash);
    }
<?php    } else { ?>
    // - synchronous 
    ::dsn::error_code <?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
        __out_param <?=$f->get_cpp_return_type()?>& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        dsn::message_ptr response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            <?=$f->get_rpc_code()?>, <?=$f->get_first_param()->name?>, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            ::unmarshall(response.get_msg(), resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack <?=$f->get_first_param()->get_cpp_type()?> and <?=$f->get_cpp_return_type()?> 
    ::dsn::task_ptr begin_<?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    <?=$f->get_rpc_code()?>, 
                    <?=$f->get_first_param()->name?>, 
                    this, 
                    &<?=$svc->name?>_client::end_<?=$f->name?>,
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_<?=$f->name?>(
        ::dsn::error_code err, 
        const <?=$f->get_cpp_return_type()?>& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>> and std::shared_ptr<<?=$f->get_cpp_return_type()?>> 
    ::dsn::task_ptr begin_<?=$f->name?>2(
        std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>>& <?=$f->get_first_param()->name?>,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    <?=$f->get_rpc_code()?>, 
                    <?=$f->get_first_param()->name?>, 
                    this, 
                    &<?=$svc->name?>_client::end_<?=$f->name?>2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_<?=$f->name?>2(
        ::dsn::error_code err, 
        std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>>& <?=$f->get_first_param()->name?>, 
        std::shared_ptr<<?=$f->get_cpp_return_type()?>>& resp)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
        }
    }
    
<?php    }?>
<?php } ?>

private:
    dsn_address_t _server;
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
