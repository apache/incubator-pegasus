<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$_IDL_FORMAT = $argv[4];
?>
# pragma once
# include "<?=$file_prefix?>.code.definition.h"
# include <iostream>


<?=$_PROG->get_cpp_namespace_begin()?>

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_client 
    : public virtual ::dsn::clientlet
{
public:
    <?=$svc->name?>_client(::dsn::rpc_address server) { _server = server; }
    <?=$svc->name?>_client() { }
    virtual ~<?=$svc->name?>_client() {}
    
<?php foreach ($svc->functions as $f) { ?>
 
    // ---------- call <?=$f->get_rpc_code()?> ------------
<?php    if ($f->is_one_way()) {?>
    void <?=$f->name?>(
        const <?=$f->get_cpp_request_type_name()?>& args,
        uint64_t hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        ::dsn::rpc::call_one_way_typed(server_addr.unwrap_or(_server), 
            <?=$f->get_rpc_code()?>, args, hash);
    }
<?php    } else { ?>
    // - synchronous 
    std::pair< ::dsn::error_code, <?=$f->get_cpp_return_type()?>> <?=$f->name?>_sync(
        const <?=$f->get_cpp_request_type_name()?>& args,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        uint64_t hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::wait_and_unwrap<<?=$f->get_cpp_return_type()?>>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                <?=$f->get_rpc_code()?>,
                args,
                nullptr,
                empty_callback,
                hash,
                timeout,
                0
                )
            );
    }
    
    // - asynchronous with on-stack <?=$f->get_cpp_request_type_name()?> and <?=$f->get_cpp_return_type()?>  
    template<typename TCallback>
    ::dsn::task_ptr <?=$f->name?>(
        const <?=$f->get_cpp_request_type_name()?>& args,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        uint64_t hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    <?=$f->get_rpc_code()?>, 
                    args,
                    this,
                    std::forward<TCallback>(callback),
                    hash, 
                    timeout, 
                    reply_hash
                    );
    }
<?php    }?>
<?php } ?>

private:
    ::dsn::rpc_address _server;
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
