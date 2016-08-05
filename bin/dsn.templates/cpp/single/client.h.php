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
    <?=$svc->name?>_client() { }
    explicit <?=$svc->name?>_client(::dsn::rpc_address server) { _server = server; }
    virtual ~<?=$svc->name?>_client() {}

<?php foreach ($svc->functions as $f) { ?>

    // ---------- call <?=$f->get_rpc_code()?> ------------
<?php    if ($f->is_one_way()) {?>
    void <?=$f->name?>(
        const <?=$f->get_cpp_request_type_name()?>& args,
        int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
        uint64_t partition_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        ::dsn::rpc::call_one_way_typed(server_addr.unwrap_or(_server),
            <?=$f->get_rpc_code()?>, args, thread_hash, partition_hash);
    }
<?php    } else { ?>
    // - synchronous
    std::pair< ::dsn::error_code, <?=$f->get_cpp_return_type()?>> <?=$f->name?>_sync(
        const <?=$f->get_cpp_request_type_name()?>& args,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
        uint64_t partition_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::wait_and_unwrap< <?=$f->get_cpp_return_type()?>>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                <?=$f->get_rpc_code()?>,
                args,
                nullptr,
                empty_callback,
                timeout,
                thread_hash,
                partition_hash
                )
            );
    }

    // - asynchronous with on-stack <?=$f->get_cpp_request_type_name()?> and <?=$f->get_cpp_return_type()?>
    template<typename TCallback>
    ::dsn::task_ptr <?=$f->name?>(
        const <?=$f->get_cpp_request_type_name()?>& args,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
        uint64_t request_partition_hash = 0,
        int reply_thread_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server),
                    <?=$f->get_rpc_code()?>,
                    args,
                    this,
                    std::forward<TCallback>(callback),
                    timeout,
                    request_thread_hash,
                    request_partition_hash,
                    reply_thread_hash
                    );
    }
<?php    }?>
<?php } ?>

private:
    ::dsn::rpc_address _server;
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
