<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include <dsn/dist/replication.h>
# include "<?=$file_prefix?>.code.definition.h"
# include <iostream>

<?=$_PROG->get_cpp_namespace_begin()?>

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_client 
    : public ::dsn::replication::replication_app_client_base
{
public:
    using replication_app_client_base::replication_app_client_base;
    virtual ~<?=$svc->name?>_client() {}

    // from requests to partition index
    // PLEASE DO RE-DEFINE THEM IN A SUB CLASS!!!
<?php
$keys = array();
foreach ($svc->functions as $f)
    $keys[$f->get_first_param()->get_cpp_type()] = 1;
    
    
foreach ($keys as $k => $v)
{
    echo "    virtual uint64_t get_key_hash(const ".$k."& key) { return 0; }".PHP_EOL;
}
?>
<?php foreach ($svc->functions as $f) { ?>

    // ---------- call <?=$f->get_rpc_code()?> ------------
    // - synchronous  
    std::pair< ::dsn::error_code, <?=$f->get_cpp_return_type()?>> <?=$f->name?>_sync(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0)<?=$f->is_write ? PHP_EOL:",".PHP_EOL."        ::dsn::replication::read_semantic semantic = ::dsn::replication::read_semantic::ReadLastUpdate".PHP_EOL?>
        )
    {
        return dsn::rpc::wait_and_unwrap<<?=$f->get_cpp_return_type()?>>(
            ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?>(
                get_key_hash(<?=$f->get_first_param()->name?>),
                <?=$f->get_rpc_code()?>,
                <?=$f->get_first_param()->name?>,
                this,
                empty_callback,
                timeout,
                0<?=$f->is_write ? PHP_EOL:",".PHP_EOL."                semantic".PHP_EOL?>
                )
            );
    }
 
    // - asynchronous with on-stack <?=$f->get_first_param()->get_cpp_type()?> and <?=$f->get_cpp_return_type()?> 
    template<typename TCallback>
    ::dsn::task_ptr <?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0<?=$f->is_write ? PHP_EOL:",".PHP_EOL."        ::dsn::replication::read_semantic semantic = ::dsn::replication::read_semantic::ReadLastUpdate".PHP_EOL?>
        )
    {
        return ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?>(
            get_key_hash(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            this,
            std::forward<TCallback>(callback),
            timeout,
            reply_hash<?=$f->is_write ? PHP_EOL:",".PHP_EOL."            semantic".PHP_EOL?>
            );
    }
<?php    }?>
};
<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
