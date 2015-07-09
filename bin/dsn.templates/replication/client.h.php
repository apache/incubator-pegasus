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
    <?=$svc->name?>_client(
        const std::vector<end_point>& meta_servers,
        const char* app_name)
        : ::dsn::replication::replication_app_client_base(meta_servers, app_name) 
    {
    }
    
    virtual ~<?=$svc->name?>_client() {}
    
    // from requests to partition index
    // PLEASE DO RE-DEFINE THEM IN A SUB CLASS!!!
<?php
$keys = array();
foreach ($svc->functions as $f)
    $keys[$f->get_first_param()->get_cpp_type()] = 1;
    
    
foreach ($keys as $k => $v)
{
    echo "\tvirtual int get_partition_index(const ".$k."& key) { return 0;};".PHP_EOL;
}
?>
<?php foreach ($svc->functions as $f) { ?>

    // ---------- call <?=$f->get_rpc_code()?> ------------
<?php if ($f->is_one_way()) {?>
    void <?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?><?=$f->is_write ? "":", ". PHP_EOL."\t\t::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate"?> 
        )
    {
        ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?><<?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            nullptr,
            nullptr,
            nullptr,
            0,
            0<?=$f->is_write ? "":", ". PHP_EOL."\t\t\tread_semantic"?> 
            );
    }
<?php    } else { ?>
    // - synchronous 
    ::dsn::error_code <?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
        __out_param <?=$f->get_cpp_return_type()?>& resp, 
        int timeout_milliseconds = 0<?=$f->is_write ? "":", ". PHP_EOL."\t\t::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate"?> 
        )
    {
        auto resp_task = ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?><<?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            nullptr,
            nullptr,
            nullptr,
            timeout_milliseconds,
            0<?=$f->is_write ? "":", ". PHP_EOL."\t\t\tread_semantic"?> 
            );
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_OK)
        {
            unmarshall(resp_task->get_response()->reader(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack <?=$f->get_first_param()->get_cpp_type()?> and <?=$f->get_cpp_return_type()?> 
    ::dsn::rpc_response_task_ptr begin_<?=$f->name?>(
        const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>,
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0<?=$f->is_write ? "":",  ". PHP_EOL."\t\t::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate"?> 
        )
    {
        return ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?><<?=$svc->name?>_client, <?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>, 
            <?=$f->get_first_param()->name?>,
            this,
            &<?=$svc->name?>_client::end_<?=$f->name?>, 
            context,
            timeout_milliseconds,
            reply_hash<?=$f->is_write ? "":", ". PHP_EOL."\t\t\tread_semantic"?> 
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
    ::dsn::rpc_response_task_ptr begin_<?=$f->name?>2(
        std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>>& <?=$f->get_first_param()->name?>,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0<?=$f->is_write ? "":", ". PHP_EOL."\t\t::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate"?> 
        )
    {
        return ::dsn::replication::replication_app_client_base::<?=$f->is_write ? "write":"read"?><<?=$svc->name?>_client, <?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(*<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            this,
            &<?=$svc->name?>_client::end_<?=$f->name?>2, 
            timeout_milliseconds,
            reply_hash<?=$f->is_write ? "":", ". PHP_EOL."\t\t\tread_semantic"?> 
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
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
