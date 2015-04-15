<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include "<?=$file_prefix?>.code.definition.h"
# include "replication_app_client_base.h"

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
<?php
$keys = array();
foreach ($svc->functions as $f)
	$keys[$f->get_first_param()->get_cpp_type()] = 1;
	
	
foreach ($keys as $k => $v)
{
	echo "\tvirtual int get_partition_index(const ".$k."& key) = 0;".PHP_EOL;
}
?>
<?php foreach ($svc->functions as $f) { ?>

	// ---------- call <?=$f->get_rpc_code()?> ------------
<?php if ($f->is_one_way()) {?>
	void <?=$f->name?>(
		const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
		int hash = 0)
	{
		::dsn::message_ptr msg = ::dsn::message::create_request(<?=$f->get_rpc_code()?>, 0, hash);
		marshall(msg->writer(), <?=$f->get_first_param()->name?>);
		::dsn::service::rpc::call_one_way(_server, msg);
	}
<?php	} else { ?>
	// - synchronous 
	::dsn::error_code <?=$f->name?>(
		const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 
		__out_param <?=$f->get_cpp_return_type()?>& resp, 
		int timeout_milliseconds = 0
		)
	{
		auto resp_task = ::dsn::replication::replication_app_client_base::read<<?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            nullptr,
            nullptr,
            timeout_milliseconds
            );
		resp_task->wait();
		if (resp_task->error() == ::dsn::ERR_SUCCESS)
		{
			unmarshall(resp_task->get_response()->reader(), resp);
		}
		return resp_task->error();
	}
	
	// - asynchronous with on-stack <?=$f->get_first_param()->get_cpp_type()?> and <?=$f->get_cpp_return_type()?> 
	::dsn::rpc_response_task_ptr begin_<?=$f->name?>(
		const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0
		)
	{
		return ::dsn::replication::replication_app_client_base::read<simple_kv_client, std::string, std::string>(
            get_partition_index(<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>, 
            <?=$f->get_first_param()->name?>,
            this,
            &<?=$svc->name?>_client::end_<?=$f->name?>, 
            timeout_milliseconds,
			reply_hash
            );
	}

	virtual void end_<?=$f->name?>(
		::dsn::error_code err, 
		const <?=$f->get_cpp_return_type()?>& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
		}
	}
	
	// - asynchronous with on-heap std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>> and std::shared_ptr<<?=$f->get_cpp_return_type()?>> 
	::dsn::rpc_response_task_ptr begin_<?=$f->name?>2(
		std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>>& <?=$f->get_first_param()->name?>, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0
		)
	{
		return ::dsn::replication::replication_app_client_base::read<<?=$svc->name?>_client, <?=$f->get_first_param()->get_cpp_type()?>, <?=$f->get_cpp_return_type()?>>(
            get_partition_index(*<?=$f->get_first_param()->name?>),
            <?=$f->get_rpc_code()?>,
            <?=$f->get_first_param()->name?>,
            this,
            &<?=$svc->name?>_client::end_<?=$f->name?>2, 
            timeout_milliseconds,
			reply_hash
            );
	}

	virtual void end_<?=$f->name?>2(
		::dsn::error_code err, 
		std::shared_ptr<<?=$f->get_first_param()->get_cpp_type()?>>& <?=$f->get_first_param()->name?>, 
		std::shared_ptr<<?=$f->get_cpp_return_type()?>>& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
		}
	}
	
<?php	}?>
<?php } ?>
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
