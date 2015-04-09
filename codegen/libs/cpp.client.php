<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include "<?=$file_prefix?>.code.definition.h"
# include <dsn/internal/service.api.oo.h>

<?=$_PROG->get_cpp_namespace_begin()?>

<? foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_client 
	: public virtual ::dsn::service::servicelet
{
public:
	<?=$svc->name?>_client(const ::dsn::end_point& server) { _server = server; }
	virtual ~<?=$svc->name?>_client() {}

<? foreach ($svc->functions as $f) { ?>

	// ---------- call <?=$f->get_rpc_code()?> ------------
<?	if ($f->is_one_way()) {?>
	void <?=$f->name?>(
		<?=$f->get_first_param()->type_name?>& <?=$f->get_first_param()->name?>, 
		int hash = 0)
	{
		::dsn::message_ptr msg = ::dsn::message::create_request(<?=$f->get_rpc_code()?>, 0, hash);
		marshall(msg->writer(), <?=$f->get_first_param()->name?>);
		::dsn::service::rpc::call_one_way(_server, msg);
	}
<?	} else { ?>
	// - synchronous 
	::dsn::error_code <?=$f->name?>(
		<?=$f->get_first_param()->type_name?>& <?=$f->get_first_param()->name?>, 
		__out_param <?=$f->ret?>& resp, 
		int timeout_milliseconds = 0, 
		int hash = 0)
	{
		::dsn::message_ptr msg = ::dsn::message::create_request(<?=$f->get_rpc_code()?>, timeout_milliseconds, hash);
		marshall(msg->writer(), <?=$f->get_first_param()->name?>);
		auto resp_task = ::dsn::service::rpc::call(_server, msg, nullptr);
		resp_task->wait();
		if (resp_task->error() == ::dsn::ERR_SUCCESS)
		{
			unmarshall(resp_task->get_response()->reader(), resp);
		}
		return resp_task->error();
	}
	
	// - asynchronous with on-stack <?=$f->get_first_param()->type_name?> and <?=$f->ret?> 
	::dsn::rpc_response_task_ptr begin_<?=$f->name?>(
		const <?=$f->get_first_param()->type_name?>& <?=$f->get_first_param()->name?>, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0,
		int request_hash = 0)
	{
		return ::dsn::service::rpc::call_typed(
					_server, 
					<?=$f->get_rpc_code()?>, 
					<?=$f->get_first_param()->name?>, 
					this, 
					&<?=$svc->name?>_client::end_<?=$f->name?>, 
					request_hash, 
					timeout_milliseconds, 
					reply_hash
					);
	}

	virtual void end_<?=$f->name?>(
		::dsn::error_code err, 
		const <?=$f->ret?>& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
		}
	}
	
	// - asynchronous with on-heap std::shared_ptr<<?=$f->get_first_param()->type_name?>> and std::shared_ptr<<?=$f->ret?>> 
	::dsn::rpc_response_task_ptr begin_<?=$f->name?>2(
		std::shared_ptr<<?=$f->get_first_param()->type_name?>>& <?=$f->get_first_param()->name?>, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0,
		int request_hash = 0)
	{
		return ::dsn::service::rpc::call_typed(
					_server, 
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
		std::shared_ptr<<?=$f->get_first_param()->type_name?>>& <?=$f->get_first_param()->name?>, 
		std::shared_ptr<<?=$f->ret?>>& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply <?=$f->get_rpc_code()?> err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply <?=$f->get_rpc_code()?> ok" << std::endl;
		}
	}
	
<?	}?>
<? } ?>

private:
	::dsn::end_point _server;
};

<? } ?>
<?=$_PROG->get_cpp_namespace_end()?>
