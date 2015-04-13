<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include "replication_app_base.h"
# include "<?=$file_prefix?>.code.definition.h"
# include <iostream>

<?=$_PROG->get_cpp_namespace_begin()?>

<? foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_service 
	: public ::dsn::replication::replication_app_base
{
public:
	<?=$svc->name?>_service(::dsn::replication::replica* replica, const ::dsn::replication::replication_app_config* config) 
		: ::dsn::replication::replication_app_base(replica, config)
	{
		open_service();
	}
	
	virtual ~<?=$svc->name?>_service() 
	{
		close_service();
	}

protected:
	// all service handlers to be implemented further
<? foreach ($svc->functions as $f) { ?>
	// <?=$f->get_rpc_code()?> 
<? 	if ($f->is_one_way()) {?>
	virtual void on_<?=$f->name?>(const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>)
	{
		std::cout << "... exec <?=$f->get_rpc_code()?> ... (not implemented) " << std::endl;
	}
<? 	} else {?>
	virtual void on_<?=$f->name?>(const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, ::dsn::service::rpc_replier<<?=$f->get_cpp_return_type()?>>& reply)
	{
		std::cout << "... exec <?=$f->get_rpc_code()?> ... (not implemented) " << std::endl;
		<?=$f->get_cpp_return_type()?> resp;
		reply(resp);
	}
<? 	} ?>
<? } ?>
	
public:
	void open_service()
	{
<? foreach ($svc->functions as $f) { ?>
<? 	if ($f->is_one_way()) {?>
		this->register_rpc_handler(<?=$f->get_rpc_code()?>, "<?=$f->name?>", &<?=$svc->name?>_service::on_<?=$f->name?>);
<? 	} else {?>
		this->register_async_rpc_handler(<?=$f->get_rpc_code()?>, "<?=$f->name?>", &<?=$svc->name?>_service::on_<?=$f->name?>);
<? 	} ?>
<? } ?>
	}

	void close_service()
	{
<? foreach ($svc->functions as $f) { ?>
		this->unregister_rpc_handler(<?=$f->get_rpc_code()?>);
<? } ?>
	}
};

<? } ?>
<?=$_PROG->get_cpp_namespace_end()?>
