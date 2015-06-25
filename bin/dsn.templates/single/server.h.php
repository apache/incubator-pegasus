<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include <dsn/serverlet.h>
# include "<?=$file_prefix?>.code.definition.h"
# include <iostream>

<?=$_PROG->get_cpp_namespace_begin()?>

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_service 
    : public ::dsn::service::serverlet<<?=$svc->name?>_service>
{
public:
    <?=$svc->name?>_service() : ::dsn::service::serverlet<<?=$svc->name?>_service>("<?=$svc->name?>") {}
    virtual ~<?=$svc->name?>_service() {}

protected:
    // all service handlers to be implemented further
<?php foreach ($svc->functions as $f) { ?>
    // <?=$f->get_rpc_code()?> 
<?php     if ($f->is_one_way()) {?>
    virtual void on_<?=$f->name?>(const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>)
    {
        std::cout << "... exec <?=$f->get_rpc_code()?> ... (not implemented) " << std::endl;
    }
<?php     } else {?>
    virtual void on_<?=$f->name?>(const <?=$f->get_first_param()->get_cpp_type()?>& <?=$f->get_first_param()->name?>, ::dsn::service::rpc_replier<<?=$f->get_cpp_return_type()?>>& reply)
    {
        std::cout << "... exec <?=$f->get_rpc_code()?> ... (not implemented) " << std::endl;
        <?=$f->get_cpp_return_type()?> resp;
        reply(resp);
    }
<?php     } ?>
<?php } ?>
    
public:
    void open_service()
    {
<?php foreach ($svc->functions as $f) { ?>
<?php     if ($f->is_one_way()) {?>
        this->register_rpc_handler(<?=$f->get_rpc_code()?>, "<?=$f->name?>", &<?=$svc->name?>_service::on_<?=$f->name?>);
<?php     } else {?>
        this->register_async_rpc_handler(<?=$f->get_rpc_code()?>, "<?=$f->name?>", &<?=$svc->name?>_service::on_<?=$f->name?>);
<?php     } ?>
<?php } ?>
    }

    void close_service()
    {
<?php foreach ($svc->functions as $f) { ?>
        this->unregister_rpc_handler(<?=$f->get_rpc_code()?>);
<?php } ?>
    }
};

<?php } ?>
<?=$_PROG->get_cpp_namespace_end()?>
