<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once

# include "<?=$file_prefix?>.client.h"

<?=$_PROG->get_cpp_namespace_begin()?>

<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_perf_test_client
    : public <?=$svc->name?>_client,
      public ::dsn::service::perf_client_helper
{
public:
    <?=$svc->name?>_perf_test_client(
        ::dsn::rpc_address server)
        : <?=$svc->name?>_client(server)
    {
    }

    void start_test()
    {
        perf_test_suite s;
        std::vector<perf_test_suite> suits;

<?php foreach ($svc->functions as $f) { ?>
        s.name = "<?=$svc->name?>.<?=$f->name?>";
        s.config_section = "task.<?=$f->get_rpc_code()?>";
        s.send_one = [this](int payload_bytes, int key_space_size){this->send_one_<?=$f->name?>(payload_bytes, key_space_size); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

<?php } ?>
        start(suits);
    }
<?php foreach ($svc->functions as $f) { ?>

    void send_one_<?=$f->name?>(int payload_bytes, int key_space_size)
    {
        <?=$f->get_first_param()->get_cpp_type()?> req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        <?=$f->name?>(
            req,
            [this, context = prepare_send_one()](error_code err, <?=$f->get_cpp_return_type()?>&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }
<?php } ?>
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
