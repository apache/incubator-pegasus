<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$_IDL_FORMAT = $argv[4];
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

        const char* sections[10240];
        int scount, used_count = sizeof(sections) / sizeof(const char*);
        scount = dsn_config_get_all_sections(sections, &used_count);
        dassert(scount == used_count, "too many sections (>10240) defined in config files");

        for (int i = 0; i < used_count; i++)
        {
            if (strstr(sections[i], "simple_kv.perf_test.case.") == sections[i])
            {
                s.name = std::string(sections[i]);
                s.config_section = std::string(sections[i]);
                s.send_one = [this](int payload_bytes, int key_space_size, const std::vector<double>& ratios){this->send_one(payload_bytes, key_space_size, ratios); };
                s.cases.clear();
                load_suite_config(s, <?=count($svc->functions)?>);
                suits.push_back(s);
            }
        }
        
        start(suits);
    }
    
    void send_one(int payload_bytes, int key_space_size, const std::vector<double>& ratios)
    {
        auto prob = (double)dsn_random32(0, 1000) / 1000.0;
        if (0) {}
<?php $i = 0; foreach ($svc->functions as $f) {?>
        else if (prob <= ratios[<?=$i?>])
        {
            send_one_<?=$f->name?>(payload_bytes, key_space_size);
        }
<?php $i++; }?>
        else { /* nothing to do */ }
    }
    
<?php foreach ($svc->functions as $f) { ?>

    void send_one_<?=$f->name?>(int payload_bytes, int key_space_size)
    {
        <?=$f->get_cpp_request_type_name()?> req;
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
