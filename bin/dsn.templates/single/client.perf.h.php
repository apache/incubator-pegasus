<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once

# include "<?=$file_prefix?>.client.h"
# include <dsn/internal/perf_test_helper.h>

<?=$_PROG->get_cpp_namespace_begin()?>
<?php foreach ($_PROG->services as $svc) { ?>
class <?=$svc->name?>_perf_test_client
    : public <?=$svc->name?>_client, public ::dsn::service::perf_client_helper<<?=$svc->name?>_perf_test_client>
{
public:
    <?=$svc->name?>_perf_test_client(
        const ::dsn::end_point& server)
        : <?=$svc->name?>_client(server)
    {
    }

    void start_test()
    {
        perf_test_suite s;
        std::vector<perf_test_suite> suits;

<?php foreach ($svc->functions as $f) { ?>
        s.name = "<?=$svc->name?>.<?=$f->name?>";
        s.start = [this](){this->send_one_<?=$f->name?>(); };
        load_suite_config(s);
        suits.push_back(s);
        
<?php } ?>
        start(suits);
    }                
<?php foreach ($svc->functions as $f) { ?>

    void send_one_<?=$f->name?>()
    {
        void* ctx = prepare_send_one();
        if (!ctx)
            return;

        <?=$f->get_first_param()->get_cpp_type()?> req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        
        begin_<?=$f->name?>(req, ctx, _timeout_ms);
    }

    virtual void end_<?=$f->name?>(
        ::dsn::error_code err,
        const <?=$f->get_cpp_return_type()?>& resp,
        void* context) override
    {
        end_send_one(context, err, [this](){ send_one_<?=$f->name?>();});
    }
<?php } ?>
};
<?php } ?>

<?=$_PROG->get_cpp_namespace_end()?>
