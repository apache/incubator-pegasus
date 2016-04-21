<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
$idl_format = $argv[5];
$_IDL_FORMAT = $argv[4];
?>
<?php
function generate_request_helper($client, $func, $async)
{
?>
<?=$client?>.prototype.internal_<?php if ($async) { echo "async_";} ?><?=$func->name?> = function(args, <?php if ($async) {echo "on_success, on_fail,";} ?> hash) {
    var self = this;
    var ret = null;
    dsn_call(
        this.get_<?=$func->name?>_address(hash),
        "POST",
        this.marshall(args),
        <?php if($async) {echo "true";} else {echo "false";}?>,
        function(result) {
            ret = new <?=$func->get_cpp_return_type()?>();
            self.unmarshall(result, ret);
            ret = ret.success;
<?php if ($async) { ?>
            on_success(ret);
<?php } ?>
        },
        function(xhr, textStatus, errorThrown) {
            ret = null;
<?php if ($async) { ?>
            if (on_fail) {
                on_fail(xhr, textStatus, errorThrown);
            }
<?php } ?>
        }
    );
    return ret;
}

<?php
}
foreach ($_PROG->services as $svc) 
{   
    $client = $svc->name."App";
?>
<?=$client?> = function(website) {
    this.url = website;
}

<?=$client?>.prototype = {};

<?=$client?>.prototype.marshall = function(value) {
    return marshall_thrift_json(value);
}

<?=$client?>.prototype.unmarshall = function(buf, ret) {
    unmarshall_thrift_json(buf, ret);
}

<?=$client?>.prototype.get_address = function(url, hash) {
    if (typeof hash == "undefined") {
        hash = 0;
    }
    return url + "/" + hash;
}

<?php
    foreach ($svc->functions as $func)
    {
        generate_request_helper($client, $func, false);
        generate_request_helper($client, $func, true);
?>
<?=$client?>.prototype.<?=$func->name?> = function(obj) {
    if (!obj.async) {
        return this.internal_<?=$func->name?>(obj.args, obj.hash);
    } else {
        this.internal_async_<?=$func->name?>(obj.args, obj.on_success, obj.on_fail, obj.hash);
    }
}

<?=$client?>.prototype.get_<?=$func->name?>_address = function(hash) {
    return this.get_address(this.url + "/" + "<?php echo $func->get_rpc_code(); ?>", hash);
}

<?php
    }
?>
<?php 
}
?>
