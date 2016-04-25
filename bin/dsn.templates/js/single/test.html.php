<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
$idl_format = $argv[5];
$dsn_root = getenv("DSN_ROOT");
?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>rDSN</title>

<!-- Bootstrap -->
<link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
<script src="https://code.jquery.com/jquery-2.2.2.min.js"   integrity="sha256-36cp2Co+/62rEAAYHLmRCPIych47CvdM+uTBJwSzWjI="   crossorigin="anonymous"></script>
<script src="<?=$dsn_root?>/include/dsn/js/thrift.js"></script>
<script src="<?=$dsn_root?>/include/dsn/js/dsn_transport.js"></script>
<script src="<?=$file_prefix?>.client.js"></script>
<script src="thrift/<?=$_PROG->name?>_types.js"></script>
</head>
<body>
<div class="container" style="margin-top:20px;">
    <div class="jumbotron">
        <h1 class="display-3">Test Your Code</h1>
        <p class="lead">rDSN</p>
        <p>An open framework for quickly building and managing high performance and robust distributed systems.</p>
        <hr class="m-y-2">
        <p class="lead">
        <div class="row">
            <div class="col-xs-2">
                <a class="btn btn-primary btn-lg" href="#" role="button" onclick="Test()">Start Test</a>
            </div>
            <div class="col-xs-2"></div>
                <a class="btn btn-primary btn-lg" href="#" role="button" onclick="outputClear()">Clear</a>
            </div>
        </p>
            <div class="row">
        <div class="col-xs-12">
            <textarea id="output" rows="20" cols="120" readonly = true></textarea>
        </div>
    </div>
    </div>
</div>
    
<script>

var website = "http://localhost:27001";

function outputAppend(val)
{
    var old = $('#output').val();
    $('#output').val(old + val);
}

function outputClear()
{
    $('#output').val("");
}

<?php
foreach ($_PROG->services as $svc) 
{   
    foreach ($svc->functions as $func)
    {
?>
function test_<?=$svc->name?>_<?=$func->name?>(args, async)
{
    outputAppend("test <?=$svc->name?> <?=$func->name?>: ");
    var client = new <?=$svc->name?>App(website);
    var result = null;
    if (async)
    {
        result = client.<?=$func->name?>({
            args: args,
            async: true,
            on_success: function (ret){result = ret;},
            on_fail: function (xhr, textStatus, errorThrown) {}
        });
    } else
    {
        result = client.<?=$func->name?>({
            args: args,
            async: false
        });
    }
    outputAppend(result);
    if (result == null)
    {
        outputAppend("\tfailed\n");
    }
    else
    {
        outputAppend("\tsuccess\n")
    }
}

<?php
    }
}
?>
function testCounterAdd(args, async)
{
    outputAppend("test counter add: ");
    var counter = new counterApp(website);
    var result = counter.add(args);
    outputAppend(result);
    if (result == null)
    {
        outputAppend("\tfailed\n");
    }
    else
    {
        outputAppend("\tsuccess\n")
    }
}

function Test()
{
<?php
foreach ($_PROG->services as $svc) 
{   
    foreach ($svc->functions as $func)
    {
?>
    {
        args = new <?=$func->get_cpp_request_type_name()?>();
        test_<?=$svc->name?>_<?=$func->name?>(args, false);
    }

<?php
    }
}
?>
}


</script>
</body>
</html>