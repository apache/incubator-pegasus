<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$_IDL_FORMAT = $argv[4];
?>
using System;
using System.Linq;
using System.IO;
using dsn.dev.csharp;
using rDSN.Tron.App;

namespace <?=$_PROG->get_csharp_namespace()?>
{
    // server app example
    public class <?=$_PROG->name?>ServerApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
<?php foreach ($_PROG->services as $svc) { ?>
            _<?=$svc->name?>Server.OpenService(0);
<?php } ?>
            return ErrorCode.ERR_OK;
        }

        public override ErrorCode Stop(bool cleanup = false)
        {
<?php foreach ($_PROG->services as $svc) { ?>
            _<?=$svc->name?>Server.CloseService(0);
            _<?=$svc->name?>Server.Dispose();
<?php } ?>

            return ErrorCode.ERR_OK;
        }

<?php foreach ($_PROG->services as $svc) { ?>
        private <?=$svc->name?>Server _<?=$svc->name?>Server = new <?=$svc->name?>Server_impl();
<?php } ?>
    }

    public class <?=$_PROG->name?>ClientApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            if (args.Length < 2)
            {
                throw new Exception("wrong usage: server-url or server-host server-port");
            }

            if (args.Length == 2)
            {
                _server = new RpcAddress(args[1]);
            }
            else
            {
                _server = new RpcAddress(args[1], ushort.Parse(args[2]));
            }

<?php foreach ($_PROG->services as $svc) { ?>
            _<?=$svc->name?>Client = new <?=$svc->name?>Client(_server);
<?php } ?>
            _timer = Clientlet.CallAsync2(<?=$_PROG->name?>Helper.<?=$_PROG->get_test_task_code()?>, null, this.OnTestTimer, 0, 0, 1000);
            return ErrorCode.ERR_OK;
        }

        public override ErrorCode Stop(bool cleanup = false)
        {
            _timer.Cancel(true);
<?php foreach ($_PROG->services as $svc) { ?>
            _<?=$svc->name?>Client.Dispose();
            _<?=$svc->name?>Client = null;
<?php } ?>

            return ErrorCode.ERR_OK;
        }

        private void OnTestTimer()
        {
<?php
    foreach ($_PROG->services as $svc)
    {
        echo "            // test for service '". $svc->name ."'". PHP_EOL;
        foreach ($svc->functions as $f)
    {?>
            {
                <?=$f->get_csharp_request_type_name()?> req = new <?=$f->get_csharp_request_type_name()?>();
<?php if ($f->is_one_way()) { ?>
                _<?=$svc->name?>Client.<?=$f->name?>(req);
<?php } else { ?>
                //sync:
                <?=$f->get_csharp_return_type()?> resp;
                var err = _<?=$svc->name?>Client.<?=$f->name?>(req, out resp);

                Console.WriteLine("call <?=$f->get_rpc_code()?> end, return " + err.ToString());
                //async:
                // TODO:
<?php } ?>
            }
<?php }
    }
?>
        }

        private SafeTaskHandle _timer;
        private RpcAddress  _server = new RpcAddress();

<?php foreach ($_PROG->services as $svc) { ?>
        private <?=$svc->name?>Client _<?=$svc->name?>Client;
<?php } ?>
    }
}
