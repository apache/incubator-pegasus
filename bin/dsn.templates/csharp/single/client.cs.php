<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
using System;
using System.IO;
using dsn.dev.csharp;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
<?php foreach ($_PROG->services as $svc) { ?>
    public class <?=$svc->name?>Client : Servicelet
    {
        private RpcAddress _server;
        
        public <?=$svc->name?>Client(RpcAddress server) { _server = server; }
        public <?=$svc->name?>Client() { Native.dsn_address_get_invalid(out _server.addr); }
        ~<?=$svc->name?>Client() {}

    <?php foreach ($svc->functions as $f) { ?>

        // ---------- call <?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?> ------------
<?php    if ($f->is_one_way()) {?>
        public void <?=$f->name?>(
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, 0, hash);
            s.Write(<?=$f->get_first_param()->name?>);
            s.Flush();
            
            RpcCallOneWay(server != null ? server : _server, s);
        }
<?php    } else { ?>
        // - synchronous 
        public ErrorCode <?=$f->name?>(
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>, 
            out <?=$f->get_csharp_return_type()?> resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, timeout_milliseconds, hash);
            s.Write(<?=$f->get_first_param()->name?>);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(<?=$f->get_csharp_return_type()?>);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack <?=$f->get_first_param()->get_csharp_type()?> and <?=$f->get_csharp_return_type()?> 
        public delegate void <?=$f->name?>Callback(ErrorCode err, <?=$f->get_csharp_return_type()?> resp);
        public void <?=$f->name?>(
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>, 
            <?=$f->name?>Callback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>,timeout_milliseconds, request_hash);
            s.Write(<?=$f->get_first_param()->name?>);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                <?=$f->get_csharp_return_type()?> resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle <?=$f->name?>2(
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>, 
            <?=$f->name?>Callback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>,timeout_milliseconds, request_hash);
            s.Write(<?=$f->get_first_param()->name?>);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                <?=$f->get_csharp_return_type()?> resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       
<?php    }?>
<?php } ?>
    
    }

<?php } ?>
} // end namespace
