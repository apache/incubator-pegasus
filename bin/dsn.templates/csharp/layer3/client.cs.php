<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$_IDL_FORMAT = $argv[4];
?>
using System;
using System.IO;
using dsn.dev.csharp;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
<?php foreach ($_PROG->services as $svc) { ?>
    public class <?=$svc->name?>Client : Clientlet
    {
        private RpcAddress _server;
        
        public <?=$svc->name?>Client(RpcAddress server) { _server = server; }
        public <?=$svc->name?>Client() { }
        ~<?=$svc->name?>Client() {}

    <?php foreach ($svc->functions as $f) { ?>

        // ---------- call <?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?> ------------
<?php    if ($f->is_one_way()) {?>
        public void <?=$f->name?>(
            <?=$f->get_csharp_request_type_name()?> args,
            int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
            UInt64 partition_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, 0, thread_hash, partition_hash);
            s.Write(args);
            s.Flush();
            
            RpcCallOneWay(server != null ? server : _server, s);
        }
<?php    } else { ?>
        // - synchronous 
        public ErrorCode <?=$f->name?>(
            <?=$f->get_csharp_request_type_name()?> args,
            out <?=$f->get_csharp_return_type()?> resp, 
            int timeout_milliseconds = 0, 
            int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
            UInt64 partition_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, timeout_milliseconds, thread_hash, partition_hash);
            s.Write(args);
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
        
        // - asynchronous with on-stack <?=$f->get_csharp_request_type_name()?> and <?=$f->get_csharp_return_type()?> 
        public delegate void <?=$f->name?>Callback(ErrorCode err, <?=$f->get_csharp_return_type()?> resp);
        public void <?=$f->name?>(
            <?=$f->get_csharp_request_type_name()?> args,
            <?=$f->name?>Callback callback,
            int timeout_milliseconds = 0, 
            int reply_thread_hash = 0,
            int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
            UInt64 request_partition_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>,timeout_milliseconds, request_thread_hash, request_partition_hash);
            s.Write(args);
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
                        reply_thread_hash
                        );
        }        
        
        public SafeTaskHandle <?=$f->name?>2(
            <?=$f->get_csharp_request_type_name()?> args,
            <?=$f->name?>Callback callback,
            int timeout_milliseconds = 0, 
            int reply_thread_hash = 0,
            int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
            UInt64 request_partition_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>,timeout_milliseconds, request_thread_hash, request_partition_hash);
            s.Write(args);
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
                        reply_thread_hash
                        );
        }       
<?php    }?>
<?php } ?>
    
    }

<?php } ?>
} // end namespace
