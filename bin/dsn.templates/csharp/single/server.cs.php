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
    public class <?=$svc->name?>Server : Serverlet<<?=$svc->name?>Server>
    {
        public <?=$svc->name?>Server() : base("<?=$svc->name?>") {}
        ~<?=$svc->name?>Server() { CloseService(); }
    
        // all service handlers to be implemented further
<?php foreach ($svc->functions as $f) { ?>
        // <?=$f->get_rpc_code()?> 
<?php     if ($f->is_one_way()) {?>
        private void On<?=$f->name?>Internal(RpcReadStream request)
        {
            <?=$f->get_csharp_request_type_name()?> args;
            
            try 
            {
                request.Read(out args);
            } 
            catch (Exception e)
            {
                // TODO: error handling
                return;
            }
            
            On<?=$f->name?>(args);
        }
        
        protected virtual void On<?=$f->name?>(<?=$f->get_csharp_request_type_name()?> args)
        {
            Console.WriteLine("... exec <?=$f->get_rpc_code()?> ... (not implemented) ");
        }
        
<?php     } else {?>
        private void On<?=$f->name?>Internal(RpcReadStream request, RpcWriteStream response)
        {
            <?=$f->get_csharp_request_type_name()?> args;
            
            try 
            {
                request.Read(out args);
            } 
            catch (Exception e)
            {
                // TODO: error handling
                return;
            }
            
            On<?=$f->name?>(args, new RpcReplier<<?=$f->get_csharp_return_type()?>>(response, (s, r) => 
            {
                s.Write(r);
                s.Flush();
            }));
        }
        
        protected virtual void On<?=$f->name?>(<?=$f->get_csharp_request_type_name()?> args, RpcReplier<<?=$f->get_csharp_return_type()?>> replier)
        {
            Console.WriteLine("... exec <?=$f->get_rpc_code()?> ... (not implemented) ");
            var resp =  new <?=$f->get_csharp_return_type()?>();
            replier.Reply(resp);
        }
        
<?php     } ?>
<?php } ?>
        
        public void OpenService(UInt64 gpid)
        {
<?php foreach ($svc->functions as $f) { ?>
            RegisterRpcHandler(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, "<?=$f->name?>", this.On<?=$f->name?>Internal, gpid);
<?php } ?>
        }

        public void CloseService(UInt64 gpid)
        {
<?php foreach ($svc->functions as $f) { ?>
            UnregisterRpcHandler(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, gpid);
<?php } ?>
        }
    }

<?php } ?>

} // end namespace
