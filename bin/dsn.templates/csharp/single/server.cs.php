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
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>;
            
            try 
            {
                request.Read(out <?=$f->get_first_param()->name?>);
            } 
            catch (Exception e)
            {
                // TODO: error handling
                return;
            }
            
            On<?=$f->name?>(<?=$f->get_first_param()->name?>);
        }
        
        protected virtual void On<?=$f->name?>(<?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>)
        {
            Console.WriteLine("... exec <?=$f->get_rpc_code()?> ... (not implemented) ");
        }
        
<?php     } else {?>
        private void On<?=$f->name?>Internal(RpcReadStream request, RpcWriteStream response)
        {
            <?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>;
            
            try 
            {
                request.Read(out <?=$f->get_first_param()->name?>);
            } 
            catch (Exception e)
            {
                // TODO: error handling
                return;
            }
            
            On<?=$f->name?>(<?=$f->get_first_param()->name?>, new RpcReplier<<?=$f->get_csharp_return_type()?>>(response, (s, r) => {s.Write(r);}));
        }
        
        protected virtual void On<?=$f->name?>(<?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>, RpcReplier<<?=$f->get_csharp_return_type()?>> replier)
        {
            Console.WriteLine("... exec <?=$f->get_rpc_code()?> ... (not implemented) ");
            var resp =  new <?=$f->get_csharp_return_type()?>();
            replier.Reply(resp);
        }
        
<?php     } ?>
<?php } ?>
        
        public void OpenService()
        {
<?php foreach ($svc->functions as $f) { ?>
            RegisterRpcHandler(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>, "<?=$f->name?>", this.On<?=$f->name?>Internal);
<?php } ?>
        }

        public void CloseService()
        {
<?php foreach ($svc->functions as $f) { ?>
            UnregisterRpcHandler(<?=$_PROG->name?>Helper.<?=$f->get_rpc_code()?>);
<?php } ?>
        }
    }

<?php } ?>

} // end namespace
