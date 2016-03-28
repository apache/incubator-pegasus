<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using rDSN.Tron.Utility;
using rDSN.Tron.Contract;
using rDSN.Tron;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
    <?php foreach ($_PROG->structs as $s) { ?> 
    public class <?=$s->get_csharp_name()?>
    {   
        <?php foreach ($s->fields as $fld) { ?>
        public <?=$fld->get_csharp_type()?> <?=$fld->name?>;
        <?php } ?>
    }
    <?php } ?>

    <?php foreach ($_PROG->enums as $em) { ?> 
    public enum <?=$em->get_csharp_name()?> {
        <?php foreach ($em->values as $k => $v) { ?>
        <?=$k?> = <?=$v?>,
        <?php } ?>
    }
    <?php } ?>

    <?php foreach ($_PROG->services as $svc) { ?> 
    public sealed class Service_<?=$svc->name?> : rDSN.Tron.Contract.Service
    {
        public  Service_<?=$svc->name?>(string serviceUri, string name = "")
            : base("<?=$_PROG->name?>", serviceUri, name)
        {
            Spec.SType = ServiceSpecType.<?=$idl_type?>;
            Spec.MainSpecFile = "<?=$_PROG->name?>.<?=$idl_type?>";
            Spec.ReferencedSpecFiles = new List<string>();
            Spec.IsRdsnRpc = true;
        }
        <?php foreach ($svc->functions as $f) { ?>
        public <?=$f->get_csharp_return_type()?> <?=$f->name?>(<?=$f->get_first_param()->get_csharp_type()?> <?=$f->get_first_param()->name?>) {
            throw new NotImplementedException("no need to implement, this is just a placeholder");
        }
        <?php } ?>
    }
    <?php } ?>
}
