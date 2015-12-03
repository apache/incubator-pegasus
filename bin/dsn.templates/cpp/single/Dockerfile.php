<?php
require_once($argv[1]);
require_once($argv[2]);
$file_prefix = $argv[3];
?>
FROM rdsn-dev

COPY <?=$_PROG->name?> /home/rdsn/

COPY config.ini /home/rdsn/

ENV LD_LIBRARY_PATH /home/rdsn/lib

CMD ["/home/rdsn/<?=$_PROG->name?>", "/home/rdsn/config.ini" ]

