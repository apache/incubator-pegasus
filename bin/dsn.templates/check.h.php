<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
# pragma once

# include <dsn/internal/configuration.h>

<?=$_PROG->get_cpp_namespace_begin()?>

    void install_checkers(configuration_ptr config);
    
<?=$_PROG->get_cpp_namespace_end()?>
