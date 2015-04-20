<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
[apps.<?=$_PROG->name?>.server]
name = <?=$_PROG->name?>.server
type = <?=$_PROG->name?>_server
arguments = 
port = 27001
run = true
    
[apps.client]
name = client
type = <?=$_PROG->name?>_client
arguments = localhost 27001
count = 1
run = true
    
[core]

tool = simulator
;tool = nativerun
toollets = tracer
;toollets = tracer, profiler, fault_injector
pause_on_start = false

logging_factory_name = dsn::tools::screen_logger

[tools.simulator]
random_seed = 2756568580
use_given_random_seed = false

[network]
; channel.message_format = network_provider_name, buffer_block_size
; each format will occupy a port(from app.port to app.port + 1, ...)
;RPC_CHANNEL_TCP.dsn = dsn::tools::asio_network_provider, 65536
;RPC_CHANNEL_UDP.dsn = dsn::tools::asio_network_provider, 65536
;RPC_CHANNEL_TCP.thrift = dsn::tools::asio_network_provider, 65536
;RPC_CHANNEL_UDP.thrift = dsn::tools::asio_network_provider, 65536
; how many network threads for network library(used by asio)
io_service_worker_count = 2

; specification for each thread pool
[threadpool.default]

[threadpool.THREAD_POOL_DEFAULT]
name = default
partitioned = false
worker_count = 1
max_input_queue_length = 1024
worker_priority = THREAD_xPRIORITY_NORMAL

[task.default]
is_trace = true
is_profile = true
allow_inline = false
rpc_message_channel = RPC_CHANNEL_TCP
fast_execution_in_network_thread = false
rpc_message_header_format = dsn
rpc_timeout_milliseconds = 5000

[task.LPC_AIO_IMMEDIATE_CALLBACK]
is_trace = false
is_profile = false
allow_inline = false

[task.LPC_RPC_TIMEOUT]
is_trace = false
is_profile = false
