<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
[apps..default]
run = true
count = 1
network.server.0.RPC_CHANNEL_TCP = NET_HDR_HTTP, dsn::tools::asio_network_provider, 65536

[apps.server]
name = server
type = server
arguments = 
ports = 27001
run = true
pools = THREAD_POOL_DEFAULT
    
[apps.client]
name = client
type = client
arguments = localhost 27001
count = 0
run = true
pools = THREAD_POOL_DEFAULT

<?php foreach ($_PROG->services as $svc) { ?>
[apps.client.perf.<?=$svc->name?>] 
name = client.perf.<?=$svc->name?> 
type = client.perf.<?=$svc->name?> 
arguments = localhost 27001 
count = 1
run = false
<?php } ?>

[core]

;tool = simulator
tool = nativerun
;toollets = tracer
;toollets = tracer, profiler, fault_injector
pause_on_start = false

logging_factory_name = dsn::tools::screen_logger

[tools.simulator]
random_seed = 0

[network]
; how many network threads for network library(used by asio)
io_service_worker_count = 2

; specification for each thread pool
[threadpool..default]

[threadpool.THREAD_POOL_DEFAULT]
name = default
partitioned = false
worker_count = 1
max_input_queue_length = 1024
worker_priority = THREAD_xPRIORITY_NORMAL

[task..default]
is_trace = true
is_profile = true
allow_inline = false
rpc_call_channel = RPC_CHANNEL_TCP
fast_execution_in_network_thread = false
rpc_call_header_format_name = dsn
rpc_timeout_milliseconds = 5000
perf_test_rounds = 10000
rpc_call_header_format = NET_HDR_HTTP

[task.LPC_AIO_IMMEDIATE_CALLBACK]
is_trace = false
is_profile = false
allow_inline = false

[task.LPC_RPC_TIMEOUT]
is_trace = false
is_profile = false
