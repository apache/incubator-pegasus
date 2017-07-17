/*!
 @defgroup example-complexity Example
 @ingroup tools

 Build a perfect failure detector with progressively added system complexity

 Code is [here](https://github.com/Microsoft/rDSN/tree/master/src/dist/failure_detector)

Detailed tutorial with FD coming later~

***

This tutorial is to demonstrate how rDSN helps develop and debug a system with progressively added
system complexity. You can turn the system complexities using the following toggles in the
configuration file. We usually start form very simple scenarios to ensure the basic logic flow
working smoothly. We then **gradually** add system complexity so that when things go wrong, the
reasoning space is relatively small. We also change the configurations to see whether a specific
error is still present or not, to help guess the root cause.

* Single thread versus multiple thread

You can setup thread worker count for each thread pool specifically, usually from 1 to avoid
concurrency complexities and bugs initially. Note some of the thread pools may not function
correctly when your service uses blocking calls (e.g., ```task::wait```) in the code. In that case,
worker count cannot be 1.

```
[threadpool.default]
worker_count = 1

[threadpool.THREAD_POOL_REPLICATION]
worker_count = 1
```

* Deterministic execution versus non-deterministic execution

A deterministic execution help repeat the same buggy scenario again and again until the whole flow
is fixed.

```
[core]
tool = simulator

[tools.simulator]
random_seed = 810804960
use_given_random_seed = true
```

* Non-fault environment versus Fault environment (disk, network)

You can firstly develop our system in a non-faulty environment. After everything seems ok, you can
gradually add faults to specific types of tasks, until all of them.

```
[task.default]
rpc_request_drop_ratio = 0.0
rpc_response_drop_ratio = 0.05
disk_read_fail_ratio = 0.0
disk_write_fail_ratio = 0.0

[task.LPC_REPLICATION_LOGGING]
disk_read_fail_ratio = 0.001
disk_write_fail_ratio = 0.01
```

* Standalone server versus shared server (resource interference)

Fault injection also provides options about the network, disk, and CPU interference etc.

```
[task.default]
rpc_message_delay_ms_min = 0
rpc_message_delay_ms_max = 1000
disk_io_delay_ms_min = 1
disk_io_delay_ms_max = 20
execution_extra_delay_us_max = 0

[task.LPC_GENERATE_THUMBNAIL]
execution_extra_delay_us_max = 100000
```

* Automated exploration of the combinations above

When fault injection is on, and thread number is set to be larger than 1. The system gets enough
complexities, and rDSN can help systematically explores the various combinations, including all the
faults, message orders, and thread scheduling decisions.

```

[core]

tool = simulator
;tool = nativerun
;toollets = tracer
toollets = fault_injector
;toollets = tracer, fault_injector
;toollets = tracer, profiler, fault_injector
;toollets = profiler, fault_injector
pause_on_start = false

;logging_factory_name = dsn::tools::screen_logger

[tools.simulator]
random_seed = 810804960
use_given_random_seed = false
```
When things go wrong, you can repeat the bugs. See examples
[here](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-8-systematic-test-against-various-failures-and-scheduling-decisions).


 */
