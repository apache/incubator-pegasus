add_subdirectory(test)
add_subdirectory(rpc)
add_subdirectory(task)

# TODO(zlw) remove perf_counter from dsn_runtime after the refactor by WuTao
add_library(dsn_runtime STATIC
        $<TARGET_OBJECTS:dsn.rpc>
        $<TARGET_OBJECTS:dsn.task>
        $<TARGET_OBJECTS:dsn.perf_counter>
        core_main.cpp
        dsn.layer2_types.cpp
        env.sim.cpp
        explorer.cpp
        fault_injector.cpp
        global_config.cpp
        message_utils.cpp
        nativerun.cpp
        profiler.cpp
        profiler_command.cpp
        profiler_output.cpp
        providers.common.cpp
        scheduler.cpp
        service_api_c.cpp
        service_engine.cpp
        simulator.cpp
        threadpool_code.cpp
        tool_api.cpp
        tracer.cpp
        zlocks.cpp
        )
target_link_libraries(dsn_runtime dsn_utils)
install(TARGETS dsn_runtime DESTINATION "lib")
