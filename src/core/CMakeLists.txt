add_subdirectory(core)
add_subdirectory(tools)
add_subdirectory(tests)
add_subdirectory(rpc)
add_subdirectory(task)

# TODO(zlw) remove perf_counter from dsn_runtime after the refactor by WuTao
add_library(dsn_runtime STATIC
        $<TARGET_OBJECTS:dsn.core>
        $<TARGET_OBJECTS:dsn.rpc>
        $<TARGET_OBJECTS:dsn.task>
        $<TARGET_OBJECTS:dsn.perf_counter>
        $<TARGET_OBJECTS:dsn.tools.common>
        $<TARGET_OBJECTS:dsn.tools.simulator>
        )
target_link_libraries(dsn_runtime dsn_utils)
install(TARGETS dsn_runtime DESTINATION "lib")
