cmake_minimum_required(VERSION 2.8.8)

add_library(dsn_runtime STATIC
    $<TARGET_OBJECTS:dsn.core>
    $<TARGET_OBJECTS:dsn.nfs>
    $<TARGET_OBJECTS:dsn.tools.common>
    $<TARGET_OBJECTS:dsn.tools.hpc>
    $<TARGET_OBJECTS:dsn.tools.simulator>
    )
