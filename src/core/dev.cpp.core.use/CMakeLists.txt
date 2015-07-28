file("GLOB"
    DSN_EXTRA_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dev/cpp/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dev/cpp/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dev/cpp/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dev/cpp/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dev/cpp/*.hpp"
    )
set(PROJ_SRC ${PROJ_SRC} ${DSN_EXTRA_SRC})
dsn_add_library(dsn.dev.cpp.core.use)
