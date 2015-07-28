file("GLOB"
    DSN_EXTRA_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../apps/nfs/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../apps/nfs/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../apps/nfs/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../apps/nfs/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../apps/nfs/*.hpp"
    )
set(PROJ_SRC ${PROJ_SRC} ${DSN_EXTRA_SRC})
dsn_add_library(dsn.tools.nfs)
