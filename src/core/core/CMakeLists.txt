
file("GLOB"
    DSN_EXTRA_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/../coreapi/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../coreapi/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../coreapi/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/../coreapi/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/../coreapi/*.hpp"
    )

set(BOOST_PACKAGES
    ${DSN_EXTRA_BOOST_PACKAGES}
    thread
    regex
    system
    filesystem
    chrono
    date_time
    )
ms_setup_boost(1 "${BOOST_PACKAGES}")

if(UNIX)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
endif()
find_package(Threads REQUIRED)
if(UNIX AND CMAKE_USE_PTHREADS_INIT)
    add_compile_options(-pthread)
endif()
    
set(DSN_SYSTEM_LIBS "")
set(DSN_CORE_LIBS "")
set(DSN_LIBS "")
set(DSN_CORE_TARGETS
    dsn.failure_detector
    dsn.dev.cpp
    dsn.core
    )

if(UNIX AND (NOT APPLE))
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} rt)
endif()

if("${CMAKE_SYSTEM}" MATCHES "Linux")
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} aio)
endif()

set(DSN_SYSTEM_LIBS
    ${DSN_SYSTEM_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${BOOST_REQUIRED_LIBS}
)

dsn_add_dynamic_library(dsn.core)

target_link_libraries(
    dsn.core
    LINK_PRIVATE
    ${DSN_SYSTEM_LIBS}
    dsn.tools.nfs
    dsn.tools.simulator
    dsn.tools.common
    dsn.dev.cpp.core.use
    )

