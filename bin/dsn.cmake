include(${CMAKE_CURRENT_LIST_DIR}/compiler_info.cmake)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR};${CMAKE_MODULE_PATH}) # TODO: move all Find*.cmake into cmake/

# Always generate the compilation database file (compile_commands.json) for use
# with various development tools, such as IWYU and Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

# Set DSN_PROJECT_DIR to rdsn/
set(DSN_PROJECT_DIR ${CMAKE_CURRENT_LIST_DIR})
get_filename_component(DSN_PROJECT_DIR ${DSN_PROJECT_DIR} DIRECTORY)

# Set DSN_THIRDPARTY_ROOT to rdsn/thirdparty/output
set(DSN_THIRDPARTY_ROOT ${DSN_PROJECT_DIR}/thirdparty/output)
message(STATUS "DSN_THIRDPARTY_ROOT = ${DSN_THIRDPARTY_ROOT}")

# Set DSN_ROOT to rdsn/DSN_ROOT, this is where rdsn will be installed
set(DSN_ROOT ${DSN_PROJECT_DIR}/DSN_ROOT)
message(STATUS "DSN_ROOT = ${DSN_ROOT}")

option(BUILD_TEST "build unit test" ON)
message(STATUS "BUILD_TEST = ${BUILD_TEST}")

option(ENABLE_GCOV "Enable gcov (for code coverage analysis)" OFF)
message(STATUS "ENABLE_GCOV = ${ENABLE_GCOV}")

# Disable this option before running valgrind.
option(ENABLE_GPERF "Enable gperftools (for tcmalloc)" ON)
message(STATUS "ENABLE_GPERF = ${ENABLE_GPERF}")

# ================================================================== #


# Install this target into ${CMAKE_INSTALL_PREFIX}/lib
function(dsn_install_library)
    install(TARGETS ${MY_PROJ_NAME} DESTINATION "lib")
endfunction()

# Install this target into ${CMAKE_INSTALL_PREFIX}/bin/${PROJ_NAME}
function(dsn_install_executable)
    set(MY_PROJ_TYPE "EXECUTABLE")
    set(INSTALL_DIR "bin/${MY_PROJ_NAME}")
    install(TARGETS ${MY_PROJ_NAME} DESTINATION "${INSTALL_DIR}")

    # install the extra files together with the executable
    if(NOT (MY_BINPLACES STREQUAL ""))
        foreach(BF ${MY_BINPLACES})
            install(FILES ${BF} DESTINATION "${INSTALL_DIR}")
        endforeach()
    endif()
endfunction()

function(ms_add_project PROJ_TYPE PROJ_NAME PROJ_SRC PROJ_LIBS PROJ_BINPLACES)
    if(NOT((PROJ_TYPE STREQUAL "STATIC") OR (PROJ_TYPE STREQUAL "SHARED") OR
           (PROJ_TYPE STREQUAL "EXECUTABLE") OR (PROJ_TYPE STREQUAL "OBJECT")))
        message(FATAL_ERROR "Invalid project type.")
    endif()

    if(PROJ_SRC STREQUAL "")
        message(FATAL_ERROR "No source files.")
    endif()

    if((PROJ_TYPE STREQUAL "STATIC") OR (PROJ_TYPE STREQUAL "OBJECT"))
        add_library(${PROJ_NAME} ${PROJ_TYPE} ${PROJ_SRC})
    elseif(PROJ_TYPE STREQUAL "SHARED")
        add_library(${PROJ_NAME} ${PROJ_TYPE} ${PROJ_SRC})
    elseif(PROJ_TYPE STREQUAL "EXECUTABLE")
        add_executable(${PROJ_NAME} ${PROJ_SRC})
    endif()

    if((PROJ_TYPE STREQUAL "SHARED") OR (PROJ_TYPE STREQUAL "EXECUTABLE"))
        if(PROJ_TYPE STREQUAL "SHARED")
            set(LINK_MODE PRIVATE)
        else()
            set(LINK_MODE PUBLIC)
        endif()
        target_link_libraries(${PROJ_NAME} "${LINK_MODE}" ${PROJ_LIBS})
    endif()
endfunction(ms_add_project)


# Parameters:
# - MY_PROJ_TYPE
# - MY_PROJ_NAME
# - MY_SRC_SEARCH_MODE
#     Search mode for source files under current project directory
#     "GLOB_RECURSE" for recursive search
#     "GLOB" for non-recursive search
# - MY_PROJ_SRC
# - MY_PROJ_LIBS
# - MY_BINPLACES
#     Extra files that will be installed
# - MY_BOOST_LIBS
function(dsn_add_project)
    if((NOT DEFINED MY_PROJ_TYPE) OR (MY_PROJ_TYPE STREQUAL ""))
        message(FATAL_ERROR "MY_PROJ_TYPE is empty.")
    endif()
    if((NOT DEFINED MY_PROJ_NAME) OR (MY_PROJ_NAME STREQUAL ""))
        message(FATAL_ERROR "MY_PROJ_NAME is empty.")
    endif()
    if(NOT DEFINED MY_SRC_SEARCH_MODE)
        set(MY_SRC_SEARCH_MODE "GLOB")
    endif()

    # find source files from current directory
    if(NOT DEFINED MY_PROJ_SRC)
        set(MY_PROJ_SRC "")
    endif()
    set(TEMP_SRC "")
    # We restrict the file suffix to keep our codes consistent.
    file(${MY_SRC_SEARCH_MODE} TEMP_SRC
         "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
         "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
         )
    set(MY_PROJ_SRC ${TEMP_SRC} ${MY_PROJ_SRC})

    if(NOT DEFINED MY_PROJ_LIBS)
        set(MY_PROJ_LIBS "")
    endif()
    if(NOT DEFINED MY_BINPLACES)
        set(MY_BINPLACES "")
    endif()

    if(NOT DEFINED MY_BOOST_LIBS)
        set(MY_BOOST_LIBS "")
    endif()

    if((MY_PROJ_TYPE STREQUAL "SHARED") OR (MY_PROJ_TYPE STREQUAL "EXECUTABLE"))
        set(MY_PROJ_LIBS ${MY_PROJ_LIBS} ${DEFAULT_THIRDPARTY_LIBS} ${MY_BOOST_LIBS} ${DSN_SYSTEM_LIBS})
    endif()
    ms_add_project("${MY_PROJ_TYPE}" "${MY_PROJ_NAME}" "${MY_PROJ_SRC}" "${MY_PROJ_LIBS}" "${MY_BINPLACES}")
endfunction(dsn_add_project)

function(dsn_add_static_library)
    set(MY_PROJ_TYPE "STATIC")
    dsn_add_project()
    dsn_install_library()
endfunction(dsn_add_static_library)

function(dsn_add_shared_library)
    set(MY_PROJ_TYPE "SHARED")
    dsn_add_project()
    dsn_install_library()
endfunction(dsn_add_shared_library)

function(dsn_add_executable)
    set(MY_PROJ_TYPE "EXECUTABLE")
    dsn_add_project()
endfunction(dsn_add_executable)

function(dsn_add_object)
    set(MY_PROJ_TYPE "OBJECT")
    dsn_add_project()
endfunction(dsn_add_object)

function(dsn_add_test)
    if(${BUILD_TEST})
        set(MY_EXECUTABLE_IS_TEST TRUE)
        dsn_add_executable()

        file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
        execute_process(COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_BINARY_DIR}/bin/${MY_PROJ_NAME})

        # copy the extra files together with the executable
        if(NOT (MY_BINPLACES STREQUAL ""))
            foreach(BF ${MY_BINPLACES})
                FILE(COPY ${BF} DESTINATION "${CMAKE_BINARY_DIR}/bin/${MY_PROJ_NAME}")
            endforeach()
        endif()
    endif()
endfunction()

function(dsn_setup_compiler_flags)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        add_definitions(-DDSN_BUILD_TYPE=Debug)
        add_definitions(-g)
    else()
        add_definitions(-g)
        add_definitions(-O2)
        add_definitions(-DDSN_BUILD_TYPE=Release)
    endif()
    cmake_host_system_information(RESULT BUILD_HOSTNAME QUERY HOSTNAME)
    add_definitions(-DDSN_BUILD_HOSTNAME=${BUILD_HOSTNAME})

    # We want access to the PRI* print format macros.
    add_definitions(-D__STDC_FORMAT_MACROS)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y" CACHE STRING "" FORCE)

    #  -Wall: Enable all warnings.
    add_compile_options(-Wall)
    add_compile_options(-Werror)
    #  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned integers
    add_compile_options(-Wno-sign-compare)
    add_compile_options(-Wno-strict-aliasing)
    add_compile_options(-Wuninitialized)
    add_compile_options(-Wno-unused-result)
    add_compile_options(-Wno-unused-variable)
    add_compile_options(-Wno-deprecated-declarations)
    add_compile_options(-Wno-inconsistent-missing-override)
    add_compile_options(-Wno-attributes)
    # -fno-omit-frame-pointer
    #   use frame pointers to allow simple stack frame walking for backtraces.
    #   This has a small perf hit but worth it for the ability to profile in production
    add_compile_options( -fno-omit-frame-pointer)

    find_program(CCACHE_FOUND ccache)
    if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
        if ("${COMPILER_FAMILY}" STREQUAL "clang")
            add_compile_options(-Qunused-arguments)
        endif()
        message(STATUS "use ccache to speed up compilation")
    endif(CCACHE_FOUND)

    # add sanitizer check
    if(DEFINED SANITIZER)
        if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
        ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.8")))
            message(SEND_ERROR "Cannot use sanitizer without clang or gcc >= 4.8")
        endif()

        message(STATUS "Running cmake with sanitizer=${SANITIZER}")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=${SANITIZER}" CACHE STRING "" FORCE)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=${SANITIZER}" CACHE STRING "" FORCE)
    endif()

    set(CMAKE_EXE_LINKER_FLAGS
        "${CMAKE_EXE_LINKER_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
        CACHE
        STRING
        ""
        FORCE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
        CACHE
        STRING
        ""
        FORCE)
endfunction(dsn_setup_compiler_flags)

# find necessary system libs
function(dsn_setup_system_libs)
    find_package(Threads REQUIRED)

    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS ON)
        message(STATUS "FIND_LIBRARY_USE_LIB64_PATHS = ON")
    endif()

    set(DSN_SYSTEM_LIBS "")

    find_package(RT REQUIRED)
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${RT_LIBRARIES})

    find_package(AIO REQUIRED)
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${AIO_LIBRARIES})

    find_package(DL REQUIRED)
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${DL_LIBRARIES})

    # for md5 calculation
    find_package(OpenSSL REQUIRED)
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${OPENSSL_CRYPTO_LIBRARY})

    if(ENABLE_GPERF)
        set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} tcmalloc_and_profiler)
        add_definitions(-DDSN_ENABLE_GPERF)
    endif()

    set(DSN_SYSTEM_LIBS
        ${DSN_SYSTEM_LIBS}
        ${CMAKE_THREAD_LIBS_INIT} # the thread library found by FindThreads
        CACHE STRING "rDSN system libs" FORCE
    )
endfunction(dsn_setup_system_libs)

function(dsn_setup_include_path)#TODO(huangwei5): remove this
    include_directories(${DSN_THIRDPARTY_ROOT}/include)
endfunction(dsn_setup_include_path)

function(dsn_setup_thirdparty_libs)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)

    set(CMAKE_PREFIX_PATH ${DSN_THIRDPARTY_ROOT};${CMAKE_PREFIX_PATH})
    find_package(Boost COMPONENTS system filesystem regex REQUIRED)
    include_directories(${Boost_INCLUDE_DIRS})

    find_library(THRIFT_LIB NAMES libthrift.a PATHS ${DSN_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)
    if(NOT THRIFT_LIB)
        message(FATAL_ERROR "thrift library not found in ${DSN_THIRDPARTY_ROOT}/lib")
    endif()
    find_package(fmt REQUIRED)
    set(DEFAULT_THIRDPARTY_LIBS ${THRIFT_LIB} fmt::fmt CACHE STRING "default thirdparty libs" FORCE)

    # rocksdb
    file(GLOB ROCKSDB_DEPENDS_MODULE_PATH ${DSN_PROJECT_DIR}/thirdparty/src/*/cmake/modules)
    if(NOT ROCKSDB_DEPENDS_MODULE_PATH)
        message(WARNING "Cannot find RocksDB depends cmake modules path, might not find snappy, zstd, lz4")
    endif()
    list(APPEND CMAKE_MODULE_PATH "${ROCKSDB_DEPENDS_MODULE_PATH}")
    find_package(snappy)
    find_package(zstd)
    find_package(lz4)
    find_package(RocksDB REQUIRED)

    link_directories(${DSN_THIRDPARTY_ROOT}/lib)
    link_directories(${DSN_THIRDPARTY_ROOT}/lib64)
endfunction(dsn_setup_thirdparty_libs)

function(dsn_common_setup)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__FILENAME__='\"$(notdir $(abspath $<))\"'" CACHE STRING "" FORCE)

    if(NOT (UNIX))
        message(FATAL_ERROR "Only Unix are supported.")
    endif()

    if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
        message(FATAL_ERROR "In-source builds are not allowed.")
    endif()

    if(NOT DEFINED DSN_BUILD_RUNTIME)
        set(DSN_BUILD_RUNTIME FALSE)
    endif()

    set(BUILD_SHARED_LIBS OFF)

    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-std=c++1y" COMPILER_SUPPORTS_CXX1Y)
    if(NOT ${COMPILER_SUPPORTS_CXX1Y})
        message(FATAL_ERROR "You need a compiler with C++1y support.")
    endif()

    dsn_setup_system_libs()
    dsn_setup_compiler_flags()
    dsn_setup_include_path()
    dsn_setup_thirdparty_libs()
endfunction(dsn_common_setup)
