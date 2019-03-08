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

function(ms_add_project PROJ_TYPE PROJ_NAME PROJ_SRC PROJ_INC_PATH PROJ_LIBS PROJ_LIB_PATH PROJ_BINPLACES)
    if(NOT((PROJ_TYPE STREQUAL "STATIC") OR (PROJ_TYPE STREQUAL "SHARED") OR
           (PROJ_TYPE STREQUAL "EXECUTABLE") OR (PROJ_TYPE STREQUAL "OBJECT")))
        message(FATAL_ERROR "Invalid project type.")
    endif()

    if(PROJ_SRC STREQUAL "")
        message(FATAL_ERROR "No source files.")
    endif()

    if(NOT (PROJ_INC_PATH STREQUAL ""))
        include_directories(${PROJ_INC_PATH})
    endif()
    if(NOT (PROJ_LIB_PATH STREQUAL ""))
        link_directories(${PROJ_LIB_PATH})
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
# - MY_PROJ_INC_PATH TODO(wutao1): remove this
# - MY_PROJ_LIB_PATH TODO(wutao1): remove this
# - MY_PROJ_LIBS
# - MY_BINPLACES
#     Extra files that will be installed
# - MY_BOOST_PACKAGES
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

    if(NOT DEFINED MY_PROJ_INC_PATH)
        set(MY_PROJ_INC_PATH "")
    endif()
    if(NOT DEFINED MY_PROJ_LIBS)
        set(MY_PROJ_LIBS "")
    endif()
    if(NOT DEFINED MY_PROJ_LIB_PATH)
        set(MY_PROJ_LIB_PATH "")
    endif()
    if(NOT DEFINED MY_BINPLACES)
        set(MY_BINPLACES "")
    endif()
    if(NOT DEFINED MY_BOOST_PACKAGES)
        set(MY_BOOST_PACKAGES "")
    endif()

    set(MY_BOOST_LIBS "")
    if(NOT (MY_BOOST_PACKAGES STREQUAL ""))
        ms_setup_boost(TRUE "${MY_BOOST_PACKAGES}" MY_BOOST_LIBS)
    endif()

    if((MY_PROJ_TYPE STREQUAL "SHARED") OR (MY_PROJ_TYPE STREQUAL "EXECUTABLE"))
        if(DSN_BUILD_RUNTIME AND(DEFINED DSN_IN_CORE) AND DSN_IN_CORE)
            set(TEMP_LIBS "")
        else()
            set(TEMP_LIBS dsn_runtime)
        endif()
        set(MY_PROJ_LIBS ${MY_PROJ_LIBS} ${TEMP_LIBS} ${MY_BOOST_LIBS} ${DSN_SYSTEM_LIBS})
    endif()

    ms_add_project("${MY_PROJ_TYPE}" "${MY_PROJ_NAME}" "${MY_PROJ_SRC}" "${MY_PROJ_INC_PATH}" "${MY_PROJ_LIBS}" "${MY_PROJ_LIB_PATH}" "${MY_BINPLACES}")
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
    else()
        add_definitions(-g)
        add_definitions(-O2)
        add_definitions(-DDSN_BUILD_TYPE=Release)
    endif()
    cmake_host_system_information(RESULT BUILD_HOSTNAME QUERY HOSTNAME)
    add_definitions(-DDSN_BUILD_HOSTNAME=${BUILD_HOSTNAME})

    # We want access to the PRI* print format macros.
    add_definitions(-D__STDC_FORMAT_MACROS)

    # -fno-omit-frame-pointer
    #   use frame pointers to allow simple stack frame walking for backtraces.
    #   This has a small perf hit but worth it for the ability to profile in production
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -fno-omit-frame-pointer" CACHE STRING "" FORCE)

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

    find_program(CCACHE_FOUND ccache)
    if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
        if ("${COMPILER_FAMILY}" STREQUAL "clang")
            add_compile_options(-Qunused-arguments)
        endif()
        message(STATUS "use ccache to speed up compilation")
    endif(CCACHE_FOUND)

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

macro(ms_setup_boost STATIC_LINK PACKAGES BOOST_LIBS)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)

    find_package(Boost COMPONENTS ${PACKAGES} REQUIRED)

    if(NOT Boost_FOUND)
        message(FATAL_ERROR "Cannot find library boost")
    endif()

    set(TEMP_LIBS "")
    foreach(PACKAGE ${PACKAGES})
        string(TOUPPER ${PACKAGE} PACKAGE)
        set(TEMP_LIBS ${TEMP_LIBS} ${Boost_${PACKAGE}_LIBRARY})
    endforeach()
    set(${BOOST_LIBS} ${TEMP_LIBS})
endmacro(ms_setup_boost)

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
        thrift
        ${CMAKE_THREAD_LIBS_INIT} # the thread library found by FindThreads
        CACHE STRING "rDSN system libs" FORCE
    )
endfunction(dsn_setup_system_libs)

function(dsn_setup_include_path)
    if(DEFINED BOOST_ROOT)
        include_directories(${BOOST_ROOT}/include)
    endif()
    include_directories(${BOOST_INCLUDEDIR})
    include_directories(${DSN_THIRDPARTY_ROOT}/include)
endfunction(dsn_setup_include_path)

function(dsn_setup_link_path)
    link_directories(${BOOST_LIBRARYDIR})
    link_directories(${DSN_THIRDPARTY_ROOT}/lib)
endfunction(dsn_setup_link_path)

function(dsn_common_setup)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__FILENAME__='\"$(notdir $(abspath $<))\"'")

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
    dsn_setup_link_path()
endfunction(dsn_common_setup)