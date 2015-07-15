function(ms_add_library PROJ_TYPE PROJ_NAME PROJ_SRC DO_INSTALL)
    if(NOT((PROJ_TYPE STREQUAL "STATIC") OR (PROJ_TYPE STREQUAL "SHARED") OR (PROJ_TYPE STREQUAL "MODULE")))
        message(FATAL_ERROR "Invalid project type")
    endif()
    
    if(PROJ_NAME STREQUAL "")
        message(FATAL_ERROR "Invalid project name")
    endif()

    if(MSVC)
        add_definitions(-D_LIB)
    endif()

    include_directories(${DSN_EXTRA_INCLUDEDIR})
    add_library(${PROJ_NAME} ${PROJ_TYPE} ${PROJ_SRC})

    if(DO_INSTALL)
        install(TARGETS ${PROJ_NAME} DESTINATION lib)
    endif()
endfunction(ms_add_library PROJ_TYPE PROJ_NAME PROJ_SRC DO_INSTALL)

function(ms_add_executable PROJ_NAME PROJ_SRC INPUT_LIBS BINPLACE_FILES DO_INSTALL)
    if(PROJ_NAME STREQUAL "")
        message(FATAL_ERROR "Invalid project name")
    endif()
    
    if(MSVC)
        add_definitions(-D_CONSOLE)
    endif()

    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${PROJ_NAME}")
    set(INSTALL_BINPLACE_DIR "bin/${PROJ_NAME}")

    include_directories(${DSN_EXTRA_INCLUDEDIR})
    link_directories(${DSN_EXTRA_LIBRARYDIR})
    add_executable(${PROJ_NAME} ${PROJ_SRC})
    target_link_libraries(${PROJ_NAME} LINK_PUBLIC ${INPUT_LIBS})
    
    set(BINPLACE_DIR "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/")
    foreach(BF ${BINPLACE_FILES})
        add_custom_command(
            TARGET ${PROJ_NAME}
            POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy ${BF} "${BINPLACE_DIR}"
            )
        if(DO_INSTALL)
            install(FILES ${BF} DESTINATION "${INSTALL_BINPLACE_DIR}")
        endif()
    endforeach()

    if(DO_INSTALL)
        install(TARGETS ${PROJ_NAME} DESTINATION "${INSTALL_BINPLACE_DIR}")
    endif()
endfunction(ms_add_executable PROJ_NAME PROJ_SRC INPUT_LIBS BINPLACE_FILES DO_INSTALL)

macro(ms_add_compiler_flags LANGUAGES SUFFIXES FLAGS)
    foreach(LANG ${LANGUAGES})
        foreach(SUFFIX ${SUFFIXES})
            if(SUFFIX STREQUAL "<EMPTY>")
                set(SUFFIX "")
            else()
                string(TOUPPER ${SUFFIX} SUFFIX)
                set(SUFFIX "_${SUFFIX}")
            endif()
            set(FLAG_VAR "CMAKE_${LANG}_FLAGS${SUFFIX}")
            set(${FLAG_VAR} "${${FLAG_VAR}} ${FLAGS}" PARENT_SCOPE)
            message(STATUS ${FLAG_VAR} ":" ${${FLAG_VAR}})
        endforeach()
    endforeach()
endmacro(ms_add_compiler_flags LANGUAGES SUFFIXES FLAGS)

macro(ms_link_static_runtime FLAG_VAR)
    if(MSVC)
        if(${FLAG_VAR} MATCHES "/MD")
            string(REPLACE "/MD"  "/MT" "${FLAG_VAR}" "${${FLAG_VAR}}")
            #Save persistently
            set(${FLAG_VAR} ${${FLAG_VAR}} CACHE STRING "" FORCE)
        endif()
    endif()
endmacro(ms_link_static_runtime)

macro(ms_replace_compiler_flags REPLACE_OPTION)
    set(SUFFIXES "")
    if((NOT DEFINED CMAKE_CONFIGURATION_TYPES) OR (CMAKE_CONFIGURATION_TYPES STREQUAL ""))
        #set(SUFFIXES "_DEBUG" "_RELEASE" "_MINSIZEREL" "_RELWITHDEBINFO")
        if((DEFINED CMAKE_BUILD_TYPE) AND (NOT (CMAKE_BUILD_TYPE STREQUAL "")))
            string(TOUPPER ${CMAKE_BUILD_TYPE} SUFFIXES)
            set(SUFFIXES "_${SUFFIXES}")
        endif()
    else()
        foreach(SUFFIX ${CMAKE_CONFIGURATION_TYPES})
            string(TOUPPER ${SUFFIX} SUFFIX)
            set(SUFFIXES ${SUFFIXES} "_${SUFFIX}")
        endforeach()
    endif()

    foreach(SUFFIX "" ${SUFFIXES})
        foreach(LANG C CXX)
            set(FLAG_VAR "CMAKE_${LANG}_FLAGS${SUFFIX}")
            if(${REPLACE_OPTION} STREQUAL "STATIC_LINK")
                ms_link_static_runtime(${FLAG_VAR})
            endif()
        endforeach()
        #message(STATUS ${FLAG_VAR} ":" ${${FLAG_VAR}})
    endforeach()
endmacro(ms_replace_compiler_flags REPLACE_OPTION)

function(ms_check_cxx11_support)
    if(UNIX)
        include(CheckCXXCompilerFlag)        
        CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
    else()
        if(MSVC_VERSION LESS 1700)
            set(COMPILER_SUPPORTS_CXX11 0)
        else()
            set(COMPILER_SUPPORTS_CXX11 1)
        endif()
    endif()

    if(COMPILER_SUPPORTS_CXX11)
    else()
        message(FATAL_ERROR "You need a compiler with C++11 support.")
    endif()
endfunction(ms_check_cxx11_support)


function(dsn_add_library PROJ_NAME)
    if((NOT DEFINED DSN_RECURSIVE_SRC) OR (NOT DSN_RECURSIVE_SRC))
        set(MY_GLOB_OPTION "GLOB")
    else()
        set(MY_GLOB_OPTION "GLOB_RECURSE")
    endif()

    file(${MY_GLOB_OPTION}
        PROJ_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
        )
    set(PROJ_SRC ${PROJ_SRC} ${DSN_EXTRA_SRC})
    if (PROJ_SRC STREQUAL "")
        message (FATAL "input sources cannot be empty")
    endif()
    ms_add_library("STATIC" ${PROJ_NAME} "${PROJ_SRC}" 1)
endfunction(dsn_add_library)

function(dsn_add_executable PROJ_NAME BINPLACE_FILES)
    if((NOT DEFINED DSN_RECURSIVE_SRC) OR (NOT DSN_RECURSIVE_SRC))
        set(MY_GLOB_OPTION "GLOB")
    else()
        set(MY_GLOB_OPTION "GLOB_RECURSE")
    endif()

    file(${MY_GLOB_OPTION}
        PROJ_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
        )
    set(PROJ_SRC ${PROJ_SRC} ${DSN_EXTRA_SRC})
    set(INPUT_LIBS ${DSN_EXTRA_LIBS} ${DSN_LIBS})
    if (PROJ_SRC STREQUAL "")
        message (FATAL "input sources cannot be empty")
    endif()
    ms_add_executable(${PROJ_NAME} "${PROJ_SRC}" "${INPUT_LIBS}" "${BINPLACE_FILES}" 0)
endfunction(dsn_add_executable PROJ_NAME BINPLACE_FILES)

function(dsn_setup_compiler_flags)
    if(UNIX)
        set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")
    endif()
    ms_replace_compiler_flags("STATIC_LINK")

    if(UNIX)
        add_compile_options(-std=c++11)
        if(DEFINED DSN_PEDANTIC)
            add_compile_options(-Werror)
        endif()
    elseif(MSVC)
        add_definitions(-D_CRT_SECURE_NO_WARNINGS)
        add_definitions(-DWIN32_LEAN_AND_MEAN)        
        add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
        add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS=1)
        add_definitions(-D_WIN32_WINNT=0x0501)
        add_definitions(-D_UNICODE)
        add_definitions(-DUNICODE)
        if(DEFINED DSN_PEDANTIC)
            add_compile_options(-WX)
        endif()
    endif()
endfunction(dsn_setup_compiler_flags)

function(ms_setup_boost STATIC_LINK PACKAGES)
    set(Boost_USE_MULTITHREADED            ON)
    if(STATIC_LINK)
        set(Boost_USE_STATIC_LIBS        ON)
        set(Boost_USE_STATIC_RUNTIME    ON)
    else()
        set(Boost_USE_STATIC_LIBS        OFF)
        set(Boost_USE_STATIC_RUNTIME    OFF)
    endif()

    find_package(Boost COMPONENTS ${PACKAGES} REQUIRED)

    set(BOOST_REQUIRED_LIBS "")
    foreach(PACKAGE ${PACKAGES})
        string(TOUPPER ${PACKAGE} PACKAGE)
        set(BOOST_REQUIRED_LIBS ${BOOST_REQUIRED_LIBS} ${Boost_${PACKAGE}_LIBRARY})
    endforeach()
    set(BOOST_REQUIRED_LIBS ${BOOST_REQUIRED_LIBS} CACHE STRING "Required boost packages" FORCE)

endfunction(ms_setup_boost STATIC_LINK PACKAGES)

function(dsn_setup_packages)
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
        dsn.tools.simulator
        dsn.tools.common
        dsn.dev
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

    if(DSN_BUILD_RUNTIME)
        set(DSN_LIBS ${DSN_CORE_TARGETS})
    else()
        ######Useless#######
        if(MSVC)
            set(RLEXT ".lib")
        else()
            set(RLEXT ".a")
        endif()

        foreach(RL ${DSN_EXTRA_TARGETS})
            #set(DSN_LIBS ${DSN_LIBS} "lib${RL}${RLEXT}")
        endforeach()

        foreach(RL ${DSN_CORE_TARGETS})
            #set(DSN_LIBS ${DSN_LIBS} "lib${RL}${RLEXT}")
        endforeach()
        ####################

        set(DSN_LIBS ${DSN_CORE_TARGETS})
    endif()

    set(DSN_LIBS ${DSN_LIBS} ${DSN_SYSTEM_LIBS} CACHE STRING "rDSN libs" FORCE)
endfunction(dsn_setup_packages)

function(dsn_set_output_path)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin CACHE STRING "" FORCE)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib CACHE STRING "" FORCE)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY} CACHE STRING "" FORCE)
endfunction(dsn_set_output_path)

function(dsn_setup_version)
    set(DSN_VERSION_MAJOR 1 CACHE STRING "rDSN major version" FORCE)
    set(DSN_VERSION_MINOR 0 CACHE STRING "rDSN minor version" FORCE)
    set(DSN_VERSION_PATCH 0 CACHE STRING "rDSN patch version" FORCE)
endfunction(dsn_setup_version)

function(dsn_setup_include_path)
    include_directories(${BOOST_INCLUDEDIR})
    if(DSN_BUILD_RUNTIME)
        include_directories(${CMAKE_SOURCE_DIR}/include)
    else()
        include_directories(${DSN_ROOT}/include)
    endif()
endfunction(dsn_setup_include_path)

function(dsn_setup_link_path)
    link_directories(${BOOST_LIBRARYDIR} ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
    if(DSN_BUILD_RUNTIME)
    else()
        link_directories(${DSN_ROOT}/lib)
    endif()
endfunction(dsn_setup_link_path)

function(dsn_setup_install)
    if(DSN_BUILD_RUNTIME)
        install(DIRECTORY include/ DESTINATION include)
        install(DIRECTORY bin/ DESTINATION bin)
        if(MSVC)
            install(FILES "bin/dsn.cg.bat" DESTINATION bin)
        else()
            install(PROGRAMS "bin/dsn.cg.sh" DESTINATION bin)
            install(PROGRAMS "bin/Linux/thrift" DESTINATION bin/Linux)
            install(PROGRAMS "bin/Linux/protoc" DESTINATION bin/Linux)
            install(PROGRAMS "bin/Darwin/thrift" DESTINATION bin/Darwin)
            install(PROGRAMS "bin/Darwin/protoc" DESTINATION bin/Darwin)
            install(PROGRAMS "bin/FreeBSD/thrift" DESTINATION bin/FreeBSD)
            install(PROGRAMS "bin/FreeBSD/protoc" DESTINATION bin/FreeBSD)
        endif()
    endif()
endfunction(dsn_setup_install)

function(dsn_add_pseudo_projects)
    if(DSN_BUILD_RUNTIME AND MSVC_IDE)
        file(GLOB_RECURSE
            PROJ_SRC
            "${CMAKE_SOURCE_DIR}/include/*.h"
            "${CMAKE_SOURCE_DIR}/include/*.hpp"
            )
        add_custom_target("dsn.include" SOURCES ${PROJ_SRC})
    endif()
endfunction(dsn_add_pseudo_projects)

function(dsn_common_setup)
    if(NOT (UNIX OR WIN32))
        message(FATAL_ERROR "Only Unix and Windows are supported.")
    endif()

    if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
        message(FATAL_ERROR "In-source builds are not allowed.")
    endif()

    if(NOT DEFINED DSN_BUILD_RUNTIME)
        message(FATAL_ERROR "DSN_BUILD_RUNTIME is not defined.")
    endif()
    
    set(BUILD_SHARED_LIBS OFF)
    dsn_setup_version()
    ms_check_cxx11_support()
    dsn_setup_compiler_flags()
    dsn_setup_packages()
    dsn_setup_include_path()
    dsn_set_output_path()
    dsn_setup_link_path()
    dsn_setup_install()
endfunction(dsn_common_setup)

