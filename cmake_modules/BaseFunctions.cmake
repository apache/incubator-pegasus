# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# Helper function to add preprocesor definition of FILE_BASENAME
# to pass the filename without directory path for debugging use.
#
# Note that in header files this is not consistent with
# __FILE__ and __LINE__ since FILE_BASENAME will be the
# compilation unit source file name (.c/.cpp).
#
# Example:
#
#   define_file_basename_for_sources(my_target)
#
# Will add -DFILE_BASENAME="filename" for each source file depended on
# by my_target, where filename is the name of the file.
#
function(define_file_basename_for_sources targetname)
  get_target_property(source_files "${targetname}" SOURCES)
  foreach(sourcefile ${source_files})
    # Add the FILE_BASENAME=filename compile definition to the list.
    get_filename_component(basename "${sourcefile}" NAME)
    # Set the updated compile definitions on the source file.
    set_property(
            SOURCE "${sourcefile}" APPEND
            PROPERTY COMPILE_DEFINITIONS "__FILENAME__=\"${basename}\"")
  endforeach()
endfunction()

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
  define_file_basename_for_sources(${MY_PROJ_NAME})
  target_compile_features(${MY_PROJ_NAME} PRIVATE cxx_std_17)
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
    add_definitions(-DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0)
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

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -gdwarf-4" CACHE STRING "" FORCE)

  #  -Wall: Enable all warnings.
  add_compile_options(-Wall)
  add_compile_options(-Werror)
  #  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned integers
  add_compile_options(-Wno-sign-compare)
  add_compile_options(-Wno-strict-aliasing)
  add_compile_options(-Wuninitialized)
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
    ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "5.4.0")))
      message(SEND_ERROR "Cannot use sanitizer without clang or gcc >= 5.4.0")
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

  if (NOT APPLE)
    find_package(RT REQUIRED)
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${RT_LIBRARIES})
  endif()

  find_package(DL REQUIRED)
  set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${DL_LIBRARIES})

  # for md5 calculation
  find_package(OpenSSL REQUIRED)
  set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} ${OPENSSL_CRYPTO_LIBRARY})

  if (NOT APPLE)
    if(ENABLE_GPERF)
      set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} tcmalloc_and_profiler)
      add_definitions(-DDSN_ENABLE_GPERF)
    endif()
  endif()

  if(USE_JEMALLOC)
    find_package(Jemalloc REQUIRED)
    # also use cpu profiler provided by gperftools
    set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} JeMalloc::JeMalloc profiler)
    add_definitions(-DDSN_USE_JEMALLOC)
  endif()

  set(DSN_SYSTEM_LIBS
          ${DSN_SYSTEM_LIBS}
          ${CMAKE_THREAD_LIBS_INIT} # the thread library found by FindThreads
          CACHE STRING "rDSN system libs" FORCE
          )
endfunction(dsn_setup_system_libs)

function(dsn_setup_include_path)#TODO(huangwei5): remove this
  include_directories(${THIRDPARTY_INSTALL_DIR}/include)
endfunction(dsn_setup_include_path)

function(dsn_setup_thirdparty_libs)
  set(Boost_USE_MULTITHREADED ON)
  set(Boost_USE_STATIC_LIBS OFF)
  set(Boost_USE_STATIC_RUNTIME OFF)
  set(BOOST_ROOT ${THIRDPARTY_INSTALL_DIR})
  set(Boost_NO_SYSTEM_PATHS ON)
  set(Boost_NO_BOOST_CMAKE ON)

  set(CMAKE_PREFIX_PATH ${THIRDPARTY_INSTALL_DIR};${CMAKE_PREFIX_PATH})
  message(STATUS "CMAKE_PREFIX_PATH = ${CMAKE_PREFIX_PATH}")
  find_package(Boost COMPONENTS system filesystem REQUIRED)
  include_directories(${Boost_INCLUDE_DIRS})

  find_library(THRIFT_LIB NAMES libthrift.a PATHS ${THIRDPARTY_INSTALL_DIR}/lib NO_DEFAULT_PATH)
  if(NOT THRIFT_LIB)
    message(FATAL_ERROR "thrift library not found in ${THIRDPARTY_INSTALL_DIR}/lib")
  endif()
  find_package(fmt REQUIRED)
  set(DEFAULT_THIRDPARTY_LIBS ${THRIFT_LIB} fmt::fmt CACHE STRING "default thirdparty libs" FORCE)

  # rocksdb
  file(GLOB ROCKSDB_DEPENDS_MODULE_PATH ${THIRDPARTY_ROOT}/build/Source/rocksdb/cmake/modules)
  if(NOT ROCKSDB_DEPENDS_MODULE_PATH)
    message(WARNING "Cannot find RocksDB depends cmake modules path, might not find snappy, zstd, lz4")
  endif()
  list(APPEND CMAKE_MODULE_PATH "${ROCKSDB_DEPENDS_MODULE_PATH}")
  find_package(snappy)
  find_package(zstd)
  find_package(lz4)
  if(USE_JEMALLOC)
    find_package(Jemalloc REQUIRED)
  endif()
  find_package(RocksDB REQUIRED)

  # libhdfs
  find_package(JNI REQUIRED)
  message (STATUS "JAVA_JVM_LIBRARY=${JAVA_JVM_LIBRARY}")
  link_libraries(${JAVA_JVM_LIBRARY})

  find_package(OpenSSL REQUIRED)
  include_directories(${OPENSSL_INCLUDE_DIR})
  link_libraries(${OPENSSL_CRYPTO_LIBRARY})
  link_libraries(${OPENSSL_SSL_LIBRARY})

  # abseil
  find_package(absl REQUIRED)

  link_directories(${THIRDPARTY_INSTALL_DIR}/lib)
  if (NOT APPLE)
    link_directories(${THIRDPARTY_INSTALL_DIR}/lib64)
  endif()
endfunction(dsn_setup_thirdparty_libs)

function(dsn_common_setup)
  if(NOT (UNIX))
    message(FATAL_ERROR "Only Unix are supported.")
  endif()

  if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR "In-source builds are not allowed.")
  endif()

  find_program(CCACHE "ccache")
  if(CCACHE)
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
    message(STATUS "CCACHE: ${CCACHE}")

    set(ENV{CCACHE_COMPRESS} "true")
    set(ENV{CCACHE_COMPRESSLEVEL} "6")
    set(ENV{CCACHE_MAXSIZE} "1024M")
  endif(CCACHE)

  if(NOT DEFINED DSN_BUILD_RUNTIME)
    set(DSN_BUILD_RUNTIME FALSE)
  endif()

  set(BUILD_SHARED_LIBS OFF)

  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)

  dsn_setup_system_libs()
  dsn_setup_compiler_flags()
  dsn_setup_include_path()
  dsn_setup_thirdparty_libs()

  include(ThriftUtils)

endfunction(dsn_common_setup)
