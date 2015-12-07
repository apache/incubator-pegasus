
#set(project_name googletest)
#set(target_url  https://github.com/imzhenyu/googletest.git)
#set(my_cmake_args "-Dgtest_force_shared_crt=OFF;")
#if(WIN32)
#    set(target_binaries gtest.lib gtest_main.lib)
#else()
#    set(target_binaries libgtest.a libgtest_main.a)
#endif()

include(ExternalProject)

string(TOUPPER ${project_name} PROJECT_NAME_U)

set(target_bin_dir ${PROJECT_BINARY_DIR}/${project_name}-lib)

if(WIN32)
    #Set the Build configuration name.
    if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
      if(BUILD_TYPE)
        string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
               CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
      else()
        set(CMAKE_INSTALL_CONFIG_NAME "Release")
      endif()
      message(STATUS "Build configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
    endif()
endif()

set(install_cmd "")
foreach(file_i ${target_binaries})
    if(WIN32)
        set(cp "copy /Y ${target_bin_dir}/${CMAKE_INSTALL_CONFIG_NAME}/${file_i}")
    else()
        set(cp "cp ${target_bin_dir}/${file_i}")
    endif()
    
    if(install_cmd STREQUAL "")
        set(install_cmd "${cp} ${CMAKE_INSTALL_PREFIX}/lib")
    else()
        set(install_cmd "${install_cmd} && ${cp} ${CMAKE_INSTALL_PREFIX}/lib")
    endif()
endforeach()

message (INFO " install_cmd = ${install_cmd}")

ExternalProject_Add(${project_name}
    GIT_REPOSITORY ${target_url}
    GIT_TAG master
    CMAKE_ARGS "${CMAKE_ARGS};-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX};${my_cmake_args};"
    BINARY_DIR "${target_bin_dir}"
    INSTALL_DIR "${target_bin_dir}"
    #INSTALL_COMMAND "${install_cmd}"
    INSTALL_COMMAND "" #TODO: fix installation later to be cross-platform
)

# Specify source dir
ExternalProject_Get_Property(${project_name} source_dir)
set(my_source_dir ${source_dir})

# Specify link libraries
ExternalProject_Get_Property(${project_name} binary_dir)
set(my_binary_dir ${binary_dir})
