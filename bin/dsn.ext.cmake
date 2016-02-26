
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

set(target_bin_dir ${PROJECT_BINARY_DIR}/${project_name})
if (DEFINED target_bin_subdir)
    set(target_bin_subdir /${target_bin_subdir})
else ()
    set(target_bin_subdir "")
endif ()
set(install_cmd "")

if(WIN32)
    set (install_cmd CALL ${PROJECT_SOURCE_DIR}/bin/dsn.ext.copy.cmd ${target_bin_dir}${target_bin_subdir} ${PROJECT_BINARY_DIR}/lib)
    set (install_cmd cmd /c ${install_cmd})
else()
    foreach(file_i ${target_binaries})
        if(install_cmd STREQUAL "")
            set(install_cmd ${CMAKE_COMMAND} -E copy "${target_bin_dir}${binary_subdir}/${file_i}" "${PROJECT_BINARY_DIR}/lib")
        else()
            set(install_cmd ${install_cmd} COMMAND ${CMAKE_COMMAND} -E copy "${target_bin_dir}${binary_subdir}/${file_i}" "${PROJECT_BINARY_DIR}/lib")
        endif()
    endforeach()
endif()

#message (INFO " install_cmd = ${install_cmd}")

ExternalProject_Add(${project_name}
    GIT_REPOSITORY ${target_url}
    GIT_TAG master
    CMAKE_ARGS "${CMAKE_ARGS};-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX};${my_cmake_args};"
    BINARY_DIR "${target_bin_dir}"
    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND ${install_cmd}
)

# Specify source dir
ExternalProject_Get_Property(${project_name} source_dir)
set(my_source_dir ${source_dir})

# Specify link libraries
ExternalProject_Get_Property(${project_name} binary_dir)
set(my_binary_dir ${binary_dir})
