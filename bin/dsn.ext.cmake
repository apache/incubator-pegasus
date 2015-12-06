
#set(project_name googletest)
#set(target_url  https://github.com/imzhenyu/googletest.git)
#set(my_cmake_args "-Dgtest_force_shared_crt=OFF;")
#if(WIN32)
#	set(target_binaries gtest.lib gtest_main.lib)
#else()
#	set(target_binaries libgtest.a libgtest_main.a)
#endif()

include(ExternalProject)

string(TOUPPER ${project_name} PROJECT_NAME_U)

set(target_bin_dir ${PROJECT_BINARY_DIR}/${project_name}-lib)

set(install_cmd "")
foreach(file_i ${target_binaries})
	if(WIN32)
		set(cp "copy /Y")
	else()
		set(cp "cp")
	endif()
	
	if(install_cmd STREQUAL "")
		set(install_cmd "${cp} ${target_bin_dir}/${file_i} ${CMAKE_INSTALL_PREFIX}/lib")
	else()
		set(install_cmd "${install_cmd} && ${cp} ${target_bin_dir}/${file_i} ${CMAKE_INSTALL_PREFIX}/lib")
	endif()
endforeach()

message (INFO " install_cmd = ${install_cmd}")

if(WIN32)
	# TODO: get current build config so we get the right path of the target file
	set(install_cmd "")
endif()

ExternalProject_Add(${project_name}
	GIT_REPOSITORY ${target_url}
	GIT_TAG master
    CMAKE_ARGS "${CMAKE_ARGS};-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX};${my_cmake_args};"
	BINARY_DIR "${target_bin_dir}"
	INSTALL_DIR "${target_bin_dir}"
	INSTALL_COMMAND "${install_cmd}"
)

# Specify source dir
ExternalProject_Get_Property(${project_name} source_dir)
set(my_source_dir ${source_dir})

# Specify link libraries
ExternalProject_Get_Property(${project_name} binary_dir)
set(my_binary_dir ${binary_dir})
