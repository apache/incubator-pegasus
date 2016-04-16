#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://github.com/gflags/gflags.git")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/gflags.git")
else()
    message(FATAL_ERROR "not support other git source")
endif()

if (WIN32)
	ExternalProject_Add(gflags
		GIT_REPOSITORY ${target_url}
		GIT_TAG master
	   #   CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
	   #            -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
	   #            -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
		BINARY_DIR "${PROJECT_BINARY_DIR}/lib"
		# disable install
		INSTALL_COMMAND ""
		)
else()
	ExternalProject_Add(gflags
		GIT_REPOSITORY ${target_url}
		GIT_TAG master
	   #   CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
	   #            -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
	   #            -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
		BINARY_DIR "${PROJECT_BINARY_DIR}/gflags"

		INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
		INSTALL_COMMAND "${CMAKE_COMMAND}" -E copy "${PROJECT_BINARY_DIR}/gflags/lib/libgflags.a" "${PROJECT_BINARY_DIR}/lib" COMMAND "${CMAKE_COMMAND}" -E copy "${PROJECT_BINARY_DIR}/gflags/lib/libgflags_nothreads.a" "${PROJECT_BINARY_DIR}/lib"
		)	
endif()

# Specify include dir
ExternalProject_Get_Property(gflags source_dir)
set(GFLAGS_INCLUDE_DIRS ${PROJECT_BINARY_DIR}/gflags/include PARENT_SCOPE)

