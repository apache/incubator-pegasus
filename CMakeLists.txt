cmake_minimum_required(VERSION 2.8.8)


function(dsn_add_library PROJ_NAME)
	if(PROJ_NAME STREQUAL "")
		message(FATAL_ERROR "Invalid project name")
	endif()

	if(MSVC)
		add_definitions(-D_LIB)
	endif()

	file(GLOB PROJ_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
	add_library(${PROJ_NAME} STATIC ${PROJ_SRC})
	install(TARGETS ${PROJ_NAME} DESTINATION lib)
endfunction(dsn_add_library)

function(dsn_add_executable PROJ_NAME INPUT_LIBS BINPLACE_FILES)
	if(PROJ_NAME STREQUAL "")
		message(FATAL_ERROR "Invalid project name")
	endif()

	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${PROJ_NAME}")
	set(INSTALL_BINPLACE_DIR "bin/${PROJ_NAME}")

	file(GLOB PROJ_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
	add_executable(${PROJ_NAME} ${PROJ_SRC})
	target_link_libraries(${PROJ_NAME} LINK_PUBLIC ${INPUT_LIBS})
	
	set(BINPLACE_DIR "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/")
	foreach(BF ${BINPLACE_FILES})
		add_custom_command(
			TARGET ${PROJ_NAME}
			POST_BUILD
			COMMAND ${CMAKE_COMMAND} -E copy ${BF} "${BINPLACE_DIR}"
			)
		#install(FILES ${BF} DESTINATION "${INSTALL_BINPLACE_DIR}")
	endforeach()

	#install(TARGETS ${PROJ_NAME} DESTINATION "${INSTALL_BINPLACE_DIR}")
endfunction(dsn_add_executable)

macro(dsn_check_cxx11_support)
	if(UNIX)
		include(CheckCXXCompilerFlag)
		CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
	else()
		if(MSVC_VERSION LESS 1800)
			set(COMPILER_SUPPORTS_CXX11 0)
		else()
			set(COMPILER_SUPPORTS_CXX11 1)
		endif()
	endif()

	if(NOT COMPILER_SUPPORTS_CXX11)
		message(FATAL_ERROR "You need a compiler with C++11 support.")
	endif()
endmacro(dsn_check_cxx11_support)

macro(dsn_link_static_runtime FLAG_VAR)
	if(MSVC)
		if(${FLAG_VAR} MATCHES "/MD")
			string(REGEX REPLACE "/MD"  "/MT" "${FLAG_VAR}" "${${FLAG_VAR}}")
		endif(${FLAG_VAR} MATCHES "/MD")
	endif()
	#message(STATUS ${FLAG_VAR} ":" ${${FLAG_VAR}})
endmacro(dsn_link_static_runtime)

macro(dsn_set_compiler_flags)
	foreach(LANG C CXX)
		foreach(SUF ${CMAKE_CONFIGURATION_TYPES})
			string(TOUPPER ${SUF} SUFFIX)
			set(FLAG_VAR "CMAKE_${LANG}_FLAGS_${SUFFIX}")
			dsn_link_static_runtime(${FLAG_VAR})
		endforeach()
		foreach(SUFFIX "" "_DEBUG" "_RELEASE" "_MINSIZEREL" "_RELWITHDEBINFO")
			set(FLAG_VAR "CMAKE_${LANG}_FLAGS${SUFFIX}")
			dsn_link_static_runtime(${FLAG_VAR})
		endforeach()
	endforeach()

	if(UNIX)
		set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -std=c++11")
	elseif(MSVC)
		add_definitions(-D_CONSOLE)
		add_definitions(-D_CRT_SECURE_NO_WARNINGS)
		add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
		add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS=1)
		set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -WX")
	endif()
endmacro(dsn_set_compiler_flags)

macro(dsn_check_package)
	set(Boost_USE_STATIC_LIBS		ON)
	set(Boost_USE_MULTITHREADED		ON)
	set(Boost_USE_STATIC_RUNTIME	ON)
	find_package(Boost COMPONENTS thread regex system filesystem chrono date_time REQUIRED)
	find_package(Threads REQUIRED)

	if(UNIX AND (NOT APPLE))
		set(DSN_SYSTEM_LIBS ${DSN_SYSTEM_LIBS} rt)
	endif()
	
	set(DSN_SYSTEM_LIBS
		${DSN_SYSTEM_LIBS}
		${CMAKE_THREAD_LIBS_INIT}
		${Boost_THREAD_LIBRARY}
		${Boost_SYSTEM_LIBRARY}
		${Boost_FILESYSTEM_LIBRARY}
		${Boost_REGEX_LIBRARY}
		${Boost_CHRONO_LIBRARY}
		${Boost_DATE_TIME_LIBRARY}
	)
endmacro(dsn_check_package)

macro(dsn_set_output_path)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY})
endmacro(dsn_set_output_path)


project(dsn C CXX)

set(DSN_VERSION_MAJOR 0)
set(DSN_VERSION_MINOR 0)
set(DSN_VERSION_PATCH 0)

if(NOT (UNIX OR WIN32))
	message(FATAL_ERROR "Only Unix and Windows are supported.")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
	message(FATAL_ERROR "In-source builds are not allowed.")
endif()

dsn_check_cxx11_support()
dsn_check_package()
dsn_set_compiler_flags()
dsn_set_output_path()

set(DSN_COMMON_LIBS
	dsn.tools.simulator
	dsn.tools.common
	dsn.dev
	dsn.core
	)
set(DSN_LIBS ${DSN_COMMON_LIBS} ${DSN_SYSTEM_LIBS})

set(BUILD_SHARED_LIBS OFF)
include_directories(${BOOST_INCLUDEDIR} ${CMAKE_SOURCE_DIR}/inc)
link_directories(${BOOST_LIBRARYDIR} ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
install(DIRECTORY inc/dsn DESTINATION include)
install(FILES "codegen/dsn.generate_code.php" DESTINATION bin)
install(FILES "codegen/dsn.cg.bat" DESTINATION bin)
install(FILES "codegen/dsn.cg.sh" DESTINATION bin)
install(DIRECTORY codegen/dsn.templates DESTINATION bin)

add_subdirectory(src)
