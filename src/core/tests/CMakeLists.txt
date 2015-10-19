set(MY_PROJ_NAME dsn.core.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
	${GTEST_INCLUDE_DIRS} 
	../core ../tools/common ../tools/simulator ../tools/hpc ../tools/nfs 
	../../dev/cpp
	)
	
set(MY_BOOST_PACKAGES system)

set(MY_PROJ_LIBS gtest
                 dsn.tools.nfs
                 dsn.dev.cpp.core.use
                 dsn.tools.hpc
				 dsn.tools.simulator
                 dsn.tools.common
                 dsn.corelib
  )

if(UNIX AND (NOT APPLE))
	set(MY_PROJ_LIBS ${MY_PROJ_LIBS} rt)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
	set(MY_PROJ_LIBS ${MY_PROJ_LIBS} dl aio)
endif()

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config-bad-section.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-dup-key.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-dup-section.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-empty.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-no-key.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-no-section.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-null-section.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-sample.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-test.ini"
                 "${CMAKE_CURRENT_SOURCE_DIR}/config-unmatch-section.ini"
)

dsn_add_executable()


