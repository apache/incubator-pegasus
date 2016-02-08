set(MY_PROJ_NAME dsn.scheduler.docker.test)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
ms_find_source_files("CXX" "${CMAKE_CURRENT_SOURCE_DIR}/../scheduler_providers" "GLOB" MY_PROJ_SRC)
# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../scheduler_providers")

set(MY_BOOST_PACKAGES system)

set(MY_PROJ_LIBS "dsn.scheduler.providers")

set(MY_PROJ_LIB_PATH "")

set(INI_FILES "")
file(GLOB
    INI_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    )

# Extra files that will be installed
set(MY_BINPLACES "${INI_FILES}"
                 "${CMAKE_CURRENT_SOURCE_DIR}/run_docker.sh")

dsn_add_executable()
