set(MY_PROJ_NAME dsn.rep_tests.simple_kv)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH ../../apps/replication/client_lib)

set(MY_BOOST_PACKAGES system)

set(MY_PROJ_LIBS dsn.replication dsn.replication.meta_server dsn.replication.clientlib dsn.failure_detector)

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
FILE(GLOB_RECURSE CASE_FILES "case-*")
set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/addcase.sh"
    "${CASE_FILES}"
)

dsn_add_executable()
