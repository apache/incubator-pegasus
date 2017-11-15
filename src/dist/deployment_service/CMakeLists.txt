set(MY_PROJ_NAME "dsn.deploy.service")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH ${RAPIDJSON_INCLUDE_DIR})

set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIBS 
    "dsn.scheduler.providers")

set(MY_PROJ_LIB_PATH "")

file(GLOB
    RES_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/config.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile"
    "${CMAKE_CURRENT_SOURCE_DIR}/*_nodes"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cmd"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../scripts/windows/echoc.exe"
)

# Extra files that will be installed
set(MY_BINPLACES ${RES_FILES})

dsn_add_executable()

add_custom_target( docker 
    COMMAND docker build -t "${MY_PROJ_NAME}-image" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${MY_PROJ_NAME}")
add_dependencies( docker "${MY_PROJ_NAME}")
