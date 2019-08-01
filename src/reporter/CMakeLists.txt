set(MY_PROJ_NAME "pegasus_reporter")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")
include_directories(../../rdsn/thirdparty/output)
link_directories(../../rdsn/thirdparty/output/lib)
link_libraries(
	"libprometheus-cpp-core.so"
	"libprometheus-cpp-push.so"
	"libprometheus-cpp-pull.so")


set(MY_PROJ_LIBS 
	prometheus-cpp-core
	prometheus-cpp-pull
	prometheus-cpp-push
	curl
	${MY_PROJ_LIBS}
	)

set(MY_BINPLACES "config.ini")

dsn_add_static_library()
