set(MY_PROJ_NAME dsn.block_service.fds)

#Source files under CURRENT project directory will be automatically included.
#You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

#Search mode for source files under CURRENT project directory ?
#"GLOB_RECURSE" for recursive search
#"GLOB" for non - recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

#Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()

find_path(FDS_INCLUDE_DIR NAMES constants.h PATHS ${DSN_THIRDPARTY_ROOT}/include PATH_SUFFIXES fds) # TODO(huangwei5): move to dsn_setup_thirdparty_libs(), if not found, return fatal
target_include_directories(dsn.block_service.fds PRIVATE "${FDS_INCLUDE_DIR}")
