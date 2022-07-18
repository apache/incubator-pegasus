set(MY_PROJ_NAME dsn_http_test)

set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
    dsn_http
    dsn_runtime
    gtest
    gtest_main
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
)

dsn_add_test()
