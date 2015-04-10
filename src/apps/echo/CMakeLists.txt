set(INPUT_LIBS dsn.failure_detector ${DSN_LIBS})
set(BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/echo.ini")
dsn_add_executable(echo "${INPUT_LIBS}" "${BINPLACE_FILES}")
