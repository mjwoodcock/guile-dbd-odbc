add_library (guile-dbd-odbc SHARED guile-dbd-odbc.c)
target_include_directories(guile-dbd-odbc PUBLIC ${GUILE_INCLUDE_DIRS})
target_link_libraries(guile-dbd-odbc ${ODBC})
install (TARGETS guile-dbd-odbc
	LIBRARY DESTINATION lib)
