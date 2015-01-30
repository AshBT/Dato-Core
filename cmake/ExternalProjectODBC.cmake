ExternalProject_Add(odbc
    PREFIX ${GraphLab_SOURCE_DIR}/deps/odbc
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/unixODBC-2.3.2.tar.gz
    URL_MD5 5e4528851eda5d3d4aed249b669bd05b
    # If you want a more liberal license but less stability, use this instead
    #URL http://downloads.sourceforge.net/project/iodbc/iodbc/3.52.9/libiodbc-3.52.9.tar.gz
    #URL_MD5 98a681e06a1df809af9ff9a16951b8b6
    INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
    # DO NOT remove "--enable-shared".  If you do, we will be in violation of the LGPL 2.1 (if using unixODBC)!
    #CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --enable-shared=yes --enable-static=no --libdir=<INSTALL_DIR>/lib
    CONFIGURE_COMMAND <SOURCE_DIR>/configure 
    BUILD_IN_SOURCE 1
    BUILD_COMMAND ""
    INSTALL_COMMAND cp <SOURCE_DIR>/include/autotest.h <SOURCE_DIR>/include/odbcinstext.h <SOURCE_DIR>/include/odbcinst.h <SOURCE_DIR>/include/sqlext.h <SOURCE_DIR>/include/sql.h <SOURCE_DIR>/include/sqltypes.h <SOURCE_DIR>/include/sqlucode.h <SOURCE_DIR>/include/uodbc_extras.h <SOURCE_DIR>/include/uodbc_stats.h <SOURCE_DIR>/unixodbc_conf.h <INSTALL_DIR>/include
    )

macro(requires_odbc NAME)
  add_dependencies(${NAME} odbc)
  #target_link_libraries(${NAME} odbc)
endmacro()
