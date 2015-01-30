if (APPLE)
  ExternalProject_Add(libevent
    PREFIX ${GraphLab_SOURCE_DIR}/deps/event
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libevent-2.0.18-stable.tar.gz
    URL_MD5 aa1ce9bc0dee7b8084f6855765f2c86a
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --disable-openssl --enable-shared=no
    INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
  )
else()
  ExternalProject_Add(libevent
    PREFIX ${GraphLab_SOURCE_DIR}/deps/event
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libevent-2.0.18-stable.tar.gz
    URL_MD5 aa1ce9bc0dee7b8084f6855765f2c86a
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --disable-openssl --enable-shared=no 
    INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
    INSTALL_COMMAND prefix=<INSTALL_DIR>/ make install && ${GraphLab_SOURCE_DIR}/patches/libevent_clean_and_remap.sh <INSTALL_DIR>/lib
  )
endif()
 macro(requires_libevent NAME)
  add_dependencies(${NAME} libevent)
  target_link_libraries(${NAME} event event_pthreads)
 endmacro(requires_libevent)

