ExternalProject_Add(libjpeg
  PREFIX ${CMAKE_SOURCE_DIR}/deps/libjpeg
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/jpegsrc.v8d.tar.gz
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
  INSTALL_COMMAND make install && bash -c "rm -f <INSTALL_DIR>/lib/libjpeg*.so*" && bash -c "rm -f <INSTALL_DIR>/lib/libjpeg*.dylib*"
  BUILD_IN_SOURCE 1)

macro(requires_libjpeg NAME)
  add_dependencies(${NAME} libjpeg)
  target_link_libraries(${NAME} jpeg)
endmacro(requires_libjpeg)
