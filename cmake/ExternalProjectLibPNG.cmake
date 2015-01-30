ExternalProject_Add(libpng
  PREFIX ${CMAKE_SOURCE_DIR}/deps/libjpeg
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libpng-1.6.14.tar.gz
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
  INSTALL_COMMAND make install && bash -c "rm -f <INSTALL_DIR>/lib/libpng*.so*" && bash -c "rm -f <INSTALL_DIR>/lib/libpng*.dylib*"
  BUILD_IN_SOURCE 1)

macro(requires_libpng NAME)
  add_dependencies(${NAME} libpng)
  target_link_libraries(${NAME} png)
endmacro(requires_libpng)
