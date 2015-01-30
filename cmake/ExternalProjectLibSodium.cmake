set(SODIUM_CC "${CMAKE_C_COMPILER} -fPIC")

ExternalProject_Add(libsodium
  PREFIX ${CMAKE_SOURCE_DIR}/deps/libsodium
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libsodium-0.7.1.tar.gz
  URL_MD5 c224fe3923d1dcfe418c65c8a7246316
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND <SOURCE_DIR>/configure CFLAGS=-fPIC CPPFLAGS=-fPIC --enable-pie --with-pic --prefix=<INSTALL_DIR> --enable-static=yes --enable-shared=no --libdir=<INSTALL_DIR>/lib
  PATCH_COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/libsodium.patch || true
  BUILD_IN_SOURCE 1
  BUILD_COMMAND make 
  INSTALL_COMMAND make install )

macro(requires_libsodium NAME)
  target_link_libraries(${NAME} sodium)
  add_dependencies(${NAME} libsodium)
endmacro()
