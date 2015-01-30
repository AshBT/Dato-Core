ExternalProject_Add(libxml2
  PREFIX ${GraphLab_SOURCE_DIR}/deps/libxml2
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libxml2-2.9.1.tar.gz
  URL_MD5 9c0cfef285d5c4a5c80d00904ddab380
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --enable-shared=no --enable-static=yes --with-python=./ --without-lzma --libdir=<INSTALL_DIR>/lib
  )
# the with-python=./ prevents it from trying to build/install some python stuff
# which is poorly installed (always ways to stick it in a system directory)
include_directories(${GraphLab_SOURCE_DIR}/deps/local/include/libxml2)

macro(requires_libxml2 NAME)
  add_dependencies(${NAME} libxml2)
  target_link_libraries(${NAME} xml2 z)
endmacro()
