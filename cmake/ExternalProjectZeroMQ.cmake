if (APPLE_CLANG)
SET(ZEROMQ_C_FLAGS "-fPIC -DHAVE_LIBSODIUM")
SET(ZEROMQ_CPP_FLAGS "-stdlib=libc++ -fPIC -DHAVE_LIBSODIUM")
set(ZEROMQ_LD_FLAGS "-stdlib=libc++ -L${CMAKE_SOURCE_DIR}/deps/local/lib -lsodium -lm -lsodium")
ExternalProject_Add(zeromq
  PREFIX ${CMAKE_SOURCE_DIR}/deps/zeromq
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/zeromq-4.0.3.tar.gz
  URL_MD5 8348341a0ea577ff311630da0d624d45
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  PATCH_COMMAND patch -N -p0 -i ${CMAKE_SOURCE_DIR}/patches/zeroMq.patch || true
  CONFIGURE_COMMAND <SOURCE_DIR>/autogen.sh && <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --enable-static=yes --enable-shared=yes CC=clang CXX=clang++ CFLAGS=${ZEROMQ_C_FLAGS} CXXFLAGS=${ZEROMQ_CPP_FLAGS} LDFLAGS=${ZEROMQ_LD_FLAGS} LIBS=-lm --with-libsodium=<INSTALL_DIR> --with-pic
  BUILD_IN_SOURCE 1
  BUILD_COMMAND make -j4
  INSTALL_COMMAND make install && ${GraphLab_SOURCE_DIR}/patches/zeromq_del_so.sh <INSTALL_DIR>/lib
  )
else()
SET(ZEROMQ_C_FLAGS "-fPIC -DHAVE_LIBSODIUM")
SET(ZEROMQ_CPP_FLAGS "-fPIC -DHAVE_LIBSODIUM")
set(ZEROMQ_LD_FLAGS "-L${CMAKE_SOURCE_DIR}/deps/local/lib -lsodium")
ExternalProject_Add(zeromq
  PREFIX ${CMAKE_SOURCE_DIR}/deps/zeromq
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/zeromq-4.0.3.tar.gz
  URL_MD5 8348341a0ea577ff311630da0d624d45
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  PATCH_COMMAND patch -N -p0 -i ${CMAKE_SOURCE_DIR}/patches/zeroMq.patch || true
  CONFIGURE_COMMAND <SOURCE_DIR>/autogen.sh && <SOURCE_DIR>/configure CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=${ZEROMQ_C_FLAGS} CXXFLAGS=${ZEROMQ_CPP_FLAGS} LDFLAGS=${ZEROMQ_LD_FLAGS} LIBS=-lm --prefix=<INSTALL_DIR> --enable-static=yes --enable-shared=yes --with-libsodium=<INSTALL_DIR> --with-libsodium-include-dir=<INSTALL_DIR>/include --with-libsodium-lib-dir=<INSTALL_DIR>/lib --with-pic --libdir=<INSTALL_DIR>/lib
  BUILD_IN_SOURCE 1
  BUILD_COMMAND make -j4
  INSTALL_COMMAND make install && ${GraphLab_SOURCE_DIR}/patches/zeromq_del_so.sh <INSTALL_DIR>/lib
  )
endif()

add_dependencies(zeromq libsodium)

macro(requires_zeromq NAME)
  target_link_libraries(${NAME} zmq)
  add_dependencies(${NAME} zeromq)
  requires_libsodium(${NAME})
  set_property(TARGET ${NAME} APPEND PROPERTY COMPILE_DEFINITIONS HAVE_LIBSODIUM)
endmacro(requires_zeromq)


