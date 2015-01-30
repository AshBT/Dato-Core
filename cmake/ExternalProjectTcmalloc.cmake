# TCMalloc  ===================================================================
# We use tcmalloc for improved memory allocation performance
if(APPLE_CLANG)
  SET(TCMALLOC_COMPILE_FLAGS "-stdlib=libc++ -fPIC")
  SET(TCMALLOC_C_FLAGS "-fPIC")
  ExternalProject_Add(libtcmalloc
    PREFIX ${CMAKE_SOURCE_DIR}/deps/tcmalloc
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/gperftools-2.1.tar.gz
    URL_MD5 5e5a981caf9baa9b4afe90a82dcf9882
    PATCH_COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/tcmalloc.patch || true
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --prefix=<INSTALL_DIR> --enable-shared=no CFLAGS=${TCMALLOC_C_FLAGS} CXXFLAGS=${TCMALLOC_COMPILE_FLAGS} CC=clang CXX=clang++  LDFLAGS=-stdlib=libc++
    INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local)
  unset(TCMALLOC_COMPILE_FLAGS)
  unset(TCMALLOC_C_FLAGS)
else()
  ExternalProject_Add(libtcmalloc
    PREFIX ${CMAKE_SOURCE_DIR}/deps/tcmalloc
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/gperftools-2.1.tar.gz
    URL_MD5 5e5a981caf9baa9b4afe90a82dcf9882
    PATCH_COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/tcmalloc.patch || true
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --prefix=<INSTALL_DIR> --enable-shared=no CXXFLAGS=-fPIC CFLAGS=-fPIC CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} 
    INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local)
endif()

#link_libraries(tcmalloc)
set(TCMALLOC-FOUND 1)


macro(requires_tcmalloc NAME)
  add_dependencies(${NAME} libtcmalloc)
  target_link_libraries(${NAME} tcmalloc pthread)
  set_property(TARGET ${NAME} APPEND PROPERTY COMPILE_DEFINITIONS HAS_TCMALLOC) 
endmacro(requires_tcmalloc)
