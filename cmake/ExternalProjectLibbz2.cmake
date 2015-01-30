
# libbz  =================================================================
ExternalProject_Add(libbz2
  PREFIX ${CMAKE_SOURCE_DIR}/deps/libbz2
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/bzip2-1.0.6.tar.gz
  URL_MD5 00b516f4704d4a7cb50a1d97e6e8e15b
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND ""
  PATCH_COMMAND patch -N -p0 -i ${CMAKE_SOURCE_DIR}/patches/libbz2_fpic.patch || true
  BUILD_IN_SOURCE 1
  BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} make install PREFIX=<INSTALL_DIR>
  INSTALL_COMMAND "" )



macro(requires_libbz2 NAME)
  add_dependencies(${NAME} libbz2)
  target_link_libraries(${NAME} bz2)
endmacro(requires_libbz2)

