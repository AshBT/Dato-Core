ExternalProject_Add(avrocpp
  PREFIX ${CMAKE_SOURCE_DIR}/deps/avro-cpp
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/avro-cpp-1.7.6.tar.gz
  URL_MD5 4dbcfc48abf8feab7ea9d7f8ec8766c9
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ""
  PATCH_COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/libavro.patch || true
  BUILD_COMMAND env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} BOOST_ROOT=${CMAKE_SOURCE_DIR}/deps/local <SOURCE_DIR>/build.sh clean && cd build && make avrocpp_s
  INSTALL_COMMAND chmod 755 <SOURCE_DIR>/api &&
                  cp -r <SOURCE_DIR>/api ${CMAKE_SOURCE_DIR}/deps/local/include/avro &&
		  cp <SOURCE_DIR>/build/libavrocpp_s.a ${CMAKE_SOURCE_DIR}/deps/local/lib
)

add_dependencies(avrocpp boost)

macro(requires_avrocpp NAME)
  add_dependencies(${NAME} avrocpp)
  target_link_libraries(${NAME} avrocpp_s)
endmacro(requires_avrocpp)
