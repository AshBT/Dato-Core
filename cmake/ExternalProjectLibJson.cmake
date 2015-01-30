if (APPLE_CLANG)
  SET(TMP "-c -O3 -DNDEBUG -stdlib=libc++ -fPIC")
  SET(TMP2 "")
  SET (JSON_COMPILER_OPTIONS CXX=clang++ EXTRA_FLAGS=${TMP})
  UNSET(TMP2)
  UNSET(TMP)
else()
  SET(TMP2 "")
  SET(JSON_COMPILER_OPTIONS EXTRA_FLAGS=-fPIC)
  UNSET(TMP2)
endif()

# libjson ====================================================================
# Lib Json is used to support json serialization for long term storage of
# graph data.
ExternalProject_Add(libjson
  PREFIX ${CMAKE_SOURCE_DIR}/deps/json
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/libjson_7.6.0.zip
  URL_MD5 dcb326038bd9b710b8f717580c647833
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ""
  PATCH_COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/libjson.patch || true
  BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} make ${JSON_COMPILER_OPTIONS}
  INSTALL_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} make install ${JSON_COMPILER_OPTIONS} prefix=<INSTALL_DIR>/ 
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  )



 macro(requires_libjson NAME)
  add_dependencies(${NAME} libjson)
  target_link_libraries(${NAME} json)
 endmacro(requires_libjson)
