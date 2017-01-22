if(APPLE)
  # SSL seems to link fine even when compiled using the default compiler
  # The alternative to get openssl to use gcc on mac requires a patch to
  # the ./Configure script
ExternalProject_Add(libssl
  PREFIX ${GraphLab_SOURCE_DIR}/deps/ssl
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/openssl-1.0.2.tar.gz
  URL_MD5 38373013fc85c790aabf8837969c5eba
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ./Configure darwin64-x86_64-cc --prefix=<INSTALL_DIR>
  BUILD_COMMAND make -j1
  INSTALL_COMMAND make -j1 install && cp ./libcrypto.a <INSTALL_DIR>/ssl && cp ./libssl.a <INSTALL_DIR>/ssl
  )
else()
ExternalProject_Add(libssl
  PREFIX ${GraphLab_SOURCE_DIR}/deps/ssl
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/openssl-1.0.2.tar.gz
  URL_MD5 38373013fc85c790aabf8837969c5eba
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} ./config --prefix=<INSTALL_DIR>
  BUILD_COMMAND make -j1
  INSTALL_COMMAND make -j1 install_sw
  )
endif()

macro(requires_openssl NAME)
  add_dependencies(${NAME} libssl)
  target_link_libraries(${NAME} ${CMAKE_SOURCE_DIR}/deps/local/lib/libssl.a ${CMAKE_SOURCE_DIR}/deps/local/lib/libcrypto.a dl)
endmacro()
