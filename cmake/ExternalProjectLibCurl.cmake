if (APPLE_CLANG)
  ExternalProject_Add(libcurl
    PREFIX ${GraphLab_SOURCE_DIR}/deps/curl
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/curl-7.33.0.tar.bz2 
    URL_MD5 57409d6bf0bd97053b8378dbe0cadcef
    INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --without-winidn --without-libidn --without-nghttp2 --without-ca-bundle --without-polarssl --without-cyassl --without-nss --with-darwinssl --disable-crypto-auth --enable-shared=no --enable-static=yes --disable-ldap --without-librtmp --without-zlib --without-ssl --libdir=<INSTALL_DIR>/lib
    )
  find_library(security_framework NAMES Security)
  find_library(core_framework NAMES CoreFoundation)
  macro(requires_curl NAME)
    add_dependencies(${NAME} libcurl)
    target_link_libraries(${NAME} ${core_framework})
    target_link_libraries(${NAME} ${security_framework})
    target_link_libraries(${NAME} curl)
  endmacro()
else()
  ExternalProject_Add(libcurl
    PREFIX ${GraphLab_SOURCE_DIR}/deps/curl
    URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/curl-7.33.0.tar.bz2 
    URL_MD5 57409d6bf0bd97053b8378dbe0cadcef
    INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS=-fPIC CPPFLAGS=-fPIC LIBS=-ldl <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --without-winidn --without-libidn --without-nghttp2 --without-ca-bundle --without-polarssl --without-cyassl --without-nss  --without-darwinssl --disable-crypto-auth --enable-shared=no --enable-static=yes --disable-ldap --without-librtmp --without-zlib --with-ssl=<INSTALL_DIR> --libdir=<INSTALL_DIR>/lib
    )

  add_dependencies(libcurl libssl)

  macro(requires_curl NAME)
    add_dependencies(${NAME} libcurl)
    target_link_libraries(${NAME} curl)
    if (NOT APPLE)
      target_link_libraries(${NAME} rt)
    endif()
    requires_openssl(${NAME})
  endmacro()
endif()
