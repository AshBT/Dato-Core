ExternalProject_Add(zookeeper
  PREFIX ${CMAKE_SOURCE_DIR}/deps/zookeeper
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/zookeeper-3.4.6.tar.gz
  URL_MD5 971c379ba65714fd25dc5fe8f14e9ad1
  PATCH_COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/patches/zookeeper/ <SOURCE_DIR>
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND CFLAGS=-fPIC CPPFLAGS=-fPIC CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./configure --prefix=<INSTALL_DIR> --disable-shared
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local)



 macro(requires_zookeeper NAME)
  add_dependencies(${NAME} zookeeper)
  target_link_libraries(${NAME} zookeeper_mt)
 endmacro(requires_zookeeper)
