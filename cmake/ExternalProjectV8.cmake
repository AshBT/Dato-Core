ExternalProject_Add(libv8
  PREFIX ${CMAKE_SOURCE_DIR}/deps/libv8
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/v8.tar.bz2
  URL_MD5 089955a7a968247cd2be4d368037f41e
  BUILD_IN_SOURCE 1
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  #CONFIGURE_COMMAND make dependencies
  CONFIGURE_COMMAND ""
  UPDATE_COMMAND "" # Disable updates
  BUILD_COMMAND make native library=shared
  INSTALL_COMMAND sh -c
  "cp -r <SOURCE_DIR>/include <INSTALL_DIR>/include/v8  & cp <SOURCE_DIR>/out/native/*${CMAKE_SHARED_LIBRARY_SUFFIX} <INSTALL_DIR>/lib" )

ExternalProject_Add(cvv8
  PREFIX ${CMAKE_SOURCE_DIR}/deps/cvv8
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/v8-convert.tar.bz2
  URL_MD5 443bc3cdf7ed724f7def81b6b779b5c2
  BUILD_IN_SOURCE 1
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND
  rsync -acC <SOURCE_DIR>/include/cvv8/ <INSTALL_DIR>/include/cvv8)



macro(requires_v8 NAME)
  get_target_property(tmp ${NAME} COMPILE_FLAGS)
  if (NOT tmp)
  set(tmp "-I${CMAKE_SOURCE_DIR}/deps/local/include/v8")
  else()
  set(tmp "${tmp} -I${CMAKE_SOURCE_DIR}/deps/local/include/v8")
  endif()
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS "${tmp}")
  target_link_libraries(${NAME} v8)
  add_dependencies(${NAME} libv8 cvv8)
endmacro(requires_v8)

