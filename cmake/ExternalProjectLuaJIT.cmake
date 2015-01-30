ExternalProject_Add(luajitproj
  PREFIX ${CMAKE_SOURCE_DIR}/deps/luajit
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/LuaJIT-2.0.3.tar.gz
  URL_MD5 f14e9104be513913810cd59c8c658dc0
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ""
  BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} make -j4
  INSTALL_COMMAND mkdir -p <INSTALL_DIR>/lib && mkdir -p <INSTALL_DIR>/include/lua && mkdir -p <INSTALL_DIR>/bin &&
                  cp ${CMAKE_SOURCE_DIR}/deps/luajit/src/luajitproj/src/libluajit.a <INSTALL_DIR>/lib && 
                  bash -c "cp ${CMAKE_SOURCE_DIR}/deps/luajit/src/luajitproj/src/*.h <INSTALL_DIR>/include/lua" && 
                  cp ${CMAKE_SOURCE_DIR}/deps/luajit/src/luajitproj/src/luajit <INSTALL_DIR>/bin 
  )

macro(requires_luajit NAME)
  target_link_libraries(${NAME} luajit)
  add_dependencies(${NAME} luajitproj)
endmacro(requires_luajit)


