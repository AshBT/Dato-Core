project(unity_cython)
include(UseCython)

include_directories(${NUMPY_INCLUDE_DIRS})

add_custom_target(libbase_dep.so

  COMMAND cp ${CMAKE_BINARY_DIR}/src/unity/libunity_common_without_sdk_types.a ${CMAKE_BINARY_DIR}/src/cppipc/libcppipc.a ${CMAKE_BINARY_DIR}/src/flexible_type/libflexible_type.a ${CMAKE_BINARY_DIR}/src/random/librandom.a ${CMAKE_BINARY_DIR}/src/parallel/libparallel.a ${CMAKE_BINARY_DIR}/src/logger/liblogger.a ${CMAKE_BINARY_DIR}/src/fault/libfault_comm.a ${CMAKE_BINARY_DIR}/src/cppipc/libcancel_serverside_ops.a ${CMAKE_BINARY_DIR}/src/image/libimage_type.a  ${CMAKE_BINARY_DIR}/../deps/local/lib/libboost_thread.a ${CMAKE_BINARY_DIR}/../deps/local/lib/libzmq.a ${CMAKE_BINARY_DIR}/../deps/local/lib/libsodium.a ${CMAKE_BINARY_DIR}/../deps/local/lib/libboost_chrono.a ${CMAKE_BINARY_DIR}/../deps/local/lib/libboost_system.a ${CMAKE_BINARY_DIR}/../deps/local/lib/libboost_date_time.a  .
  COMMAND rm -f *.o
  COMMAND ar -x libunity_common_without_sdk_types.a
  COMMAND ar -x libcppipc.a
  COMMAND ar -x libflexible_type.a
  COMMAND ar -x librandom.a
  COMMAND ar -x liblogger.a
  COMMAND ar -x libparallel.a
  COMMAND ar -x libfault_comm.a
  COMMAND ar -x libcancel_serverside_ops.a
  COMMAND ar -x libimage_type.a
  COMMAND ar -x libboost_thread.a
  COMMAND ar -x libzmq.a
  COMMAND ar -x libsodium.a
  COMMAND ar -x libboost_chrono.a
  COMMAND ar -x libboost_system.a
  COMMAND ar -x libboost_date_time.a
  COMMAND ${CMAKE_CXX_COMPILER} -static-libstdc++ -std=c++11 -shared *.o -o libbase_dep.so 
  DEPENDS unity_common_without_sdk_types cppipc flexible_type random logger parallel fault_comm cancel_serverside_ops image_type )

function(cython_add_graphlab_module _module_name _sources)
  set_source_files_properties( ${_sources} PROPERTIES CYTHON_IS_CXX TRUE)
  cython_add_module( ${_module_name} ${_sources} )
  if(DISTRIBUTION)
    if (NOT APPLE)
      set_property(TARGET ${_module_name} APPEND PROPERTY LINK_FLAGS "-static-libstdc++")
    endif()
  endif()
  requires(${_module_name} cppipc flexible_type random)
endfunction()


function(cython_add_graphlab_module2 _module_name _sources)
  set_source_files_properties( ${_sources} PROPERTIES CYTHON_IS_CXX TRUE)
  cython_add_module( ${_module_name} ${_sources} )
  if(DISTRIBUTION)
    if (NOT APPLE)
      set_property(TARGET ${_module_name} APPEND PROPERTY LINK_FLAGS "-static-libstdc++")
    endif()
  endif()
  if (APPLE)
    set_property(TARGET ${_module_name} PROPERTY INSTALL_NAME_DIR "@rpath")
  endif()
  set_property(TARGET ${_module_name} APPEND_STRING PROPERTY LINK_FLAGS " -Wl,-rpath,$ORIGIN -Wl,-rpath,${CMAKE_CURRENT_BINARY_DIR} ")
  requires(${_module_name} boost zeromq ${CMAKE_CURRENT_BINARY_DIR}/libbase_dep.so)
  add_dependencies(${_module_name} libbase_dep.so)

  if (APPLE)
    add_custom_command(TARGET ${_module_name}
                      POST_BUILD
                      COMMAND install_name_tool -change libbase_dep.so @rpath/libbase_dep.so ${_module_name}.so)
  endif()
endfunction()

cython_add_graphlab_module2(cy_unity cy_unity.pyx)
cython_add_graphlab_module2(cy_ipc cy_ipc.pyx)
cython_add_graphlab_module2(cy_graph cy_graph.pyx)
cython_add_graphlab_module2(cy_variant cy_variant.pyx)
cython_add_graphlab_module2(cy_model cy_model.pyx)
cython_add_graphlab_module2(cy_sframe cy_sframe.pyx)
cython_add_graphlab_module2(cy_dataframe cy_dataframe.pyx)
cython_add_graphlab_module2(cy_sarray cy_sarray.pyx)
cython_add_graphlab_module2(cy_sketch cy_sketch.pyx)
cython_add_graphlab_module2(cy_flexible_type cy_flexible_type.pyx)
cython_add_graphlab_module2(cy_type_utils cy_type_utils.pyx)
cython_add_graphlab_module2(cy_test_utils cy_test_utils.pyx)
