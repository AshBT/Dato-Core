project(unity)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/graphlab
)

add_subdirectory(graphlab/cython)

# With CMake, a clean separation can be made between the source tree and the
# build tree.  When all source is compiled, as with pure C/C++, the source is
# no-longer needed in the build tree.  However, with pure *.py source, the
# source is processed directly.  To handle this, we reproduce the availability
# of the source files in the build tree.

# enable_testing()
# find_file( PYTEST_EXECUTABLE py.test )
# add_test( pytests "env PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR} ${PYTEST_EXECUTABLE}" ${cython_interface_SOURCE_DIR}/tests )

ADD_CUSTOM_TARGET (python_source ALL)
ADD_CUSTOM_COMMAND(
  COMMENT "build python source"
  COMMAND
  ${CMAKE_COMMAND} -P
  ${CMAKE_SOURCE_DIR}/cmake/ReplicatePythonSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  TARGET python_source
)

add_dependencies(python_source python_deps_psutil)

ADD_CUSTOM_TARGET (clean_python)
ADD_CUSTOM_COMMAND(
  COMMENT "clean python source"
  COMMAND
  ${CMAKE_COMMAND} -P
  ${CMAKE_SOURCE_DIR}/cmake/RemovePythonSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  TARGET clean_python
)
