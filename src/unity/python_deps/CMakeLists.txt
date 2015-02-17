project(python_deps)


ADD_CUSTOM_TARGET (python_deps_psutil ALL)
ADD_CUSTOM_COMMAND(
  COMMENT "build psutil"
  COMMAND rm -rf ${CMAKE_CURRENT_BINARY_DIR}/../python/graphlab_util/psutil
  COMMAND rm -rf ${CMAKE_CURRENT_BINARY_DIR}/psutil
  COMMAND bash -c "ARCHFLAGS=\"-Wno-error=unused-command-line-argument-hard-error-in-future\" python setup.py build -b ${CMAKE_CURRENT_BINARY_DIR}/psutil"
  COMMAND mv ${CMAKE_CURRENT_BINARY_DIR}/psutil/lib*/* ${CMAKE_CURRENT_BINARY_DIR}/psutil
  COMMAND rm -rf ${CMAKE_CURRENT_BINARY_DIR}/psutil/temp*
  COMMAND rm -rf ${CMAKE_CURRENT_BINARY_DIR}/psutil/lib*
  COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/../python/graphlab_psutil
  COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/psutil/*.so ${CMAKE_CURRENT_BINARY_DIR}/../python/graphlab_psutil
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/psutil
  TARGET python_deps_psutil
)

