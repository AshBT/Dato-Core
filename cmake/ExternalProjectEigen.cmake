ExternalProject_Add(eigen
  PREFIX ${CMAKE_SOURCE_DIR}/deps/eigen
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/deps/eigen_3.1.2.tar.bz2
  URL_MD5 e9c081360dde5e7dcb8eba3c8430fde2
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND cp -r Eigen unsupported <INSTALL_DIR>/
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local/include)
add_definitions(-DHAS_EIGEN -DEIGEN_DONT_PARALLELIZE)

macro(requires_eigen NAME)
  add_dependencies(${NAME} eigen)
endmacro(requires_eigen)

