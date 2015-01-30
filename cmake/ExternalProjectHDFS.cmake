# LibHDFS =====================================================================
# If JNI is found we install libhdfs which allows programs to read and write
# to hdfs filesystems
# md5: c78cf463decb3ae9611bcea3cee1948e 
# url: http://s3-us-west-2.amazonaws.com/glbin-engine/gl_libhdfs.tar.gz
if (APPLE)
ExternalProject_Add(hadoop
  PREFIX ${CMAKE_SOURCE_DIR}/deps/hadoop
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/gl_libhdfs_20140714190709.tar.gz
  URL_MD5 0e9d4b0a1e6811d25ae78f8e7adcc328
  BUILD_IN_SOURCE 1
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND bash -c "mkdir -p <INSTALL_DIR>/include && cp <SOURCE_DIR>/hdfs.h <INSTALL_DIR>/include && cp <SOURCE_DIR>/osx/libhdfs.so <INSTALL_DIR>/lib")
else()
ExternalProject_Add(hadoop
  PREFIX ${CMAKE_SOURCE_DIR}/deps/hadoop
  URL http://s3-us-west-2.amazonaws.com/glbin-engine/gl_libhdfs_20140714190709.tar.gz
  URL_MD5 0e9d4b0a1e6811d25ae78f8e7adcc328
  BUILD_IN_SOURCE 1
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND bash -c "mkdir -p <INSTALL_DIR>/include && cp <SOURCE_DIR>/hdfs.h <INSTALL_DIR>/include && cp <SOURCE_DIR>/linux/libhdfs.so <INSTALL_DIR>/lib")
endif()
add_definitions(-DHAS_HADOOP)
set(HAS_HADOOP ON CACHE bool "Use hadoop dependency")

macro(requires_hdfs NAME)
	target_link_libraries(${NAME} hdfs ${JAVA_JVM_LIBRARY})
	add_dependencies(${NAME} hadoop)
endmacro(requires_hdfs)



