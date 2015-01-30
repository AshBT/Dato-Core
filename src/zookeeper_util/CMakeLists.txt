project(zookeeper_util)

make_library(zookeeper_util
  SOURCES
  key_value.cpp
  server_list.cpp
  zookeeper_common.cpp
  REQUIRES
   zookeeper
)
