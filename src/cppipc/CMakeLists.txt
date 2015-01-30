project(cppipc)


# Libraries should not link both cppipc and cppipc_zookeeper together
# They are not compatible
make_library(cppipc
  SOURCES
    client/comm_client.cpp
    common/message_types.cpp
    common/object_factory.cpp
    common/object_factory_impl.cpp
    common/ipc_deserializer.cpp
    server/comm_server.cpp
  REQUIRES
    fault_comm boost logger cancel_serverside_ops
  EXTERNAL_VISIBILITY
)

make_library(cancel_serverside_ops
  SOURCES
    server/cancel_ops.cpp
)

set_property(TARGET cppipc APPEND PROPERTY COMPILE_DEFINITIONS FAKE_ZOOKEEPER) 

# Libraries should not link both cppipc and cppipc_zookeeper together
# They are not compatible
make_library(cppipc_zookeeper
  SOURCES
    client/comm_client.cpp
    common/message_types.cpp
    common/object_factory.cpp
    common/object_factory_impl.cpp
    common/ipc_deserializer.cpp
    server/comm_server.cpp
  REQUIRES
    fault_comm_zookeeper boost logger
  EXTERNAL_VISIBILITY
)
