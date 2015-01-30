project(fault)


# Libraries should not link both fault_comm_zookeeper and fault_comm together
# They are not compatible
make_library(fault_comm
  SOURCES
  zmq/zmq_msg_standard_free.cpp
  zmq/zmq_msg_vector.cpp
  zmq/print_zmq_error.cpp
  util/net_util.cpp
  sockets/get_next_port_number.cpp
  sockets/socket_config.cpp
  sockets/reply_socket.cpp
  sockets/request_socket.cpp
  sockets/publish_socket.cpp
  sockets/subscribe_socket.cpp
  sockets/async_request_socket.cpp
  sockets/async_reply_socket.cpp
  sockets/socket_receive_pollset.cpp
  REQUIRES 
  zeromq
)
set_property(TARGET fault_comm APPEND PROPERTY COMPILE_DEFINITIONS FAKE_ZOOKEEPER) 


# Libraries should not link both fault_comm_zookeeper and fault_comm together
# They are not compatible
make_library(fault_comm_zookeeper
  SOURCES
  zmq/zmq_msg_standard_free.cpp
  zmq/zmq_msg_vector.cpp
  zmq/print_zmq_error.cpp
  util/net_util.cpp
  sockets/get_next_port_number.cpp
  sockets/socket_config.cpp
  sockets/reply_socket.cpp
  sockets/request_socket.cpp
  sockets/publish_socket.cpp
  sockets/subscribe_socket.cpp
  sockets/async_request_socket.cpp
  sockets/async_reply_socket.cpp
  sockets/socket_receive_pollset.cpp
  REQUIRES 
  zeromq
  zookeeper_util
)

make_library(fault
  SOURCES
  message_types.cpp
  query_object.cpp
  query_object_client.cpp
  query_object_server_common.cpp
  query_object_server_master.cpp
  query_object_server_replica.cpp
  query_object_server_process.cpp
  query_object_server_manager.cpp
  REQUIRES 
  zeromq
  libsodium
  boost
  zookeeper_util
  fault_comm_zookeeper
)

# INSTALL(TARGETS fault ARCHIVE DESTINATION lib)
