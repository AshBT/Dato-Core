/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef QUERY_OBJECT_SERVER_REPLICA_HPP
#define QUERY_OBJECT_SERVER_REPLICA_HPP
#include <stdint.h>
#include <vector>
#include <string>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <zookeeper_util/key_value.hpp>
#include <fault/sockets/reply_socket.hpp>
#include <fault/sockets/subscribe_socket.hpp>
#include <fault/query_object_server_common.hpp>
#include <fault/query_object.hpp>
#include <fault/zmq/zmq_msg_vector.hpp>
namespace libfault {

/**
 * \ingroup fault
 */
struct query_object_server_replica {

  void* z_ctx;
  graphlab::zookeeper_util::key_value* keyval;
  std::string objectkey; // the object key associated with this object
  size_t replicaid;
  query_object* qobj; // The query object
  reply_socket* repsock; // the reply socket associated with the query object
  subscribe_socket* subsock; // If this is a master, it also has an associated
  bool waiting_for_snapshot;
  size_t zk_kv_callback_id;


  boost::shared_mutex query_obj_rwlock;

  int localpipes[2];

  std::vector<zmq_msg_vector> buffered_messages;

  socket_receive_pollset pollset;

  query_object_server_replica(void* zmq_ctx,
                             graphlab::zookeeper_util::key_value* zk_keyval,
                             std::string objectkey,
                             query_object* qobj,
                             size_t replicaid);

  bool replica_reply_callback(zmq_msg_vector& recv,
                              zmq_msg_vector& reply);

  bool subscribe_callback(zmq_msg_vector& recv);

  // returns 0 on completion, -1 on failure, +1 if promote to master
  int start();

  void playback_recorded_messages();


  void keyval_change(graphlab::zookeeper_util::key_value* unused,
                     const std::vector<std::string>& newkeys,
                     const std::vector<std::string>& deletedkeys,
                     const std::vector<std::string>& modifiedkeys);

  ~query_object_server_replica();
};



} // namespace libfault
#endif
