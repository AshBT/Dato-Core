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
#ifndef FAULT_SOCKETS_PUBLISH_SOCKET_HPP
#define FAULT_SOCKETS_PUBLISH_SOCKET_HPP
#include <string>
#include <vector>
#include <set>
#include <zmq.h>
#include <boost/thread/mutex.hpp>
#include <fault/zmq/zmq_msg_vector.hpp>

namespace graphlab { 
namespace zookeeper_util {
class key_value;
} 
}

namespace libfault {

/**
 * \ingroup fault
 * Constructs a zookeeper backed publish socket.
 * This object is very much single threaded.
 * Sends to this socket will be received by all machines
 * subscribed to the socket.
 * 
 * \code
 * publish_socket pubsock(zmq_ctx, NULL, listen_addr);
 * pubsock.send(...);
 * \endcode
 */
class publish_socket {
 public:
  /**
   * Constructs a request socket.
   * The request will be sent to the current owners of the key
   *
   * \param zmq_ctx A zeroMQ Context
   * \param keyval A zookeeper key_value object to bind to
   * \param alternate_bind_address If set, this will be address to bind to.
   *                               Otherwise, binds to a free tcp address.
   *
   * keyval can be NULL, in which case, the alternate_bind_address must be
   * provided, in which case this socket behaves like a simple
   * messaging wrapper around ZeroMQ which provides a publish socket.
   */
  publish_socket(void* zmq_ctx,
                 graphlab::zookeeper_util::key_value* keyval,
                 std::string alternate_bind_address = "");

  /**
   * Closes this socket. Once closed, the socket cannot be used again.
   */
  void close();


  /**
   * Sends a message. All subscribers which match the message (by prefix)
   * will receive a copy.
   */
  void send(zmq_msg_vector& msg);

  ~publish_socket();

  /**
   * Tries to register this socket under a given object key.
   * Returns true on success and false on failure. Only relevant when zookeeper
   * is used. Otherwise this will always return true.
   */
  bool register_key(std::string key);

  /**
   * Like register, but sets the key to an empty value.
   * Reserves ownership of the key, but prohibits people from joining. Only
   * relevant when zookeeper is used. Otherwise this will always return true.
   */
  bool reserve_key(std::string key);


  /**
   * Tries to unregister this socket from a given object key.
   * Returns true on success and false on failure. Only relevant when zookeeper
   * is used. Otherwise this will always return true.
   */
  bool unregister_key(std::string key);

  /**
   * Unregisters all keys this socket was registered under. Only relevant when
   * zookeeper is used. Otherwise this will always return true.
   */
  void unregister_all_keys();

  /**
   * Returns the address the socket is bound to
   */
  std::string get_bound_address();

 private:
  void* z_ctx;
  void* z_socket;
  graphlab::zookeeper_util::key_value* zk_keyval;
  std::string local_address;
  std::set<std::string> registered_keys;
};


} // namespace fault
#endif
