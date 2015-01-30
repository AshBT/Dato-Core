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
#include <cassert>
#include <fault/query_object_server_common.hpp>
#include <fault/query_object_client.hpp>
#include <fault/zmq/zmq_msg_standard_free.hpp>
#include <fault/message_types.hpp>
#include <fault/message_flags.hpp>

namespace libfault {
namespace zookeeper_util = graphlab::zookeeper_util;

query_object_client::query_object_client(void* zmq_ctx,
                                         std::vector<std::string> zkhosts,
                                         std::string prefix,
                                         size_t replica_count)
    :z_ctx(zmq_ctx), replica_count(replica_count) {
  zk_keyval = new zookeeper_util::key_value(zkhosts, prefix, "");
  my_keyval = true;
  pollset.start_poll_thread();
}


query_object_client::query_object_client(void* zmq_ctx,
                                         zookeeper_util::key_value* keyval,
                                         size_t replica_count)
    :z_ctx(zmq_ctx), zk_keyval(keyval), replica_count(replica_count) {
  my_keyval = false;
  pollset.start_poll_thread();
}



query_object_client::~query_object_client() {
  pollset.stop_poll_thread();
  std::map<std::string, socket_data*>::iterator iter = sockets.begin();
  while (iter != sockets.end()) {
    if (iter->second != NULL) {
      if (iter->second->sock != NULL) {
        delete iter->second->sock;
      }
      delete iter->second;
    }
    ++iter;
  }
  sockets.clear();
  if (my_keyval) delete zk_keyval;
}


void* query_object_client::get_object_handle(const std::string& objectkey) {
  return get_socket(objectkey);
}


query_object_client::socket_data*
query_object_client::get_socket(const std::string& objectkey) {
  boost::lock_guard<boost::mutex> guard(lock);
  std::map<std::string, socket_data*>::iterator iter = sockets.find(objectkey);
  if (iter != sockets.end()) return iter->second;
  else {
    // create a socket
    socket_data* newsock = new socket_data;
    newsock->creation_time = time(NULL);
    newsock->key = objectkey;
    newsock->randid = ((uint64_t)(rand()) << 32) + rand();
    std::string masterkey = get_zk_objectkey_name(objectkey, 0);
    std::vector<std::string> slavekeys;
    for (size_t i = 1; i <= replica_count; ++i) {
      slavekeys.push_back(get_zk_objectkey_name(objectkey, i));
    }

    newsock->sock = new async_request_socket(z_ctx,
                                             zk_keyval,
                                             masterkey,
                                             slavekeys);

    newsock->sock->add_to_pollset(&pollset);

    sockets[objectkey] = newsock;
    return newsock;
  }
}


query_object_client::query_result
query_object_client::query_update_general(void* objecthandle,
                                          char* msg, size_t msglen,
                                          uint64_t flags) {
  // the returned object
  query_result ret;

  socket_data* sock = (socket_data*)(objecthandle);

  query_object_message qmsg;
  qmsg.header.flags = flags;
  qmsg.header.msgid = __sync_fetch_and_add(&sock->randid, 113);
  qmsg.msg = msg;
  qmsg.msglen = msglen;

  zmq_msg_vector send;
  qmsg.write(send);
  if (flags & QO_MESSAGE_FLAG_ANY_TARGET) {
    ret.content_ptr->future = sock->sock->request_any(send);
  } else {
    ret.content_ptr->future = sock->sock->request_master(send);
  }
  return ret;
}




} // namespace libfault
