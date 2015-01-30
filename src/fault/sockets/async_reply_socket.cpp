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
#include <cstdlib>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <fault/sockets/socket_errors.hpp>
#include <fault/sockets/socket_config.hpp>
#include <fault/sockets/async_reply_socket.hpp>
#include <fault/zmq/print_zmq_error.hpp>
#include <fault/util/net_util.hpp>
#include <fault/sockets/get_next_port_number.hpp>
#ifdef FAKE_ZOOKEEPER
#include <fault/fake_key_value.hpp>
#else
#include <zookeeper_util/key_value.hpp>
#endif
namespace libfault {

static size_t ASYNC_REPLY_SOCKET_CTR = 1;


async_reply_socket::async_reply_socket(void* zmq_ctx,
                           graphlab::zookeeper_util::key_value* keyval,
                           callback_type callback,
                           size_t nthreads,
                           std::string alternate_bind_address,
                           std::string secret_key)
    :z_ctx(zmq_ctx), zk_keyval(keyval), callback(callback), associated_pollset(NULL) {
  // create a socket
  z_socket = zmq_socket(z_ctx, ZMQ_ROUTER);
  assert(z_socket);

  if (!secret_key.empty()) {
    const int is_server = 1;
    int rc = zmq_setsockopt (z_socket, ZMQ_CURVE_SERVER, &is_server, sizeof(int));
    assert (rc == 0);

    assert(secret_key.length() == 40);
    rc = zmq_setsockopt (z_socket, ZMQ_CURVE_SECRETKEY, secret_key.c_str(), 40);
    assert (rc == 0);
  }

  set_conservative_socket_parameters(z_socket);
  assert(z_socket != NULL);

  if (!alternate_bind_address.empty()) {
    local_address = alternate_bind_address;
    int rc = zmq_bind(z_socket, local_address.c_str());
    if (rc != 0) {
      print_zmq_error("async_reply_socket construction: ");
      assert(rc == 0);
    }
  } else {
    // what is the listening socket?
    std::string localip = graphlab::get_local_ip_as_str(true);
    bool ok = false;
    while (!ok) {
      size_t port = get_next_port_number();
      char port_as_string[128];
      sprintf(port_as_string, "%ld", port);
      local_address = "tcp://" + localip + ":" + port_as_string;
      // try to bind
      int rc = zmq_bind(z_socket, local_address.c_str());
      ok = (rc == 0);
      /*if (rc == EADDRINUSE) {
        std::cout << local_address << " in use. Trying another port.\n";
        continue;
      } else if (rc != 0) {
        std::cout << "Unable to bind to " << local_address << ". "
                  << "Error(" << rc << ") = " << zmq_strerror(rc) << "\n";
      }*/
    }
  }
  std::cout << "Bound to " << local_address << "\n";

  // now construct the threads and the required inproc sockets
  char inprocname[64];
  sprintf(inprocname, "inproc://async_rep_%ld", ASYNC_REPLY_SOCKET_CTR);
  ++ASYNC_REPLY_SOCKET_CTR;
  inproc_pull_socket = zmq_socket(zmq_ctx, ZMQ_PULL);
  if (inproc_pull_socket == NULL) {
    print_zmq_error("async_reply_socket");
    assert(inproc_pull_socket != NULL);
  }
  int rc = zmq_bind(inproc_pull_socket, inprocname);
  if (rc != 0) {
    print_zmq_error("async_reply_socket");
    assert(false);
  }

  assert(nthreads > 0);
  threads.resize(nthreads);
  queue_terminate = false;
  for (size_t i = 0;i < threads.size(); ++i) {
    threads[i].parent = this;
    threads[i].inproc_push_socket = zmq_socket(zmq_ctx, ZMQ_PUSH);
    if (threads[i].inproc_push_socket == NULL) {
      print_zmq_error("async_reply_socket");
      assert(threads[i].inproc_push_socket != NULL);
    }
    // connect the push to the pull
    int rc = zmq_connect(threads[i].inproc_push_socket, inprocname);
    if (rc != 0) {
      print_zmq_error("async_reply_socket");
      assert(rc == 0);
    }

    // launch the threads
    threads[i].thread = new boost::thread(boost::bind(&async_reply_socket::thread_function,
                                                      this,
                                                      &threads[i]));
  }
}

void async_reply_socket::close() {
  if (z_socket != NULL) {
    remove_from_pollset();
    unregister_all_keys();
    // kill all threads
    queuelock.lock();
    queue_terminate = true;
    queuecond.notify_all();
    queuelock.unlock();
    for (size_t i = 0;i < threads.size(); ++i) {
      threads[i].thread->join();
      delete threads[i].thread;
      zmq_close(threads[i].inproc_push_socket);
    }
    threads.clear();
    zmq_close(z_socket);
    zmq_close(inproc_pull_socket);
    z_socket = NULL;
  }
}

async_reply_socket::~async_reply_socket() {
  close();
}

bool async_reply_socket::register_key(std::string key) {
  if (zk_keyval == NULL) return false;
  bool ret = zk_keyval->insert(key, local_address);
  if (ret) registered_keys.insert(key);
  return ret;
}

bool async_reply_socket::reserve_key(std::string key) {
  if (zk_keyval == NULL) return false;
  bool ret = zk_keyval->insert(key, "");
  if (ret) registered_keys.insert(key);
  return ret;
}



bool async_reply_socket::unregister_key(std::string key) {
  if (zk_keyval == NULL) return false;
  if (registered_keys.count(key)) {
    registered_keys.erase(registered_keys.find(key));
    return zk_keyval->erase(key);
  } else {
    return false;
  }
}

void async_reply_socket::unregister_all_keys() {
  // make a copy of the registered_keys set since the
  // unregister_key call will modify it
  std::set<std::string> keys = registered_keys;
  std::set<std::string>::const_iterator iter = keys.begin();
  while (iter != keys.end()) {
    assert(unregister_key(*iter));
    ++iter;
  }
}


void async_reply_socket::wrapped_callback(socket_receive_pollset* unused,
                                          const zmq_pollitem_t& unused2) {
  std::vector<zmq_msg_vector*> to_push_into_queue;
  while(1) {
    zmq_msg_vector* recv = new zmq_msg_vector;
    // receive with a timeout of 0
    // if nothing to receive, return
    if (recv->recv(z_socket, 0) != 0) {
      delete recv;
      break;
    }
    to_push_into_queue.push_back(recv);
  }

  queuelock.lock();
  for (size_t i = 0;i < to_push_into_queue.size(); ++i) {
    jobqueue.push(to_push_into_queue[i]);
    queuecond.notify_one();
  }
  queuelock.unlock();
}


void async_reply_socket::add_to_pollset(socket_receive_pollset* pollset) {
  assert(associated_pollset == NULL);
  associated_pollset = pollset;
  zmq_pollitem_t item;
  item.socket = z_socket;
  item.fd = 0;
  pollset->add_pollitem(item, boost::bind(&async_reply_socket::wrapped_callback, this,
                                          _1, _2));
  item.socket = inproc_pull_socket;
  pollset->add_pollitem(item, boost::bind(&async_reply_socket::pull_socket_callback, this,
                                            _1, _2));
}

void async_reply_socket::remove_from_pollset() {
  if (associated_pollset != NULL) {
    assert(associated_pollset != NULL);
    zmq_pollitem_t item;
    item.socket = z_socket;
    item.fd = 0;
    associated_pollset->remove_pollitem(item);
    item.socket = inproc_pull_socket;
    associated_pollset->remove_pollitem(item);
    associated_pollset = NULL;
  }
}



void async_reply_socket::pull_socket_callback(socket_receive_pollset* unused,
                                              const zmq_pollitem_t& unused2) {
  while(1) {
    zmq_msg_vector data;
    // receive with a timeout of 0
    // if nothing to receive, return
    if (data.recv(inproc_pull_socket, 0) != 0) break;
    // send out the router
    int rc = 0;
    rc = data.send(z_socket);

    if (rc != 0) {
      std::cerr << "Failed to send message: " << zmq_strerror(rc) << std::endl;
    }
  }
}

void async_reply_socket::process_job(thread_data* data, zmq_msg_vector* msg) {
  zmq_msg_vector send;

  // strip message envelope from incoming message
  // and put it to send 
  while(msg->size() > 0) {
    zmq_msg_init(send.insert_back());
    zmq_msg_copy(send.back(), msg->front()); msg->pop_front_and_free();
    if (zmq_msg_size(send.back()) == 0) break;
  }

  // bad packet
  if (msg->size() == 0) {
    std::cerr << "Unexpected Message Format\n";
    delete msg;
    return;
  }

  // check that this is for a key I am registered for
  if (zk_keyval) {
    std::string s = msg->extract_front();
    if(registered_keys.count(s) == 0) {
      std::cerr << "Received message "<< s << " destined for a different object!\n";
      delete msg;
      return;
    }
  }
  zmq_msg_vector reply;
  bool hasreply = callback(*msg, reply);

  delete msg;

  if (hasreply) {
    while(!reply.empty()) {
      zmq_msg_init(send.insert_back());
      zmq_msg_copy(send.back(), reply.front());
      reply.pop_front_and_free();
    }
    // reply
    int rc = 0;
    rc = send.send(data->inproc_push_socket);

    if (rc != 0) {
      std::cerr << "Failed to push message: " << zmq_strerror(rc) << std::endl;
    }
  }
}

void async_reply_socket::thread_function(thread_data* data) {
  boost::unique_lock<boost::mutex> lock(queuelock);
  while(1) {
    while(jobqueue.empty() && !queue_terminate) {
      queuecond.wait(lock);
    }
    // at this point, we have the lock, either
    // jobqueue has something, or queue_terminate is set
    if (queue_terminate) break;
    assert(!jobqueue.empty());
    zmq_msg_vector* msg = jobqueue.front();
    jobqueue.pop();
    // we process the job outside of the lock
    lock.unlock();

    // this function also frees msg
    process_job(data, msg);

    lock.lock();
  }
}

std::string async_reply_socket::get_bound_address() {
  char* buf[256];
  size_t optlen = 256;
  zmq_getsockopt(z_socket, ZMQ_LAST_ENDPOINT, (void*)buf, &optlen);
  return std::string((char*)buf);
}

} // namespace libfault

