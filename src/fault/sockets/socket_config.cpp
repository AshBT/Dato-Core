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
#include <zmq.h>
#include <boost/integer_traits.hpp>
namespace libfault {
int SEND_TIMEOUT = 1000; 
int RECV_TIMEOUT = 5000;

void set_send_timeout(int ms) {
  SEND_TIMEOUT = ms;
}
void set_recv_timeout(int ms) {
  RECV_TIMEOUT = ms;
}


void set_conservative_socket_parameters(void* z_socket) {
  int lingerval = 500;
  int timeoutms = 500;
  int hwm = 0;
  int rc = zmq_setsockopt(z_socket, ZMQ_LINGER, &lingerval, sizeof(lingerval));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_RCVTIMEO, &timeoutms, sizeof(timeoutms));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms));
  assert(rc == 0);
  
  rc = zmq_setsockopt(z_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
  assert(rc == 0);
  
}

};
