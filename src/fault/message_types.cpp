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
#include <vector>
#include <zmq.h>
#include <fault/message_types.hpp>
#include <fault/zmq/zmq_msg_standard_free.hpp>
namespace libfault {



/**************************************************************************/
/*                                                                        */
/*                             Query Message                              */
/*                                                                        */
/**************************************************************************/


void query_object_message::parse(zmq_msg_vector& data) {
  // there should be 2 parts. A header 
  // then the actual data
  assert(data.num_unread_msgs() >= 2);
  zmq_msg_t* zhead = data.read_next();
  zmq_msg_t* zmsg = data.read_next();
  assert(zmq_msg_size(zhead) == sizeof(header));

  header= *(header_type*)zmq_msg_data(zhead);
  msg = (char*)zmq_msg_data(zmsg);
  msglen = zmq_msg_size(zmsg);
}

void query_object_message::write(zmq_msg_vector& outdata) {
  // create 2 message part. One with the header 
  // then the actual data.
  zmq_msg_t* zhead = outdata.insert_back();
  zmq_msg_init_size(zhead, sizeof(header_type));
  (*(header_type*)zmq_msg_data(zhead)) = header;

  zmq_msg_t* zmsg = outdata.insert_back();
  zmq_msg_init_data(zmsg, msg, msglen, zmq_msg_standard_free, NULL);
}



/**************************************************************************/
/*                                                                        */
/*                              Query Reply                               */
/*                                                                        */
/**************************************************************************/

void query_object_reply::parse(zmq_msg_vector& data) {
  // there should be 2 parts. A header 
  // then the actual data
  assert(data.num_unread_msgs() >= 2);
  zmq_msg_t* zhead = data.read_next();
  zmq_msg_t* zmsg = data.read_next();
  assert(zmq_msg_size(zhead) == sizeof(header));

  header= *(header_type*)zmq_msg_data(zhead);
  msg = (char*)zmq_msg_data(zmsg);
  msglen = zmq_msg_size(zmsg);
}


void query_object_reply::write(zmq_msg_vector& outdata) {
  // create 2 message part. One with the header 
  // then the actual data.
  zmq_msg_t* zhead = outdata.insert_back();
  zmq_msg_init_size(zhead, sizeof(header_type));
  (*(header_type*)zmq_msg_data(zhead)) = header;

  zmq_msg_t* zmsg = outdata.insert_back();
  zmq_msg_init_data(zmsg, msg, msglen, zmq_msg_standard_free, NULL);
}


} // namespace libfault
