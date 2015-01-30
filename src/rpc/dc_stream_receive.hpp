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
/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef DC_STREAM_RECEIVE_HPP
#define DC_STREAM_RECEIVE_HPP
#include <boost/type_traits/is_base_of.hpp>
#include <rpc/circular_char_buffer.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_compile_parameters.hpp>
#include <rpc/dc_receive.hpp>
#include <parallel/atomic.hpp>
#include <parallel/pthread_tools.hpp>
#include <logger/logger.hpp>
namespace graphlab {
class distributed_control;

namespace dc_impl {

/**
 * \internal
  \ingroup rpc
  Receiver processor for the dc class.
  The job of the receiver is to take as input a byte stream
  (as received from the socket) and cut it up into meaningful chunks.
  This can be thought of as a receiving end of a multiplexor.
  
  This is the default unbuffered receiver.
*/
class dc_stream_receive: public dc_receive{
 public:
  
  dc_stream_receive(distributed_control* dc, procid_t associated_proc): 
                  writebuffer(NULL), write_buffer_written(0), dc(dc), 
                  associated_proc(associated_proc) { 
    writebuffer = (char*)malloc(RECEIVE_BUFFER_SIZE);
    write_buffer_len = RECEIVE_BUFFER_SIZE;
  }

 private:

  char* writebuffer;
  size_t write_buffer_written;
  size_t write_buffer_len;
  
  /// pointer to the owner
  distributed_control* dc;

  procid_t associated_proc;
  
  void shutdown();

  
  char* get_buffer(size_t& retbuflength);
  

  char* advance_buffer(char* c, size_t wrotelength, 
                              size_t& retbuflength);
  
};


} // namespace dc_impl
} // namespace graphlab
#endif

