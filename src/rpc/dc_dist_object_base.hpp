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
/**  
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


#ifndef GRAPHLAB_DC_DIST_OBJECT_BASE_HPP
#define GRAPHLAB_DC_DIST_OBJECT_BASE_HPP
#include <vector>
#include <rpc/dc_internal_types.hpp>
namespace graphlab {

namespace dc_impl {
/**
 * \ingroup rpc
 * \internal
Provides an interface for extracting and updating counters from dc_dist_objects
*/
class dc_dist_object_base{
 public:

  virtual ~dc_dist_object_base() { } 

  /// Increment the number of calls sent from this object
  virtual void inc_calls_sent(procid_t source) = 0;
  /// Increment the number of calls received by this object
  virtual void inc_calls_received(procid_t dest) = 0;
  
  /// Increment the number of bytes sent from this object
  virtual void inc_bytes_sent(procid_t target, size_t bytes) = 0;

  /// Return the number of calls received by this object
  virtual size_t calls_received() const = 0;
  /// Return the number of calls sent from this object
  virtual size_t calls_sent() const = 0;
};

}
}

#endif

