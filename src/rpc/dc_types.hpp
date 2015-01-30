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


#ifndef DISTRIBUTED_CONTROL_TYPES_HPP
#define DISTRIBUTED_CONTROL_TYPES_HPP
#include <inttypes.h>
#include <serialization/iarchive.hpp>
namespace graphlab {
  /// The type used for numbering processors \ingroup rpc
  typedef uint16_t procid_t;

  /**
   * \internal
   * \ingroup rpc
   * The underlying communication protocol
   */
  enum dc_comm_type {
    TCP_COMM,   ///< TCP/IP
    SCTP_COMM   ///< SCTP (limited support)
  };


  /**
   * \internal
   * \ingroup rpc
   * A pointer that points directly into
   * the middle of a deserialized buffer.
   */
  struct wild_pointer {
    const void* ptr;

    void load(iarchive& iarc) {
      assert(iarc.buf != NULL);
      ptr = reinterpret_cast<const void*>(iarc.buf + iarc.off);
    }
  };
};
#include <rpc/dc_packet_mask.hpp>
#endif

