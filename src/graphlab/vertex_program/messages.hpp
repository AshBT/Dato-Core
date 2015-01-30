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

#ifndef GRAPHLAB_MESSAGES_HPP
#define GRAPHLAB_MESSAGES_HPP

#include <serialization/serialization_includes.hpp>

namespace graphlab {

  namespace messages {

    /**
     * The priority of two messages is the sum
     */
    struct sum_priority : public graphlab::IS_POD_TYPE {
      double value;
      sum_priority(const double value = 0) : value(value) { }
      double priority() const { return value; }
      sum_priority& operator+=(const sum_priority& other) {
        value += other.value;
        return *this;
      }
    }; // end of sum_priority message

    /**
     * The priority of two messages is the max
     */
    struct max_priority : public graphlab::IS_POD_TYPE {
      double value;
      max_priority(const double value = 0) : value(value) { }
      double priority() const { return value; }
      max_priority& operator+=(const max_priority& other) {
        value = std::max(value, other.value);
        return *this;
      }
    }; // end of max_priority message


  }; // end of messages namespace


}; // end of graphlab namespace
#endif
