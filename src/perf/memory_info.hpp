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

#ifndef GRAPHLAB_MEMORY_INFO_HPP
#define GRAPHLAB_MEMORY_INFO_HPP

#include <string>
#include <cstdint>
#include <iostream>
#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif
#include <logger/assertions.hpp>
namespace graphlab {
  /**
   * \internal \brief Memory info namespace contains functions used to
   * compute memory usage.
   *
   * The memory info functions require TCMalloc to actually compute
   * memory usage values. If TCMalloc is not present then calls to
   * memory info will generate warnings and return the default value.
   */
  namespace memory_info {

    /**
     * \internal
     *
     * \brief Returns whether memory info reporting is
     * available on this system (if memory_info was built with TCMalloc)
     *
     * @return if memory info is available on this system.
     */
    inline bool available() {
#ifdef HAS_TCMALLOC
      return true;
#else
      return false;
#endif
    }

    /**
     * \internal
     * 
     * \brief Estimates the total current size of the memory heap in
     * bytes. If memory info is not available then 0 is returned.
     *
     * @return size of heap in bytes
     */
    inline size_t heap_bytes() {
      size_t heap_size(0);
#ifdef HAS_TCMALLOC
      MallocExtension::instance()->
        GetNumericProperty("generic.heap_size", &heap_size);
#else
      logstream(LOG_WARNING) <<
        "memory_info::heap_bytes() requires tcmalloc" << std::endl;
#endif
      return heap_size;
    }

    /**
     * \internal
     *
     * \brief Determines the total number of allocated bytes.  If
     * memory info is not available then 0 is returned.
     *
     * @return the total bytes allocated 
     */
    inline size_t allocated_bytes() {
      size_t allocated_size(0);
#ifdef HAS_TCMALLOC
      MallocExtension::instance()->
        GetNumericProperty("generic.current_allocated_bytes",
                           &allocated_size);
#else
      logstream_once(LOG_WARNING) <<
        "memory_info::allocated_bytes() requires tcmalloc" << std::endl;
#endif
      return allocated_size;
    }

    /**
     * \internal
     * 
     * \brief Print a memory usage summary prefixed by the string
     * argument.
     *
     * @param [in] label the string to print before the memory usage summary.
     */
    inline void print_usage(const std::string& label = "") {
#ifdef HAS_TCMALLOC
        const double BYTES_TO_MB = double(1) / double(1024 * 1024);
        std::cerr
          << "Memory Info: " << label << std::endl
          << "\t Heap: " << (heap_bytes() * BYTES_TO_MB) << " MB"
          << std::endl
          << "\t Allocated: " << (allocated_bytes() * BYTES_TO_MB) << " MB"
          << std::endl;
#else
        logstream_once(LOG_WARNING)
          << "Unable to print memory info for: " << label << ". "
          << "No memory extensions api available." << std::endl;
#endif
    }

    /**
     * \internal
     * 
     * \brief Log a memory usage summary prefixed by the string
     * argument.
     *
     * @param [in] label the string to print before the memory usage summary.
     */
    inline void log_usage(const std::string& label = "") {
#ifdef HAS_TCMALLOC
        const double BYTES_TO_MB = double(1) / double(1024 * 1024);
        logstream(LOG_INFO)
          << "Memory Info: " << label
          << "\n\t Heap: " << (heap_bytes() * BYTES_TO_MB) << " MB"
          << "\n\t Allocated: " << (allocated_bytes() * BYTES_TO_MB) << " MB"
          << std::endl;
#else
        logstream_once(LOG_WARNING)
          << "Unable to print memory info for: " << label << ". "
          << "No memory extensions api available." << std::endl;
#endif
    }
  } // end of namespace memory info
};

#endif


