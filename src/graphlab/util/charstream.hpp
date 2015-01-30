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


#ifndef GRAPHLAB_CHARSTREAM
#define GRAPHLAB_CHARSTREAM

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/categories.hpp>

namespace graphlab {

  /// \ingroup util_internal
  namespace charstream_impl {
    /// \ingroup util_internal
    template <bool self_deleting>
    struct resizing_array_sink {


      resizing_array_sink(size_t initial = 0) : str(NULL) { 
        if(initial > 0) {
          str = (char*)(malloc(initial));
          assert(str != NULL);
        } 
        len = 0;
        buffer_size = initial;
      }

      resizing_array_sink(const resizing_array_sink& other) :
        len(other.len), buffer_size(other.buffer_size) {
        if(self_deleting) {
          str = (char*)(malloc(other.buffer_size));
          assert(str != NULL);
          memcpy(str, other.str, len);
        } else {
          str = other.str;
        }
      }

      ~resizing_array_sink() {
        if( self_deleting && str != NULL) {
          free((void*)str);
        }        
      }

      /** Gives up the underlying pointer without
       *  freeing it */
      void relinquish() {
        str = NULL;
        len = 0;
        buffer_size = 0;
      }

      size_t size() const { return len; }
      char* c_str() { return str; }
      const char* c_str() const { return str; }

      void clear() {
        len = 0;
      }

      void clear(size_t new_buffer_size) {
        len = 0;
        str = (char*)realloc(str, new_buffer_size);
        buffer_size = new_buffer_size;
      }

      void reserve(size_t new_buffer_size) {
        if (new_buffer_size > buffer_size) {
          str = (char*)realloc(str, new_buffer_size);
          buffer_size = new_buffer_size;
        }
      }
      
      char *str;
      size_t len;
      size_t buffer_size;
      typedef char        char_type;
      struct category: public boost::iostreams::device_tag,
                       public boost::iostreams::output,
                       public boost::iostreams::multichar_tag { };
      
      /** the optimal buffer size is 0. */
      inline std::streamsize optimal_buffer_size() const { return 0; }

      inline std::streamsize advance(std::streamsize n) {
         if (len + n > buffer_size) {
          // double in length if we need more buffer
          buffer_size = 2 * (len + n);
          str = (char*)realloc(str, buffer_size);
          assert(str != NULL);
        }
        len += n;
        return n;
      }
      
      inline std::streamsize write(const char* s, std::streamsize n) {
        if (len + n > buffer_size) {
          // double in length if we need more buffer
          buffer_size = 2 * (len + n);
          str = (char*)realloc(str, buffer_size);
          assert(str != NULL);
        }
        memcpy(str + len, s, n);
        len += n;
        return n;
      }
      
      inline void swap(resizing_array_sink<self_deleting> &other) {
        std::swap(str, other.str);
        std::swap(len, other.len);
        std::swap(buffer_size, other.buffer_size);
      }

    };
    
  }; // end of impl;
  
  
  /**
   * \ingroup util
   * A stream object which stores all streamed output in memory.
   * It can be used like any other stream object.
   * For instance:
   * \code
   *  charstream cstrm;
   *  cstrm << 123 << 10.0 << "hello world" << std::endl;
   * \endcode
   *
   * stream->size() will return the current length of output
   * and stream->c_str() will return a mutable pointer to the string.
   */
  typedef boost::iostreams::stream< charstream_impl::resizing_array_sink<true> > 
  charstream;


}; // end of namespace graphlab
#endif

