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
#ifndef CACHE_STREAM_SOURCE_HPP
#define CACHE_STREAM_SOURCE_HPP

#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <fileio/general_fstream_source.hpp>
#include <fileio/fixed_size_cache_manager.hpp>

namespace graphlab {
namespace fileio_impl {

/**
 * \internal
 *
 * A boost::iostreams::input_seekable concept implemented using cache_block as the underlying
 * source device.
 */
class cache_stream_source {

 typedef fileio::cache_id_type cache_id_type;

 public:
  typedef char        char_type;
  struct category: public boost::iostreams::device_tag,
  boost::iostreams::closable_tag,
  boost::iostreams::multichar_tag,
  boost::iostreams::input_seekable {};

  /**
   * Construct the source from a cache_id.
   *
   * Intialize the underlying datasources, either the in memory array
   * or the on disk cache file.
   */
  explicit cache_stream_source(cache_id_type cache_id);

  /**
   * Attempts to read bufsize bytes into the buffer provided.
   * Returns the actual number of bytes read.
   */
  std::streamsize read(char* c, std::streamsize bufsize);

  /**
   * Closes all file handles.
   */
  void close();

  /**
   * Returns true if the stream is opened.
   */
  bool is_open() const;

  /**
   * Seeks to a different location. Will fail on compressed files.
   */
  std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

  /**
   * Returns the file size of the opened file.
   * Returns (size_t)(-1) if there is no file opened, or if there is an
   * error obtaining the file size.
   */
  size_t file_size() const;

  /**
   * Returns the number of bytes read from disk so far. Due to file 
   * compression and buffering this can be very different from how many bytes
   * were read from the stream.
   */
  size_t get_bytes_read() const;

 private:
  char* in_array;
  size_t array_size;
  size_t array_cur_pos;
  std::shared_ptr<fileio::cache_block> in_block;
  std::shared_ptr<general_fstream_source> in_file;
};


} // end of fileio
} // end of graphlab

#endif
