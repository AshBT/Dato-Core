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
#ifndef CACHE_STREAM_SINK_HPP
#define CACHE_STREAM_SINK_HPP

#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <fileio/general_fstream_sink.hpp>
#include <fileio/fixed_size_cache_manager.hpp>

namespace graphlab {
namespace fileio_impl {

/**
 * \internal
 *
 * A boost::iostreams::sink concept implemented using cache_block as the underlying
 * sink device.
 */
class cache_stream_sink {

 typedef fileio::cache_id_type cache_id_type;

 public:
  typedef char        char_type;
  struct category: public boost::iostreams::sink_tag,
  boost::iostreams::closable_tag,
  boost::iostreams::multichar_tag {};

  /**
   * Construct the sink from a cache_id.
   *
   * Intialize the underlying data sink, either the in memory array
   * or the on disk cache file.
   */
  explicit cache_stream_sink(cache_id_type cache_id);

  /// Destructor. CLoses the stream.
  ~cache_stream_sink();

  /**
   * Attempts to write bufsize bytes into the stream from the buffer.
   * Returns the actual number of bytes written. Returns -1 on failure.
   */
  std::streamsize write(const char* c, std::streamsize bufsize);

  /**
   * Returns true if the file is opened
   */
  bool is_open() const;

  /**
   * Closes all file handles
   */
  void close();

  /**
   * Seeks to a different location. Will fail on compressed files.
   */
  std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

  /**
   * Returns true if the stream is good. See std::ios_base
   */
  bool good() const;

  /**
   * Returns true if the stream is bad. See std::ios_base
   */
  bool bad() const;

  /**
   * Returns true if a stream operation failed. See std::ios_base
   */
  bool fail() const;

  /**
   * Returns the number of bytes written to disk so far. Due to file 
   * compression and buffering this can be very different from how many bytes
   * were wrtten to the stream.
   */
  size_t get_bytes_written() const;


 private:
   fileio::fixed_size_cache_manager& cache_manager;
   std::shared_ptr<fileio::cache_block> out_block;
   std::shared_ptr<general_fstream_sink> out_file;
};


} // end of fileio
} // end of graphlab

#endif
