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
#include <boost/algorithm/string.hpp>
#include <fileio/general_fstream_sink.hpp>

namespace graphlab {
namespace fileio_impl {


general_fstream_sink::general_fstream_sink(std::string file) {
  open_file(file, boost::ends_with(file, ".gz"));
}

general_fstream_sink::general_fstream_sink(std::string file, 
                                               bool gzip_compressed) {
  open_file(file, gzip_compressed);
}

void general_fstream_sink::open_file(std::string file, bool gzip_compressed) {
  out_file = std::make_shared<union_fstream>(file, std::ios_base::out);
  is_gzip_compressed = gzip_compressed;
  if (gzip_compressed) {
    compressor = std::make_shared<boost::iostreams::gzip_compressor>();
  }
  // get the underlying stream inside the union stream
  underlying_stream = out_file->get_ostream();
}

bool general_fstream_sink::is_open() const {
  return underlying_stream && !underlying_stream->bad();
}

std::streamsize general_fstream_sink::write(const char* c, 
                                            std::streamsize bufsize) {
  if (is_gzip_compressed) {
    return compressor->write(*underlying_stream, c, bufsize);
  } else {
    underlying_stream->write(c, bufsize);
    if (underlying_stream->fail()) return 0;
    else return bufsize;
  }
}

general_fstream_sink::~general_fstream_sink() {
  // if I am the only reference to the object, close it.
  if (out_file && out_file.unique()) close();
}

void general_fstream_sink::close() {
  if (compressor) {
    compressor->close(*underlying_stream, std::ios_base::out);
    compressor.reset();
  }
  if (out_file) {
    try {
      out_file->close();
    } catch (...) {
      out_file.reset();
      underlying_stream = NULL;
      throw;
    }
    out_file.reset();
  }
  underlying_stream = NULL;
}


bool general_fstream_sink::good() const {
  return underlying_stream && underlying_stream->good();
}

bool general_fstream_sink::bad() const {
  // if stream is NULL. the stream is bad
  if (underlying_stream == NULL) return true;
  return underlying_stream->bad();
}

bool general_fstream_sink::fail() const {
  // if stream is NULL. the stream is bad
  if (underlying_stream == NULL) return true;
  return underlying_stream->fail();
}

size_t general_fstream_sink::get_bytes_written() const {
  if (out_file) {
    return out_file->get_bytes_written();
  } else {
    return (size_t)(-1);
  }
}

} // namespace fileio_impl
} // namespace graphlab
