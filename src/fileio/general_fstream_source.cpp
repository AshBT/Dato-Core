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
#include <logger/assertions.hpp>
#include <fileio/general_fstream_source.hpp>

namespace graphlab {
namespace fileio_impl {


general_fstream_source::general_fstream_source(std::string file) {
  open_file(file, boost::ends_with(file, ".gz"));
}

general_fstream_source::general_fstream_source(std::string file, 
                                               bool gzip_compressed) {
  open_file(file, gzip_compressed);
}

void general_fstream_source::open_file(std::string file, bool gzip_compressed) {
  in_file = std::make_shared<union_fstream>(file, std::ios_base::in);
  is_gzip_compressed = gzip_compressed;
  if (gzip_compressed) {
    decompressor = std::make_shared<boost::iostreams::gzip_decompressor>();
  }
  underlying_stream = in_file->get_istream();
}

bool general_fstream_source::is_open() const {
  return underlying_stream && !underlying_stream->bad();
}

std::streamsize general_fstream_source::read(char* c, std::streamsize bufsize) {
  if (is_gzip_compressed) {
    return decompressor->read(*underlying_stream, c, bufsize);
  } else {
    underlying_stream->read(c, bufsize);
    return underlying_stream->gcount();
  }
}

void general_fstream_source::close() {
  if (decompressor) {
    decompressor->close(*underlying_stream, std::ios_base::in);
    decompressor.reset();
  }
  if (in_file) {
    in_file->close();
    in_file.reset();
  }
  underlying_stream = NULL;
}

std::streampos general_fstream_source::seek(std::streamoff off, 
                                            std::ios_base::seekdir way) {
  if (!decompressor) {
    underlying_stream->clear();
    underlying_stream->seekg(off, way);
    return underlying_stream->tellg();
  } else {
    ASSERT_MSG(false, "Attempting to seek in a compressed file. Fail!");
  }
}

size_t general_fstream_source::file_size() const {
  if (in_file) {
    return in_file->file_size();
  } else {
    return (size_t)(-1);
  }
}


size_t general_fstream_source::get_bytes_read() const {
  if (in_file) {
    return in_file->get_bytes_read();
  } else {
    return (size_t)(-1);
  }
}

} // namespace fileio_impl
} // namespace graphlab
