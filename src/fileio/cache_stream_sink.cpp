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
#include <fileio/cache_stream_sink.hpp>

namespace graphlab {
namespace fileio_impl {

cache_stream_sink::cache_stream_sink(cache_id_type cache_id) :
  cache_manager(fileio::fixed_size_cache_manager::get_instance()),
  out_block(cache_manager.new_cache(cache_id)) {
  if (out_block->is_file()) {
    logstream(LOG_DEBUG) << "Wrting " << cache_id << " from " 
                        << out_block->get_filename() << std::endl;
    out_file = std::make_shared<general_fstream_sink>(out_block->get_filename());
  }
}

cache_stream_sink::~cache_stream_sink() {
  close();
}

std::streamsize cache_stream_sink::write (const char* c, std::streamsize bufsize) {
  if (out_file) {
    return out_file->write(c, bufsize);
  } else {
    bool write_success = out_block->write_bytes_to_memory_cache(c, bufsize);
    if (write_success) {
      return bufsize;
    } else {
      // In memory cache is full, write out to disk.
      // switch to a file handle
      out_file = out_block->write_to_file();
      return out_file->write(c, bufsize);
    }
  }
}

void cache_stream_sink::close() {
  if (out_file) {
    out_file->close();
  }
}

bool cache_stream_sink::is_open() const {
  if (out_file) {
    return out_file->is_open();
  } else {
    return out_block->get_pointer() != NULL;
  }
}

bool cache_stream_sink::good() const {
  if (out_file) {
    return out_file->good();
  } else {
    return out_block->get_pointer() != NULL;
  }
}

bool cache_stream_sink::bad() const {
  if (out_file) {
    return out_file->bad();
  } else {
    return out_block->get_pointer() == NULL;
  }
}

bool cache_stream_sink::fail() const {
  if (out_file) {
    return out_file->fail();
  } else {
    return out_block->get_pointer() == NULL;
  }
}

size_t cache_stream_sink::get_bytes_written() const {
  if (out_file) {
    return out_file->get_bytes_written();
  } else {
    return out_block->get_pointer_size();
  }
}

} // fileio_impl
} // graphlab
