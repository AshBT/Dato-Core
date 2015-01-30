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
#include <logger/logger.hpp>
#include <fileio/cache_stream_source.hpp>
#include <fileio/fixed_size_cache_manager.hpp>

namespace graphlab {
namespace fileio_impl {


cache_stream_source::cache_stream_source(cache_id_type cache_id) {
  auto& cache_manager = fileio::fixed_size_cache_manager::get_instance();
  in_block = cache_manager.get_cache(cache_id);
  if (in_block->is_pointer()) {
    in_array = in_block->get_pointer();
    array_size = in_block->get_pointer_size();
    array_cur_pos = 0;
  } else {
    in_array = NULL;
    array_size = 0;
    array_cur_pos = 0;
    logstream(LOG_INFO) << "Reading " << cache_id << " from " 
                        << in_block->get_filename() << std::endl;
    in_file = std::make_shared<general_fstream_source>(in_block->get_filename());
  }
}

std::streamsize cache_stream_source::read(char* c, std::streamsize bufsize) {
  if (in_array) {
    size_t bytes_read = std::min<size_t>(bufsize, array_size - array_cur_pos);
    memcpy(c, in_array + array_cur_pos, bytes_read);
    array_cur_pos += bytes_read;
    return bytes_read;
  } else {
    return in_file->read(c, bufsize);
  }
}

void cache_stream_source::close() {
  if (in_file) {
    in_file->close();
  }
}

std::streampos cache_stream_source::seek(std::streamoff off, std::ios_base::seekdir way) {
  if (in_array) {
    std::streampos newpos;
    if (way == std::ios_base::beg) {
      newpos = off;
    } else if (way == std::ios_base::cur) {
      newpos = (std::streampos)(array_cur_pos) + off;
    } else if (way == std::ios_base::end) {
      newpos = array_size + off - 1;
    }

    if (newpos < 0 || newpos >= (std::streampos)array_size) {
      log_and_throw_io_failure("Bad seek. Index out of range.");
    }

    array_cur_pos = newpos;
    return newpos;
  }

  return in_file->seek(off, way);
}

bool cache_stream_source::is_open() const {
  if (in_file) {
    return in_file->is_open();
  }
  return true;
}

size_t cache_stream_source::file_size() const {
  if (in_file) {
    return in_file->file_size();
  }
  return array_size;
}

size_t cache_stream_source::get_bytes_read() const {
  if (in_file) {
    return in_file->get_bytes_read();
  }
  return array_cur_pos;
}

} // fileio_impl
} // graphlab
