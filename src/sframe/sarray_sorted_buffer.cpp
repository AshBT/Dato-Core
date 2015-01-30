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
#include<sframe/sarray_sorted_buffer.hpp>
#include<sframe/sarray_reader_buffer.hpp>
#include<unordered_set>
#include<queue>
#include <util/cityhash_gl.hpp>

namespace graphlab {

template<typename T>
sarray_sorted_buffer<T>::sarray_sorted_buffer(
    size_t buffer_size,
    comparator_type comparator,
    bool deduplicate)
  : buffer_size(buffer_size),
    comparator(comparator),
    deduplicate(deduplicate) {

    sink = std::make_shared<sink_type>();
    sink->open_for_write(1);
    out_iter = sink->get_output_iterator(0);
    buffer.reserve(buffer_size);
  }

template<typename T>
void sarray_sorted_buffer<T>::add(const value_type& val) {
  buffer_mutex.lock();
  buffer.push_back(val);
  if (buffer.size() == buffer_size) {
    std::vector<value_type> swap_buffer;
    swap_buffer.swap(buffer);
    buffer_mutex.unlock();
    save_buffer(swap_buffer);
  } else {
    buffer_mutex.unlock();
  }
}

template<typename T>
void sarray_sorted_buffer<T>::add(value_type&& val) {
  buffer_mutex.lock();
  buffer.push_back(val);
  if (buffer.size() == buffer_size) {
    std::vector<value_type> swap_buffer;
    swap_buffer.swap(buffer);
    buffer_mutex.unlock();
    save_buffer(swap_buffer);
  } else {
    buffer_mutex.unlock();
  }
}

template<typename T>
void sarray_sorted_buffer<T>::close() {
  if (sink->is_opened_for_write()){
    if (buffer.size() > 0) {
      save_buffer(buffer);
      buffer.clear();
      buffer.shrink_to_fit();
    }
    sink->close();
  }
}


/// Save the buffer into the sarray backend.
template<typename T>
void sarray_sorted_buffer<T>::save_buffer(std::vector<value_type>& swap_buffer) {
  std::sort(swap_buffer.begin(), swap_buffer.end(), comparator);
  if (deduplicate) {
    auto iter = std::unique(swap_buffer.begin(), swap_buffer.end());
    swap_buffer.resize(std::distance(swap_buffer.begin(), iter));
  }
  sink_mutex.lock();
  auto iter = swap_buffer.begin();
  while(iter != swap_buffer.end()) {
    *out_iter = *iter;
    ++iter;
    ++out_iter;
  }
  chunk_size.push_back(swap_buffer.size());
  sink_mutex.unlock();
}

// Explicit template class instantiation
template class sarray_sorted_buffer<flexible_type>;
} // end of graphlab
