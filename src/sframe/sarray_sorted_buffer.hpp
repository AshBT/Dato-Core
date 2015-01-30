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
#ifndef GRAPHLAB_SFRAME_SARRAY_SORTED_BUFFER_HPP
#define GRAPHLAB_SFRAME_SARRAY_SORTED_BUFFER_HPP

#include<mutex>
#include<memory>
#include<vector>
#include<sframe/sarray.hpp>
#include<sframe/sframe.hpp>


namespace graphlab {

/**
 * An SArray backed buffer that stores elements in sorted order.
 *
 * The container has an in memory buffer, and is backed by an sarray segment.
 * When the buffer is full, it is sorted and written into the sarray segment as a sorted chunk.
 *
 * - The add function is used to push element into the buffer.
 * - When finishing adding elements, close() can be called to close the buffer.
 * - The sort_and_write function then merges the sorted chunks and output to the destination array.
 * - When deduplicate is set in the constructor, the buffer will ignore duplicated items.
 */

template<typename T>
class sarray_sorted_buffer {
  typedef T value_type;
  typedef typename sarray<T>::iterator sink_iterator_type;
  typedef sarray<T> sink_type;
  typedef std::function<bool(const value_type&, const value_type&)> comparator_type;

 public:
   /// construct with given sarray and the segmentid as sink.
   sarray_sorted_buffer(size_t buffer_size,
                        comparator_type comparator,
                        bool deduplicate = false);

   sarray_sorted_buffer(const sarray_sorted_buffer& other) = delete;

   sarray_sorted_buffer& operator=(const sarray_sorted_buffer& other) = delete;

   /// Add a new element to the container.
   void add(const value_type& val);
   void add(value_type&& val);

   /// Sort all elements in the container and writes to the output.
   /// If deduplicate is true, only output unique elements.
   template<typename OutIterator>
   void sort_and_write(OutIterator out);

   size_t approx_size() const {
     if (sink->is_opened_for_write()){
       return 0;
     } else {
       size_t ret = 0;
       for (auto i : chunk_size) ret += i;
       return ret;
     }
   }

   /// Flush the last buffer, and close the sarray.
   void close();

  private:
   /// Writes the content into the sarray segment backend.
   void save_buffer(std::vector<value_type>& swap_buffer);

   /// The sarray storing the elements.
   std::shared_ptr<sink_type> sink;

   /// Internal output iterator for the sarray_sink segment.
   sink_iterator_type out_iter;

   /// Storing the size of each sorted chunk.
   std::vector<size_t> chunk_size;

   /// Guarding the sarray sink from parallel access.
   graphlab::mutex sink_mutex;

   /// Buffer that stores the incoming elements.
   std::vector<value_type> buffer;

   /// The limit of the buffer size.
   size_t buffer_size;

   /// Guarding the buffer from parallel access.
#ifdef  __APPLE__
   simple_spinlock buffer_mutex;
#else
   graphlab::mutex buffer_mutex;
#endif

   /// Comparator for sorting the values.
   comparator_type  comparator;

   /// If true only keep the unique items.
   bool deduplicate;
}; // end of sarray_sorted_buffer class


/**************************************************************************/
/*                                                                        */
/*                             Implementation                             */
/*                                                                        */
/**************************************************************************/
template<typename T>
template<typename OutIterator>
void sarray_sorted_buffer<T>::sort_and_write(OutIterator out) {
  DASSERT_EQ(buffer.size(), 0);
  std::shared_ptr<typename sink_type::reader_type> reader = std::move(sink->get_reader());
  // prepare the begin row and end row for each chunk.
  size_t segment_start = 0;

  // each chunk stores a sequential read of the segments,
  // and elements in each chunk are already sorted.
  std::vector<sarray_reader_buffer<T>> chunk_readers;

  size_t prev_row_start = segment_start;
  for (size_t i = 0; i < chunk_size.size(); ++i) {
    size_t row_start = prev_row_start;
    size_t row_end = row_start + chunk_size[i];
    prev_row_start = row_end;
    chunk_readers.push_back(sarray_reader_buffer<T>(reader, row_start, row_end));
  }

  // id of the chunks that still have elements.
  std::unordered_set<size_t> remaining_chunks;

  // merge the chunks and write to the out iterator
  std::vector< std::pair<value_type, size_t> > pq;
  // comparator for the pair type
  auto pair_comparator = [=](const std::pair<value_type, size_t>& a,
      const std::pair<value_type, size_t>& b) {
    return !comparator(a.first, b.first);
  };

  // insert one element from each chunk into the priority queue.
  for (size_t i = 0; i < chunk_readers.size(); ++i) {
    if (chunk_readers[i].has_next()) {
      pq.push_back({chunk_readers[i].next(), i});
      remaining_chunks.insert(i);
    }
  }
  std::make_heap(pq.begin(), pq.end(), pair_comparator);

  bool is_first_elem = true;
  value_type prev_value;
  while (!pq.empty()) {
    size_t id;
    value_type value;
    std::tie(value, id) = pq.front();
    std::pop_heap(pq.begin(), pq.end(), pair_comparator);
    pq.pop_back();
    if (deduplicate) {
      if ((value != prev_value) || is_first_elem) {
        prev_value = value;
        *out = std::move(value);
        ++out;
        is_first_elem = false;
      }
    } else {
      *out = std::move(value);
      ++out;
    }
    if (chunk_readers[id].has_next()) {
      pq.push_back({chunk_readers[id].next(), id});
      std::push_heap(pq.begin(), pq.end(), pair_comparator);
    } else {
      remaining_chunks.erase(id);
    }
  }

  // At most one chunk will be left
  ASSERT_TRUE(remaining_chunks.size() <= 1);
  if (remaining_chunks.size()) {
    size_t id = *remaining_chunks.begin();
    while(chunk_readers[id].has_next()) {
      value_type value = chunk_readers[id].next();
      if (deduplicate) {
        if ((value != prev_value) || is_first_elem) {
          prev_value = value;
          *out = std::move(value);
          ++out;
          is_first_elem = false;
        }
      } else {
        *out = std::move(value);
        ++out;
      }
    }
  }
}

}
#endif
