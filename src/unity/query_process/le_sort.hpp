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
#include <string>
#include <vector>
#include <algorithm>
#include <logger/logger.hpp>
#include <unity/query_process/lazy_eval_op_base.hpp>
#include <unity/query_process/sort_impl.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sarray.hpp>

namespace graphlab {

/**
* A sort query operator that does sort lazily.
*
* When constructed, the operator points to an sarray of strings which contains
* serialized values of original SFrame, with each segment relatively sorted.
* Each value in the sarray is a string that is stringified version of
*   (sorting_column_values, all_column_values)
*
* Upon asked for rows, the operator does a real sort on each segment and persist
* sorted result. After that, it serves rows to caller as if it is a real sframe

* \param partition_array The serialized version of original sframe that has been
    partitioned to relatively ordered chunks
* \param partition_sorted Indicates whether or not a given partition has already
    been sorted so that there is no need to sort that partition
* \param sort_column_indexes Column indexes for the columns that are sorted
* \param sort_orders Indicating ascending(true)/descending(false) for each
    sort column
* \param column_names The column names of original sframe
* \param column_types The column types of original sframe
**/
class le_sort : public lazy_eval_op_imp_base<std::vector<flexible_type>> {
public:
  le_sort(
    std::shared_ptr<sarray<std::string>> partition_array,
    const std::vector<bool>& partition_sorted,
    const std::vector<size_t>& partition_sizes,
    const std::vector<size_t>& sort_column_indexes,
    const std::vector<bool>& sort_orders,
    const std::vector<std::string>& column_names,
    const std::vector<flex_type_enum>& column_types) :
      lazy_eval_op_imp_base<std::vector<flexible_type>>(
        std::string("sort"),
        false, // this flag doesn't matter here because the sort only takes in the
               // persisted temp sarray that it generated itself, nobody will share
               // the operator anyway
        true), // This flag doesn't matter since it only has one child
      m_column_names(column_names),
      m_column_types(column_types),
      m_partition_sorted(partition_sorted),
      m_partition_sizes(partition_sizes),
      m_sort_column_indexes(sort_column_indexes),
      m_sort_orders(sort_orders),
      m_partition_array(partition_array) {

    m_size = partition_array->size();
    m_sframe_ptr = std::make_shared<sframe>();
  }

  virtual flex_type_enum get_type() const {
    return flex_type_enum::LIST;
  }

   virtual bool has_size() const {
    return true;
  }

   virtual size_t size() const {
    return m_size;
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    // no lazy op here, return empty
    return std::vector<std::shared_ptr<lazy_eval_op_base>>();
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    log_and_throw("le_sort::set_children should never be called!");
  }

  std::shared_ptr<sframe> eager_sort() {
    // materialize the sort result
    timer ti;
    if (!m_sframe_ptr->is_opened_for_read()) sort_and_persist();
    logstream(LOG_INFO) << "Sort step: " << ti.current_time() << std::endl;
    DASSERT_TRUE(m_sframe_ptr);

    return m_sframe_ptr;
  }

protected:
  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_dop = dop;

    // materialize the sort result
    timer ti;
    if (!m_sframe_ptr->is_opened_for_read()) sort_and_persist();
    logstream(LOG_INFO) << "Sort step: " << ti.current_time() << std::endl;
    DASSERT_TRUE(m_sframe_ptr);

    // Tell the materialized sframe the segment partition
    if (segment_sizes.size() == 0) {
      this->compute_chunk_sizes(dop, m_size, m_iterator_begins, m_iterator_ends);
      std::vector<size_t> my_segment_sizes(dop);
      for(size_t i = 0; i < dop; i++) {
        my_segment_sizes[i] = m_iterator_ends[i] - m_iterator_begins[i];
      }
      m_reader = m_sframe_ptr->get_reader(my_segment_sizes);
    } else {
      DASSERT_EQ(segment_sizes.size(), dop);
      this->compute_iterator_locations(segment_sizes, m_iterator_begins, m_iterator_ends);
      DASSERT_EQ(m_iterator_ends.back(), size());
      m_reader = m_sframe_ptr->get_reader(segment_sizes);
    }
  }

  virtual void stop() {
    m_iterator_begins.clear();
    m_iterator_ends.clear();
  }

  virtual size_t skip_rows(size_t segment_id, size_t num_items) {
    DASSERT_TRUE(segment_id < m_dop);
    DASSERT_TRUE(num_items > 0);

    auto iterator_begin = m_iterator_begins[segment_id];
    auto iterator_end = m_iterator_ends[segment_id];
    size_t items_to_skip = std::min(num_items, iterator_end - iterator_begin);
    m_iterator_begins[segment_id] = iterator_begin + items_to_skip;
    DASSERT_TRUE(num_items == items_to_skip);

    return items_to_skip;
  }

  virtual std::vector<std::vector<flexible_type>> get_next(size_t segment_id, size_t num_items) {
    DASSERT_TRUE(segment_id < m_dop);
    DASSERT_TRUE(num_items > 0);

    std::vector<std::vector<flexible_type>> ret;

    auto iterator_begin = m_iterator_begins[segment_id];
    auto iterator_end = m_iterator_ends[segment_id];

    // reached end of chunk, return
    if (iterator_end == iterator_begin) return ret;

    // read the rows
    size_t items_to_read = std::min(num_items, iterator_end - iterator_begin);
    size_t items_read = m_reader->read_rows(iterator_begin, iterator_begin + items_to_read, ret);

    // adjust iterator
    m_iterator_begins[segment_id] = iterator_begin + items_read;

    return ret;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    auto cloned = std::make_shared<le_sort>(
      m_partition_array,
      m_partition_sorted,
      m_partition_sizes,
      m_sort_column_indexes,
      m_sort_orders,
      m_column_names,
      m_column_types);

    cloned->m_sframe_ptr = m_sframe_ptr;
    return cloned;
  };

private:
  // Sort and persist each sframe chunk
  void sort_and_persist() {
    DASSERT_TRUE(!m_sframe_ptr->is_opened_for_read() && !m_sframe_ptr->is_opened_for_write());

    size_t num_segments = m_partition_array->num_segments();

    m_sframe_ptr->open_for_write(m_column_names, m_column_types, "", num_segments);

    auto reader = m_partition_array->get_reader();

    atomic<size_t> next_segment_to_sort = 0;
    graphlab::mutex mem_used_mutex;
    graphlab::conditional mem_threshold_cv;
    size_t mem_used = 0;

    // Multithreaded sort implementation. 
    //
    // High-level algorithm understanding: Each segment's sort key range is
    // disjoint from all other segments, but the keys are unsorted with the
    // segment.  This allows us to sort multiple segments at once and output
    // the sorted ranges in segment order.
    //
    // Implementation: This is designed so that in the ideal case, N segments
    // will be able to fit in the buffer we're given. Threads wait on the
    // condition that there is enough memory for the segment they are assigned.
    // If a thread's segment is too large for our buffer, that thread must wait
    // until no other threads are running, and then it will take up the whole
    // buffer to sort...hopefully not allocating too much memory :/. 
    parallel_for(0, thread::cpu_count(),
     [&](size_t thread_id) {

      // Each thread keep running until no more segment to sort
      std::vector<std::vector<flexible_type>> rows;
      while(true) {
        size_t segment_id = next_segment_to_sort++;
        if (segment_id >= num_segments) break;

        if (m_partition_sorted[segment_id]) {
          logstream(LOG_INFO) << "segment " << segment_id << " is already sorted, skip sorting " << std::endl;
          write_one_chunk(reader, segment_id, m_sframe_ptr->get_output_iterator(segment_id));
        } else {
          mem_used_mutex.lock();
          while((mem_used+m_partition_sizes[segment_id]) > sframe_config::SFRAME_SORT_BUFFER_SIZE) {
            if(((m_partition_sizes[segment_id] > sframe_config::SFRAME_SORT_BUFFER_SIZE) &&
                    (mem_used == 0)) ||
              (m_partition_sizes[segment_id] == 0)) {
              break;
            }
            mem_threshold_cv.wait(mem_used_mutex);
          }
          //logstream(LOG_INFO) << "sorting segment " << segment_id << " in thread " << thread_id << std::endl;
          mem_used += m_partition_sizes[segment_id];
          mem_used_mutex.unlock();
          read_one_chunk(reader, segment_id, rows);

          sort_one_chunk(rows);

          write_one_chunk(rows, m_sframe_ptr->get_output_iterator(segment_id));
          rows.clear();
          rows.shrink_to_fit();

          mem_used_mutex.lock();
          mem_used -= m_partition_sizes[segment_id];
          mem_threshold_cv.signal();
          mem_used_mutex.unlock();
        }
      }
    });

    m_sframe_ptr->close();
  }

  void read_one_chunk(
    std::unique_ptr<sarray_reader<std::string>>& reader,
    size_t segment_id,
    std::vector<std::vector<flexible_type>>& rows) {

    rows.resize(reader->segment_length(segment_id));

    size_t row_idx = 0;
    size_t num_columns = m_column_names.size();  // there is one key column
    std::vector<flexible_type> row;
    for(auto iter = reader->begin(segment_id); iter != reader->end(segment_id); ++iter) {
      row.resize(num_columns);

      // unpack the string to original vector format
      iarchive iarc((*iter).c_str(), (*iter).length());
      for(auto row_it = row.begin(); row_it != row.end(); ++row_it) {
        iarc >> *row_it;
      }

      rows[row_idx++] = std::move(row);
    }
  }

  void write_one_chunk(
    std::unique_ptr<sarray_reader<std::string>>& reader,
    size_t segment_id,
    sframe_output_iterator output_iterator) {

    size_t num_columns = m_column_names.size();  // there is one key column
    std::vector<flexible_type> row(num_columns);
    for(auto iter = reader->begin(segment_id); iter != reader->end(segment_id); ++iter) {

      // unpack the string to original vector format
      iarchive iarc((*iter).c_str(), (*iter).length());
      for(auto row_it = row.begin(); row_it != row.end(); ++row_it) {
        iarc >> *row_it;
      }

      // Note first value is the key, that we will NOT write to output
      std::vector<flexible_type> output;
      std::move(row.begin(), row.end(), std::back_inserter(output));

      *output_iterator = std::move(output);
      output_iterator++;
    }
  }

  void write_one_chunk(
    std::vector<std::vector<flexible_type>>& rows,
    sframe_output_iterator output_iterator) {

    for(auto& row : rows) {
      std::vector<flexible_type> output;
      std::move(row.begin(), row.end(), std::back_inserter(output));

      *output_iterator = std::move(output);
      output_iterator++;
    }
  }

  void sort_one_chunk(std::vector<std::vector<flexible_type>>& rows) {
    std::sort(rows.begin(), rows.end(), sframe_sort_impl::less_than_partial_function(m_sort_column_indexes, m_sort_orders));
  }

  std::vector<std::string> m_column_names;
  std::vector<flex_type_enum> m_column_types;
  std::vector<bool> m_partition_sorted;
  std::vector<size_t> m_partition_sizes;
  std::vector<size_t> m_sort_column_indexes;
  std::vector<bool> m_sort_orders;
  size_t m_size;

  // this is used before the sort is materialized
  std::shared_ptr<sarray<std::string>> m_partition_array;

  // this is used after the sort is materialized
  std::shared_ptr<sframe> m_sframe_ptr;

  // ----------------------------------------------
  // --           run time house keeping        ---
  // ----------------------------------------------
  size_t m_dop;
  std::shared_ptr<sframe_reader> m_reader;
  std::vector<size_t> m_iterator_begins;
  std::vector<size_t> m_iterator_ends;

};
}
