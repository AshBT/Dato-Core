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
#ifndef GRAPHLAB_UNITY_LE_IMP
#define GRAPHLAB_UNITY_LE_IMP

#include <limits>
#include <logger/logger.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>
#include <unity/query_process/lazy_eval_op_base.hpp>
#include <lambda/lualambda_master.hpp>
#include <lambda/pylambda_master.hpp>

namespace graphlab {

/**
 * Helper function to convert flexible_type value to expected type.
 */
inline flexible_type convert_value_to_output_type(const flexible_type& val, flex_type_enum type) {

  if (val.get_type() == type ||
      val.get_type() == flex_type_enum::UNDEFINED ||
      type == flex_type_enum::UNDEFINED) {
    return val;
  } else if (flex_type_is_convertible(val.get_type(), type)) {
    flexible_type res(type);
    res.soft_assign(val);
    return res;
  } else {
    std::string message = "Cannot convert " + std::string(val) + " to " + flex_type_enum_to_name(type);
    logstream(LOG_ERROR) <<  message << std::endl;
    throw(cppipc::bad_cast(message));
  }
}

/**
 * This iterator gives out the same constant value when asked for
 **/
class le_constant : public lazy_eval_op_imp_base<flexible_type> {
public:
  le_constant(flexible_type value, size_t size):
      lazy_eval_op_imp_base<flexible_type>(std::string("constant"), false) {
    m_value = value;
    m_size = size;
  }

  virtual flex_type_enum get_type() const {
    return m_value.get_type();
  }

  virtual bool has_size() const {
    return true;
  }

  virtual size_t size() const {
    return m_size;
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> empty_vector;
    return empty_vector;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    log_and_throw("this should never be called!");
  }

protected:

  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    if (segment_sizes.empty()) {
      this->compute_chunk_sizes(dop, size(), m_iterator_begins, m_iterator_ends);
    } else {
      DASSERT_EQ(segment_sizes.size(), dop);
      this->compute_iterator_locations(segment_sizes, m_iterator_begins, m_iterator_ends);
      DASSERT_EQ(m_iterator_ends.back(), size());
    }
  }

  virtual void stop() {
    m_iterator_begins.clear();
    m_iterator_ends.clear();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];

    size_t rows_to_skip = std::min(num_items, iterator_begin - iterator_end);
    m_iterator_begins[segment_index] = iterator_begin + rows_to_skip;
    return rows_to_skip;
  }

  virtual std::vector<flexible_type> get_next(size_t segment_index, size_t num_items) {
    auto return_value = std::vector<flexible_type>();
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];

    logstream(LOG_DEBUG) << "get_next begin " << iterator_begin << " , end: "
                         << iterator_end << " segment_index: " << segment_index
                         << std::endl;

    // nothing to read, reach end of chunk
    if (iterator_end == iterator_begin) {
      return return_value;
    }

    size_t items_to_read = std::min(num_items, iterator_end - iterator_begin);
    return_value.resize(items_to_read);

    for(size_t i = 0; i < items_to_read; i++) {
      return_value[i] = m_value;
    }

    // adjust the begin iterator so that next time we read from the correct place
    m_iterator_begins[segment_index] = iterator_begin + items_to_read;

    return return_value;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_constant>(m_value, m_size);
  };

private:

  flexible_type m_value;
  size_t m_size;

  std::vector<size_t> m_iterator_begins;
  std::vector<size_t> m_iterator_ends;
};





/**
 * This iterator gives out either 0 on 1 depending on a random seed
 **/
class le_random: public lazy_eval_op_imp_base<flexible_type> {
public:
  le_random(double percent, int seed, size_t size):
      lazy_eval_op_imp_base<flexible_type>(std::string("random"), false) {
    m_boundary = (double)percent * std::numeric_limits<uint64_t>::max();
    m_percent = percent;
    m_seed = seed;
    m_size = size;
  }

  virtual flex_type_enum get_type() const {
    return flex_type_enum::INTEGER;
  }

  virtual bool has_size() const {
    return true;
  }

  virtual size_t size() const {
    return m_size;
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> empty_vector;
    return empty_vector;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    log_and_throw("this should never be called!");
  }

protected:

  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    if (segment_sizes.empty()) {
      this->compute_chunk_sizes(dop, size(), m_iterator_begins, m_iterator_ends);
    } else {
      DASSERT_EQ(segment_sizes.size(), dop);
      this->compute_iterator_locations(segment_sizes, m_iterator_begins, m_iterator_ends);
      DASSERT_EQ(m_iterator_ends.back(), size());
    }
  }

  virtual void stop() {
    m_iterator_begins.clear();
    m_iterator_ends.clear();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];

    size_t rows_to_skip = std::min(num_items, iterator_begin - iterator_end);
    m_iterator_begins[segment_index] = iterator_begin + rows_to_skip;
    return rows_to_skip;
  }

  virtual std::vector<flexible_type> get_next(size_t segment_index, size_t num_items) {
    auto return_value = std::vector<flexible_type>();
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];

    logstream(LOG_DEBUG) << "get_next begin " << iterator_begin << " , end: "
                         << iterator_end << " segment_index: " << segment_index
                         << std::endl;

    // nothing to read, reach end of chunk
    if (iterator_end == iterator_begin) {
      return return_value;
    }

    size_t items_to_read = std::min(num_items, iterator_end - iterator_begin);
    return_value.resize(items_to_read);

    for(size_t i = 0; i < items_to_read; i++) {
      uint64_t hashval = hash64_combine(iterator_begin + i, m_seed);
      return_value[i] = hashval <= m_boundary;
    }

    // adjust the begin iterator so that next time we read from the correct place
    m_iterator_begins[segment_index] = iterator_begin + items_to_read;

    return return_value;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_random>(m_percent, m_seed, m_size);
  };

private:

  uint64_t m_boundary; // random values below here is 1, above is 0
  int m_seed;
  size_t m_size;
  double m_percent;

  std::vector<size_t> m_iterator_begins;
  std::vector<size_t> m_iterator_ends;
};



/**
  * Parallel iterator that supports consuming from a vector of parallel iterators and emit
  * vector of values
**/
class le_sframe : public lazy_eval_op_imp_base<std::vector<flexible_type>> {
public:
  le_sframe(std::vector<std::shared_ptr<lazy_eval_op_imp_base<flexible_type>>> lazy_operators):
      lazy_eval_op_imp_base<std::vector<flexible_type>>(std::string("sframe"), false) {
    m_sources = lazy_operators;
  }

  ~le_sframe() {
    m_iterators.clear();
    m_sources.clear();
  }

  virtual flex_type_enum get_type() const {
    return flex_type_enum::LIST;
  }

  virtual bool has_size() const {
    if (m_sources.size() == 0) {
      return true;
    } else {
      return m_sources[0]->has_size();
    }
  }

  virtual size_t size() const {
    if (!has_size()) {
      log_and_throw("Cannot get size of a lazy operator!");
    }

    if (m_sources.size() == 0) {
      return 0;
    } else {
      return m_sources[0]->size();
    }
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children;
    for(const auto& source : m_sources) {
      children.push_back(source);
    }
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    m_sources.resize(children.size());
    for(size_t i = 0; i < children.size(); i++) {
      m_sources[i] = std::dynamic_pointer_cast<lazy_eval_op_imp_base<flexible_type>>(children[i]);
    }
  }

protected:
  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_iterators.resize(m_sources.size());

    for(size_t i = 0; i < m_sources.size(); i++) {
      m_iterators[i] = parallel_iterator<flexible_type>::create(m_sources[i], dop, segment_sizes);
    }
  }

  virtual void stop() {
    m_iterators.clear();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    size_t rows_skipped = 0;
    for (size_t i = 0; i < m_iterators.size(); ++i) {
      size_t rows = m_iterators[i]->skip_rows(segment_index, num_items);

      if (i == 0) {
        rows_skipped = rows;
      } else {
        DASSERT_MSG(rows_skipped == rows, "Number of rows skipped should be the same");
      }
    }

    return rows_skipped;
  }

  virtual std::vector<std::vector<flexible_type>> get_next(size_t segment_index, size_t num_items) {
    std::vector<std::vector<flexible_type>> rows;
    std::vector<flexible_type> one_column;
    size_t num_cols =  m_iterators.size();
    for (size_t i = 0; i < num_cols; ++i) {
      one_column = m_iterators[i]->get_next(segment_index, num_items);

      if (i == 0) {
        rows.resize(one_column.size(), std::vector<flexible_type>(num_cols));
      }

      ASSERT_EQ(rows.size(), one_column.size());
      for (size_t j = 0;j < one_column.size(); ++j) {
        rows[j][i] = std::move(one_column[j]);
      }
    }

    return rows;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_sframe>(m_sources);
  };

private:
  std::vector<std::shared_ptr<lazy_eval_op_imp_base<flexible_type>>> m_sources;
  std::vector<std::unique_ptr<parallel_iterator<flexible_type>>> m_iterators;
};

/**
  * provide parallel block reader interface on top of an actual SArray object
**/
template<typename T>
class le_sarray : public lazy_eval_op_imp_base<T> {
  public:
    le_sarray(std::shared_ptr<sarray<T>> sarray_ptr):
        lazy_eval_op_imp_base<T>(std::string("sarray"), false) {

      DASSERT_MSG(sarray_ptr, "source cannot be NULL");

      m_source = sarray_ptr;
      m_reader = sarray_ptr->get_reader();
    }

    ~le_sarray() {
      m_reader.reset();
      m_source.reset();
    }

    std::shared_ptr<sarray<T>> get_sarray_ptr() {
      return m_source;
    }

  virtual flex_type_enum get_type() const {
    return m_source->get_type();
  }

  virtual bool has_size() const {
    return true;
  }

  virtual size_t size() const {
    return m_source->size();
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> empty_vector;
    return empty_vector;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    log_and_throw("this should never be called!");
  }

protected:

  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    if (segment_sizes.empty()) {
      this->compute_chunk_sizes(dop, size(), m_iterator_begins, m_iterator_ends);
    } else {
      DASSERT_EQ(segment_sizes.size(), dop);
      this->compute_iterator_locations(segment_sizes, m_iterator_begins, m_iterator_ends);
      DASSERT_EQ(m_iterator_ends.back(), size());
    }
  }

  virtual void stop() {
    m_iterator_begins.clear();
    m_iterator_ends.clear();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];
    size_t items_to_skip = std::min(num_items, iterator_end - iterator_begin);
    m_iterator_begins[segment_index] = iterator_begin + items_to_skip;
    return items_to_skip;
  }

  virtual std::vector<T> get_next(size_t segment_index, size_t num_items) {
    auto return_value = std::vector<T>();
    auto iterator_begin = m_iterator_begins[segment_index];
    auto iterator_end = m_iterator_ends[segment_index];

    logstream(LOG_DEBUG) << "get_next begin " << iterator_begin << " , end: "
                         << iterator_end << " segment_index: " << segment_index
                         << std::endl;

    // nothing to read, reach end of chunk
    if (iterator_end == iterator_begin) {
      return return_value;
    }

    size_t items_to_read = std::min(num_items, iterator_end - iterator_begin);

    logstream(LOG_DEBUG) << "reading " << items_to_read
                         << " items from sarray filer reader, segment_index: " << segment_index << std::endl;
    size_t items_read = m_reader->read_rows(iterator_begin, iterator_begin + items_to_read, return_value);
    logstream(LOG_DEBUG) << "read " << items_read
                         << " items from sarray file for segment_index: " << segment_index << std::endl;

    // adjust the begin iterator so that next time we read from the correct place
    m_iterator_begins[segment_index] = iterator_begin + items_read;

    return return_value;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_sarray<T>>(m_source);
  };

private:
  std::unique_ptr<sarray_reader<T>> m_reader;
  std::vector<size_t> m_iterator_begins;
  std::vector<size_t> m_iterator_ends;
  std::shared_ptr<sarray<T>> m_source;
};


/**
 * Provide parallel block reader interface on a vector of other lazy_eval_op_impl
 * objects.
 **/
template<typename T>
class le_append: public lazy_eval_op_imp_base<T> {
 public:
  le_append(std::shared_ptr<lazy_eval_op_imp_base<T>> first_child,
            std::shared_ptr<lazy_eval_op_imp_base<T>> second_child,
            size_t size):
      lazy_eval_op_imp_base<T>(std::string("append"), true, false) {
        init(first_child, second_child, size);
      }

  ~le_append() { }

  void init(std::shared_ptr<lazy_eval_op_imp_base<T>> first_child,
            std::shared_ptr<lazy_eval_op_imp_base<T>> second_child,
            size_t size) {
    m_left_child = first_child;
    m_right_child = second_child;
    m_size = size;
    m_type = m_left_child->get_type();
    DASSERT_MSG(m_type == m_right_child->get_type(),
                "Error: Children have different types");
  }

  virtual flex_type_enum get_type() const {
    return m_type;
  }

  virtual bool has_size() const {
    return true;
  }

  virtual size_t size() const {
    return m_size;
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> ret{m_left_child, m_right_child};
    return ret;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_EQ(children.size(), 2);
    m_left_child = std::dynamic_pointer_cast<lazy_eval_op_imp_base<T>>(children[0]);
    m_right_child = std::dynamic_pointer_cast<lazy_eval_op_imp_base<T>>(children[1]);
  }


protected:
  /**
   * Assign the children's iterators to the the parallel iteratoring segments.
   * To simplify the logic, we assume that all children has similar size.
   */
  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_left_child_iter.reset();
    m_right_child_iter.reset();
    m_segment_begin_iterators.clear();
    m_segment_end_iterators.clear();

    auto all_iterators = std::make_shared<std::vector<decltype(m_left_child_iter)>>();
    auto iterator_segment_sizes = std::make_shared<std::vector<size_t>>();

    if (dop == 1) {
      // This is the trivial case, we just pack both child into one internal_iterator
      // and consume each child as one segment.
      m_left_child_iter = std::move(parallel_iterator<T>::create(m_left_child, 1));
      m_right_child_iter = std::move(parallel_iterator<T>::create(m_right_child, 1));
      all_iterators->push_back(m_left_child_iter);
      all_iterators->push_back(m_right_child_iter);
      iterator_segment_sizes->resize(2 /* two children */, 1 /*each is one segment*/);
      m_segment_begin_iterators.push_back(internal_iterator(all_iterators, iterator_segment_sizes, 0, 0));
      m_segment_end_iterators.push_back(internal_iterator(all_iterators, iterator_segment_sizes, 2, 0));
    } else {
      // compute the segment chunk sizes using the shared method
      // as all other le operators.
      std::vector<size_t> this_segment_sizes(segment_sizes);
      if (segment_sizes.empty()) {
        std::vector<size_t> start_rows, end_rows;
        this->compute_chunk_sizes(dop, size(), start_rows, end_rows);
        for (size_t i = 0;  i < dop; ++i) {
          this_segment_sizes.push_back(end_rows[i] - start_rows[i]);
        }
      }

      // Next, let's compute the segment sizes for left and right children
      size_t left_child_size = m_left_child->size();
      size_t right_child_size = m_right_child->size();
      std::vector<size_t> left_segment_sizes, right_segment_sizes;

      // A queue which contains pairs of {child_space_available, child_segment_sizes_vec}.
      // The following code will fill left_segment_sizes, and right_segment_sizes
      // such that their "concat" becomes the objective segment_sizes.
      // The "concat" will not be exact, because the last segment of left child
      // and the first segment of right child might be packed into on segment.
      // Example:
      //   segment_sizes([4, 4, 2]) ->  left([4]) + right([4, 2]) // left_size 4, right_size 6
      //   segment_sizes([4, 4, 2]) ->  left([4, 1]) + right([3, 2]) // left_size 5, right_size 5
      //   segment_sizes([4, 4, 2]) ->  right([4, 4, 2] // left_size 0, right_size 10
      std::queue<std::pair<size_t, std::vector<size_t>*>> q;
      if (left_child_size > 0) q.push({left_child_size, &left_segment_sizes});
      if (right_child_size > 0) q.push({right_child_size, &right_segment_sizes});
      size_t& child_space_left = q.front().first;
      std::vector<size_t>* child_segment_sizes = q.front().second;
      for (size_t i = 0; i < dop; ++i) {
        size_t chunk_size = this_segment_sizes[i];
        if (chunk_size == 0) {
          child_segment_sizes->push_back(0);
        } else {
          while (chunk_size > 0)  {
            size_t fill_size = std::min(child_space_left, chunk_size);
            child_segment_sizes->push_back(fill_size);
            chunk_size -= fill_size;
            child_space_left -= fill_size;
            if (child_space_left == 0) {
              q.pop();
              if (!q.empty()) {
                child_space_left = q.front().first;
                child_segment_sizes = q.front().second;
              }
            }
          }
        }
      }

      // the will be at most one overlapping child
      DASSERT_TRUE(left_segment_sizes.size() + right_segment_sizes.size() <=  dop + 1);
      DASSERT_TRUE(left_segment_sizes.size() + right_segment_sizes.size() >=  dop);

      m_left_child_iter = std::move(parallel_iterator<T>::create(m_left_child,
                                                                 std::max<size_t>(left_segment_sizes.size(), 1),
                                                                 left_segment_sizes));
      m_right_child_iter = std::move(parallel_iterator<T>::create(m_right_child,
                                                                  std::max<size_t>(right_segment_sizes.size(), 1),
                                                                  right_segment_sizes));

      auto all_iterators = std::make_shared<std::vector<decltype(m_left_child_iter)>>();
      all_iterators->push_back(m_left_child_iter);
      all_iterators->push_back(m_right_child_iter);
      auto iterator_segment_sizes = std::make_shared<std::vector<size_t>>();
      iterator_segment_sizes->push_back(left_segment_sizes.size());
      iterator_segment_sizes->push_back(right_segment_sizes.size());

      for (size_t i = 0; i < left_segment_sizes.size(); ++i) {
        m_segment_begin_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              0, i));
        if (i + 1 == left_segment_sizes.size()) {
          m_segment_end_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              1, 0));
        } else {
          m_segment_end_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              0, i+1));
        }
      }
      for (size_t i = 0; i < right_segment_sizes.size(); ++i) {
        m_segment_begin_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              1, i));
        if (i + 1 == right_segment_sizes.size()) {
          m_segment_end_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              2, 0));
        } else {
          m_segment_end_iterators.push_back(internal_iterator(all_iterators,
                                                              iterator_segment_sizes,
                                                              1, i+1));
        }
      }
      bool has_overlap_segment = (left_segment_sizes.size() + right_segment_sizes.size() > dop);
      if (has_overlap_segment) {
        size_t merge_offset = left_segment_sizes.size();
        // merge the end of left segments and begin of right segments.
        m_segment_end_iterators[merge_offset-1]
            = m_segment_end_iterators[merge_offset];
        m_segment_end_iterators.erase(m_segment_end_iterators.begin() + merge_offset);
        m_segment_begin_iterators.erase(m_segment_begin_iterators.begin() + merge_offset);
      }
    }
  }

  virtual void stop() {
    m_left_child_iter.reset();
    m_right_child_iter.reset();
    m_segment_begin_iterators.clear();
    m_segment_end_iterators.clear();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    auto& begin_iter = m_segment_begin_iterators[segment_index];
    auto& end_iter = m_segment_end_iterators[segment_index];

    if (begin_iter == end_iter) {
      return 0;
    }

    size_t items_to_skip = num_items;
    while (begin_iter != end_iter && items_to_skip > 0) {
      size_t items_skipped = begin_iter.skip_rows(items_to_skip);
      items_to_skip -= items_skipped;
      if (items_to_skip == 0) {
        break;
      } else {
        ++begin_iter;
      }
    }
    size_t total_items_skipped = (num_items - items_to_skip);
    return total_items_skipped;
  }

  virtual std::vector<T> get_next(size_t segment_index, size_t num_items) {
    auto return_value = std::vector<T>();
    return_value.reserve(num_items);

    auto& begin_iter = m_segment_begin_iterators[segment_index];
    auto& end_iter = m_segment_end_iterators[segment_index];

    if (begin_iter == end_iter) {
      return return_value;
    }

    size_t item_got = 0;
    size_t item_to_fetch = num_items;

    std::vector<T> buffer;
    while (begin_iter != end_iter && item_to_fetch > 0) {
      begin_iter.read(item_to_fetch, buffer);
      item_got = buffer.size();
      item_to_fetch -= item_got;
      return_value.insert(return_value.end(),
                          std::make_move_iterator(buffer.begin()),
                          std::make_move_iterator(buffer.end()));
      if (item_to_fetch == 0) {
        break;
      } else {
        ++begin_iter;
      }
    }
    return return_value;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_append<T>>(m_left_child, m_right_child, m_size);
  }

private:
  std::shared_ptr<lazy_eval_op_imp_base<T>> m_left_child, m_right_child;
  size_t m_size;
  flex_type_enum m_type;

  // The parallel iterator for each child
  std::shared_ptr<parallel_iterator<T>> m_left_child_iter;
  std::shared_ptr<parallel_iterator<T>> m_right_child_iter;

  // The state of an internal iterator refers to the child_iterator and its segment,
  // stored as a pair of ids.
  class internal_iterator {
   public:
    internal_iterator(std::shared_ptr<std::vector<std::shared_ptr<parallel_iterator<T>>>> all_iterators,
                      std::shared_ptr<std::vector<size_t>> iterator_segment_sizes,
                      size_t iterator_id, size_t segment_id) :
        m_iters(all_iterators), m_iterator_sizes(iterator_segment_sizes),
        m_iter_id(iterator_id), m_segment_id(segment_id) { }

    inline bool operator==(const internal_iterator& other) const {
      return (m_iter_id == other.m_iter_id) && (m_segment_id == other.m_segment_id);
    }
    inline bool operator!=(const internal_iterator& other) const { return !(*this == other); }
    inline void read(size_t num_items, std::vector<T>& buffer) {
      buffer = std::move((*m_iters)[m_iter_id]->get_next(m_segment_id, num_items));
    }
    inline size_t skip_rows(size_t num_items) {
      return (*m_iters)[m_iter_id]->skip_rows(m_segment_id, num_items);
    }
    inline void operator++() {
      if (m_segment_id + 1 < (*m_iterator_sizes)[m_iter_id]) {
        ++m_segment_id;
      } else {
        m_segment_id = 0;
        ++m_iter_id;
      }
    }
   private:
    // storing each children's parallel iterator
    std::shared_ptr<std::vector<std::shared_ptr<parallel_iterator<T>>>> m_iters;
    // the segments for each parallel_iterator
    std::shared_ptr<std::vector<size_t>> m_iterator_sizes;
    size_t m_iter_id;
    size_t m_segment_id;
  };

  // The begin and end iterator for each segment.
  std::vector<internal_iterator> m_segment_begin_iterators;
  std::vector<internal_iterator> m_segment_end_iterators;
};

/**
 * This class implements a "transform" operator that would lazily evaluate incoming data
 * and emits the transformed value. When \ref get_next() is called, this operator will get
 * values from its source and transform the incoming values and then output the transformed
 * values.
**/
 template <typename S, typename TransformFn = std::function<flexible_type(const S&)>>
 class le_transform: public lazy_eval_op_imp_base<flexible_type> {
 public:
  typedef std::function<void(std::vector<S> &, std::vector<flexible_type> &)> BatchTransformFn;

  /**
   * Constructs a new parallel transform operator
   * \param  parallel_iterator The source of this operator
   * \param  transform_fn The transform function
   * \param  type The output type of transform
  **/
   le_transform(
    std::shared_ptr<lazy_eval_op_imp_base<S>> source,
    TransformFn transform_fn,
    flex_type_enum type):
      lazy_eval_op_imp_base<flexible_type>(std::string("transform"), false, true) {

    DASSERT_MSG(source != NULL, "source cannot be NULL");

    m_source = source;
    m_transform_fn = transform_fn;
    m_type = type;

    m_batch_transform_fn = [&](std::vector<S> & input, std::vector<flexible_type> & output) { this->transform_simple(input, output); };

    m_lambda_hash = (size_t)(-1);
  }

   le_transform(
    std::shared_ptr<lazy_eval_op_imp_base<S>> source,
    TransformFn transform_fn,
    bool skip_undefined,
    int seed,
    flex_type_enum type,
    std::vector<std::string> column_names = {}):
      lazy_eval_op_imp_base<flexible_type>(std::string("transform"), false, true) {

    DASSERT_MSG(source != NULL, "source cannot be NULL");

    m_source = source;
    m_seed = seed;
    m_skip_undefined = skip_undefined;
    m_transform_fn = transform_fn;
    m_type = type;

    m_column_names = column_names;

    m_batch_transform_fn = [&](std::vector<S> & input, std::vector<flexible_type> & output) { this->transform_simple(input, output); };

    m_lambda_hash = (size_t)(-1);
  }

  le_transform(
    std::shared_ptr<lazy_eval_op_imp_base<S>> source,
    const std::string& lambda,
    bool skip_undefined,
    int seed,
    flex_type_enum type,
    std::vector<std::string> column_names = {}):
      lazy_eval_op_imp_base<flexible_type>(std::string("transform"), false, true) {

    DASSERT_MSG(source != NULL, "source cannot be NULL");

    m_source = source;
    m_seed = seed;
    m_skip_undefined = skip_undefined;
    m_lambda = lambda;
    m_type = type;

    ///// Extra configuration ////
    // Column names used for transforming list -> dictionary.
    m_column_names = column_names;

    m_batch_transform_fn = [&](std::vector<S> & input, std::vector<flexible_type> & output) { this->transform_lambda(input, output); };

    m_lambda_hash = (size_t)(-1);
  }

  ~le_transform() {
    // unregister the lambda
    if (m_lambda_hash != (size_t)(-1)) {
      if (boost::starts_with(m_lambda, "LUA")) {
        lambda::lualambda_master& evaluator = lambda::lualambda_master::get_instance();
        evaluator.release_lambda(m_lambda_hash);
      } else {
        lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
        evaluator.release_lambda(m_lambda_hash);
      }
    }
  }

  virtual flex_type_enum get_type() const {
    return m_type;
  }

  virtual bool has_size() const {
    return m_source->has_size();
  };

  virtual size_t size() const {
    logstream(LOG_DEBUG)  << "getting size from le_transform" << m_source->size() << std::endl;
    return m_source->size();
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children(1);
    children[0] = m_source;
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_MSG(children.size() == 1, "There should only be one child.");
    m_source = std::dynamic_pointer_cast<lazy_eval_op_imp_base<S>>(children[0]);
  }

protected:

  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_source_iterator = parallel_iterator<S>::create(m_source, dop, segment_sizes);

    // register lambda with lambda master
    if (!m_lambda.empty() && m_lambda_hash == (size_t)(-1)) {
      if (boost::starts_with(m_lambda, "LUA")) {
        lambda::lualambda_master& evaluator = lambda::lualambda_master::get_instance();
        m_lambda_hash = evaluator.make_lambda(m_lambda);
      } else {
        lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
        m_lambda_hash = evaluator.make_lambda(m_lambda);
      }
    }
  }

  virtual void stop() {
    m_source_iterator.reset();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    return m_source_iterator->skip_rows(segment_index, num_items);
  }

  virtual std::vector<flexible_type> get_next(size_t segment_index, size_t num_items) {
    logstream(LOG_DEBUG) << "thread " << segment_index << " trying to read in le_transform" << std::to_string(num_items) << std::endl;
    std::vector<flexible_type> output;

    std::vector<S> items = m_source_iterator->get_next(segment_index, num_items);
    if (items.size() == 0) {
      return output;
    }

    output.resize(items.size());
    logstream(LOG_DEBUG) << "thread: " << segment_index << ", transforming " << items.size() << " items " << std::endl;
    m_batch_transform_fn(items, output);
    logstream(LOG_DEBUG) << "thread: " << segment_index << ", done transforming. " << std::endl;

    return output;
  }

  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    if (m_transform_fn) {
      return std::make_shared<le_transform<S, TransformFn>>(m_source, m_transform_fn, m_skip_undefined, m_seed, m_type, m_column_names);
    } else {
      return std::make_shared<le_transform<S, TransformFn>>(m_source, m_lambda, m_skip_undefined, m_seed, m_type, m_column_names);
    }
  }

private:

  // transform items in place
  void transform_simple(std::vector<S>& input, std::vector<flexible_type>& output) {
    for(size_t i = 0; i < input.size(); i++) {
      output[i] = convert_value_to_output_type(m_transform_fn(input[i]), m_type);
    }
  }

  template<typename Dummy>
  void transform_lambda(std::vector<Dummy>& input, std::vector<flexible_type>& output) {
    DASSERT_MSG(false, "Transform lambda only support input type of flexible_type of vector<flexible_type>");
  }

  /**
   * Template speciailization for lambda transform on SFrame whose value type is vector<flexible_type>.
   */
  void transform_lambda(std::vector<std::vector<flexible_type>>& input, std::vector<flexible_type>& output) {

    logstream(LOG_DEBUG) << "transform lambda, input size " << input.size() << std::endl;

    lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();

    auto lambda_output = evaluator.bulk_eval(m_lambda_hash, m_column_names, input, m_skip_undefined, m_seed);

    logstream(LOG_DEBUG) << "transform lambda done, output size " << lambda_output.size() << std::endl;

    for(size_t i = 0; i < lambda_output.size(); i++) {
      output[i] = convert_value_to_output_type(lambda_output[i], m_type);
    }
  }

  /**
   * Template speciailization for lambda transform on SArray whose value type is flexible_type.
   */
  void transform_lambda(std::vector<flexible_type>& input, std::vector<flexible_type>& output) {
    logstream(LOG_DEBUG) << "transform lambda, input size " << input.size() << std::endl;

    std::vector<flexible_type> lambda_output;
    if (boost::starts_with(m_lambda, "LUA")) {
      lambda::lualambda_master& evaluator = lambda::lualambda_master::get_instance();
      lambda_output = evaluator.bulk_eval(m_lambda_hash, input, m_skip_undefined, m_seed);
    } else {
      lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
      lambda_output = evaluator.bulk_eval(m_lambda_hash, input, m_skip_undefined, m_seed);
    }

    logstream(LOG_DEBUG) << "transform lambda done, output size " << lambda_output.size() << std::endl;

    for(size_t i = 0; i < lambda_output.size(); i++) {
      output[i] = convert_value_to_output_type(lambda_output[i], m_type);
    }
  }


  flex_type_enum m_type;
  std::shared_ptr<lazy_eval_op_imp_base<S>> m_source;
  std::shared_ptr<parallel_iterator<S>> m_source_iterator;
  TransformFn m_transform_fn;
  BatchTransformFn m_batch_transform_fn;
  std::string m_lambda;
  bool m_skip_undefined = false;
  int m_seed;
  size_t m_lambda_hash;
  std::vector<std::string> m_column_names;
};


/**
 * This class implements a "vector" operator that would lazily evaluate incoming data
 * and emits the vector result. When \ref get_next() is called, this operator will get
 * values from its both sources, compute result and then output the computed vector values
**/
 class le_vector: public lazy_eval_op_imp_base<flexible_type> {
 public:
  typedef std::function<flexible_type(const flexible_type &, const flexible_type &)> VectorOpFn;
  typedef std::vector<flexible_type> VectorType;

  /**
   * Constructs a new parallel vector operator
   * \param  parallel_iterator The source of this operator
   * \param  vector_op_fn The vector function
   * \param  type The output type of vector
  **/
   le_vector(
    std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> left,
    std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> right,
    VectorOpFn vector_op_fn,
    flex_type_enum type):
        lazy_eval_op_imp_base<flexible_type>(std::string("vector"), false, true) {

    DASSERT_MSG(vector_op_fn != NULL, "vector_op_fn cannot be NULL");
    DASSERT_MSG(left != NULL, "source cannot be NULL");
    DASSERT_MSG(right != NULL, "source cannot be NULL");

    m_left = left;
    m_right = right;
    m_vector_op_fn = vector_op_fn;
    m_type = type;
  }

  virtual flex_type_enum get_type() const {
    return m_type;
  }

  virtual bool has_size() const {
    return m_left->has_size() && m_right->has_size();
  };

  virtual size_t size() const {
    if (!has_size()) {
      log_and_throw("One or more sources of vector operator do not have size ready, check has_size first.");
    }
    return m_left->size();
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children(2);
    children[0] = m_left;
    children[1] = m_right;
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_MSG(children.size() == 2, "There should be exactly two children.");
    m_left = std::dynamic_pointer_cast<lazy_eval_op_imp_base<flexible_type>>(children[0]);
    m_right = std::dynamic_pointer_cast<lazy_eval_op_imp_base<flexible_type>>(children[1]);
  }

protected:
  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_vector>(m_left, m_right, m_vector_op_fn, m_type);
  }

  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_left_iterator = parallel_iterator<flexible_type>::create(m_left, dop, segment_sizes);
    m_right_iterator = parallel_iterator<flexible_type>::create(m_right, dop, segment_sizes);
  }

  virtual void stop() {
    m_left_iterator.reset();
    m_right_iterator.reset();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    size_t l_skipped = m_left_iterator->skip_rows(segment_index, num_items);
    size_t r_skipped = m_right_iterator->skip_rows(segment_index, num_items);
    DASSERT_MSG(l_skipped == r_skipped, "skip rows should skip the same number on both sides");
    return l_skipped;
  }

  virtual std::vector<flexible_type> get_next(size_t segment_index, size_t num_items) {
    std::vector<flexible_type> left_items = m_left_iterator->get_next(segment_index, num_items);
    std::vector<flexible_type> right_items = m_right_iterator->get_next(segment_index, num_items);

    DASSERT_MSG(right_items.size() == left_items.size(), "There should be the same amount of items read from left and right for vector operation");

    if (left_items.size() == 0) {
      return left_items;
    }

    logstream(LOG_DEBUG) << "thread: " << segment_index << ", vector operation getting from left " << left_items.size() << " items " << std::endl;
    std::vector<flexible_type> output_items;
    transform(left_items, right_items);
    logstream(LOG_DEBUG) << "thread: " << segment_index << ", done vector processing. " << std::endl;

    return left_items;
  }

private:
  // transform and do an inplace replace of left value
  void transform(VectorType& left, VectorType& right) {
    for(size_t i = 0; i < left.size(); i++) {
      left[i] = m_vector_op_fn(left[i], right[i]);
    }
  }

  flex_type_enum m_type;
  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> m_left;
  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> m_right;
  std::unique_ptr<parallel_iterator<flexible_type>> m_left_iterator;
  std::unique_ptr<parallel_iterator<flexible_type>> m_right_iterator;
  VectorOpFn m_vector_op_fn;
};

/** This class implements a "filter" operator that would lazily evaluate
 * incoming data and emits the filtered result according to the index_vector
 * (evaluated as a series of booleans). When \ref get_next() is called, this
 * operator will get values from its both sources, compute result and then
 * output the computed vector values
**/
 template<typename T>
 class le_logical_filter: public lazy_eval_op_imp_base<T> {
 public:

  /**
   * Constructs a new parallel vector filter operator. The Vector filter operator
   * supports filtering one vector according to another index vector.
   *
   * \param  source_vector The source of this operator
   * \param  index_vector The index operator
   * \param  type The output type of vector
  **/
   le_logical_filter(
    std::shared_ptr<lazy_eval_op_imp_base<T>> source_vector,
    std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> index_vector,
    flex_type_enum type):
      lazy_eval_op_imp_base<T>(std::string("logical_filter"), true, true) {

    DASSERT_MSG(source_vector != NULL, "source cannot be NULL");
    DASSERT_MSG(index_vector != NULL, "index cannot be NULL");

    m_left = source_vector;
    m_right = index_vector;
    m_type = type;
  }

  virtual flex_type_enum get_type() const {
    return m_type;
  }

  virtual bool has_size() const {
    return false;
  };

  virtual size_t size() const {
    log_and_throw("Logical filter operation needs to be materialized before size() can be calculated.");
    return 0; 
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children(2);
    children[0] = m_left;
    children[1] = m_right;
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_MSG(children.size() == 2, "There should be exactly two children.");
    m_left = std::dynamic_pointer_cast<lazy_eval_op_imp_base<T>>(children[0]);
    m_right = std::dynamic_pointer_cast<lazy_eval_op_imp_base<flexible_type>>(children[1]);
  }

protected:
  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_logical_filter<T>>(m_left, m_right, m_type);
  }

  /**
   * Set degree of parallelism, must be called before \ref get_next()
  **/
   virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_left_iterator = parallel_iterator<T>::create(m_left, dop, segment_sizes);
    m_right_iterator = parallel_iterator<flexible_type>::create(m_right, dop, segment_sizes);
    m_left_over_items = std::vector<std::vector<T>>(dop);
  }

  virtual void stop() {
    m_left_over_items.clear();
    m_left_iterator.reset();
    m_right_iterator.reset();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    size_t items_skipped = 0;

    size_t left_over_item_count = m_left_over_items[segment_index].size();
    if (left_over_item_count > 0) {
      size_t items_to_skip = std::min(num_items, left_over_item_count);

      m_left_over_items.erase(m_left_over_items.begin(), std::next(m_left_over_items.begin(), items_to_skip));
      DASSERT_TRUE(m_left_over_items.size() == left_over_item_count - items_to_skip);

      items_skipped = items_to_skip;
    }

    while (items_skipped < num_items) {
      std::vector<flexible_type> right_items = m_right_iterator->get_next(segment_index, num_items);
      if (right_items.size() == 0) break;

      // delay reading of left items untill we see any non-zero value
      std::vector<T> left_items;
      for(size_t i = 0; i < right_items.size(); i++) {
        if (!right_items[i].is_zero()) {
          if (items_skipped == num_items) {
            if (left_items.size() == 0) { // fulfill delay read left side rows
              left_items = m_left_iterator->get_next(segment_index, right_items.size());
              DASSERT_TRUE(left_items.size() == right_items.size());
            }
            m_left_over_items[segment_index].push_back(left_items[i]);
          } else {
            items_skipped++;
          }
        }
      }

      // skip left side rows if all rows are skipped
      if (left_items.size() == 0) {
        m_left_iterator->skip_rows(segment_index, right_items.size());
      }
    }

    return items_skipped;
  }

  virtual std::vector<T> get_next(size_t segment_index, size_t num_items) {
    std::vector<T> output_items(num_items);
    size_t items_got = 0;

    // Move existing items to output if any
    size_t left_over_item_count = m_left_over_items[segment_index].size();
    if (left_over_item_count > 0) {
      size_t items_to_move = std::min(num_items, left_over_item_count);
      this->move_items(m_left_over_items[segment_index], output_items, items_to_move);
      DASSERT_TRUE(m_left_over_items[segment_index].size() == left_over_item_count - items_to_move);
      items_got = items_to_move;
    }

    while(items_got < num_items) {
      std::vector<flexible_type> right_items = m_right_iterator->get_next(segment_index, num_items);
      if (right_items.size() == 0) break;

      std::vector<T> left_items;
      for(size_t i = 0; i < right_items.size(); i++) {
        if (!right_items[i].is_zero()) {
          // delay reading let side
          if (left_items.size() == 0) {
            left_items = m_left_iterator->get_next(segment_index, right_items.size());
            DASSERT_EQ(left_items.size(), right_items.size());
          }

          if (items_got == num_items) {
            m_left_over_items[segment_index].push_back(left_items[i]);
          } else {
            output_items[items_got] = left_items[i];
            items_got++;
          }
        }
      }

      if (left_items.size() == 0) {
        size_t num_skipped = m_left_iterator->skip_rows(segment_index, right_items.size());
        DASSERT_EQ(num_skipped, right_items.size());
      }
    }

    output_items.resize(items_got);

    return output_items;
  }

private:
  flex_type_enum m_type;
  std::shared_ptr<lazy_eval_op_imp_base<T>> m_left;
  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> m_right;
  std::unique_ptr<parallel_iterator<T>> m_left_iterator;
  std::unique_ptr<parallel_iterator<flexible_type>> m_right_iterator;
  std::vector<std::vector<T>> m_left_over_items; // left over items from previous chunk read, one list for each chunk
};

/**
 * This class implements a scalar filter operator that would lazily evaluate incoming data
 * and emits the filtered result. Filter is a python lambda.
**/
 class le_lambda_filter: public lazy_eval_op_imp_base<flexible_type> {
 public:
  /**
   * Constructs a new parallel vector filter operator. The Vector filter operator
   * supports filtering one vector according to another index vector.
   *
   * \param  source The source of this operator
   * \param  lambda The index operator
   * \param  skip_undefined If true, then undefined value would evaluate to false
   * \param  seed The seed value for the python labmda evaluator
   * \param  type The output type of vector
  **/
   le_lambda_filter(
    std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> source,
    const std::string& lambda,
    bool skip_undefined,
    int seed,
    flex_type_enum type):
      lazy_eval_op_imp_base<flexible_type>(std::string("lambda_filter"), true) {

    DASSERT_MSG(source != NULL, "source cannot be NULL");

    logstream(LOG_DEBUG) << "lambda string" << lambda << std::endl;

    m_source = source;
    m_lambda = lambda;
    m_skip_undefined = skip_undefined;
    m_seed = seed;
    m_type = type;

    m_lambda_hash = (size_t)(-1);
  }

  ~le_lambda_filter() {
    if (m_lambda_hash != (size_t)(-1)) {
      lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
      evaluator.release_lambda(m_lambda_hash);
    }
  }

  virtual flex_type_enum get_type() const {
    return m_type;
  }

  virtual bool has_size() const {
    return false;
  };

  virtual size_t size() const {
    log_and_throw("Logical filter operation needs to be materialized before size() can be calculated.");
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children(1);
    children[0] = m_source;
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_MSG(children.size() == 1, "There should only have one child.");
    m_source = std::dynamic_pointer_cast<lazy_eval_op_imp_base<flexible_type>>(children[0]);
  }

protected:
  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_lambda_filter>(m_source, m_lambda, m_skip_undefined, m_seed, m_type);
  }

  /**
   * Set degree of parallelism, must be called before \ref get_next()
  **/
   virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_source_iterator = parallel_iterator<flexible_type>::create(m_source, dop, segment_sizes);
    m_left_over_items = std::vector<std::vector<flexible_type>>(dop);

    if (m_lambda_hash == (size_t)(-1)) {
      lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
      m_lambda_hash = evaluator.make_lambda(m_lambda);
    }
  }

  virtual void stop() {
    m_left_over_items.clear();
    m_source_iterator.reset();
  }

  size_t skip_rows(size_t segment_index, size_t num_items) {
    size_t items_skipped = 0;

    size_t left_over_item_count = m_left_over_items[segment_index].size();
    if (left_over_item_count > 0) {
      size_t items_to_skip = std::min(num_items, left_over_item_count);

      m_left_over_items.erase(m_left_over_items.begin(), std::next(m_left_over_items.begin(), items_to_skip));
      DASSERT_TRUE(m_left_over_items.size() == left_over_item_count - items_to_skip);

      items_skipped = items_to_skip;
    }

    lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
    while(items_skipped < num_items) {
      std::vector<flexible_type> input = m_source_iterator->get_next(segment_index, num_items);

      if (input.size() == 0) break;

      // batch process the result
      auto output = evaluator.bulk_eval(m_lambda_hash, input, m_skip_undefined, m_seed);
      DASSERT_TRUE(input.size() == output.size());

      for(size_t i = 0; i < output.size(); i++) {
        if (output[i]) {
          if (items_skipped == num_items) {
            m_left_over_items[segment_index].push_back(input[i]);
          } else {
            items_skipped++;
          }
        }
      }
    }

    return items_skipped;
  }

  std::vector<flexible_type> get_next(size_t segment_index, size_t num_items) {
    std::vector<flexible_type> output_items(num_items);
    size_t items_got = 0;

    // Move existing items to output if any
    size_t left_over_item_count = m_left_over_items[segment_index].size();
    if (left_over_item_count > 0) {
      size_t items_to_move = std::min(num_items, left_over_item_count);
      move_items(m_left_over_items[segment_index], output_items, items_to_move);

      DASSERT_TRUE(m_left_over_items[segment_index].size() == left_over_item_count - items_to_move);

      items_got = items_to_move;
      if (items_got == num_items) return output_items;
    }

    lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
    while(items_got < num_items) {
      std::vector<flexible_type> input = m_source_iterator->get_next(segment_index, num_items);

      if (input.size() == 0) break;

      // batch process the result
      auto output = evaluator.bulk_eval(m_lambda_hash, input, m_skip_undefined, m_seed);
      logstream(LOG_DEBUG) << " done with evaluator, # output " << output.size() << ", # input " << input.size() << std::endl;
      DASSERT_MSG(input.size() == output.size(), "lambda bulk evaluate input and output size should be the same." );

      for(size_t i = 0; i < output.size(); i++) {
        if (output[i]) {
          if (items_got == num_items) {
            m_left_over_items[segment_index].push_back(input[i]);
          } else {
            output_items[items_got] = input[i];
            items_got++;
          }
        }
      }
    }

    output_items.resize(items_got);
    return output_items;
  }

private:
  flex_type_enum m_type;
  std::string m_lambda;
  bool m_skip_undefined;
  int m_seed;
  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> m_source;
  std::unique_ptr<parallel_iterator<flexible_type>> m_source_iterator;
  size_t m_lambda_hash;

  // left over items from previous chunk read, one list for each chunk
  std::vector<std::vector<flexible_type>> m_left_over_items;
};


/**
 * This class implements a flat_map operator using python lambda.
 * The operator takes in sframe and returns sframe. Each input row gets
 * mapped to multiple output rows.
 **/
 class le_lambda_flat_map: public lazy_eval_op_imp_base<std::vector<flexible_type>> {
 public:
  /**
   * Constructs a new parallel flat_map operator.
   * \param  source The source of this operator
   * \param  lambda The flat_map operator
   * \param  skip_undefined If true, then undefined value would evaluate to false
   * \param  seed The seed value for the python labmda evaluator
   * \param  input_column_names The column names of the input vectors
   * \param  output_column_types The column types of the output vectors
   **/
   le_lambda_flat_map(
    std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> source,
    const std::string& lambda,
    bool skip_undefined,
    int seed,
    const std::vector<std::string>& input_column_names,
    const std::vector<flex_type_enum>& output_column_types):
      lazy_eval_op_imp_base<std::vector<flexible_type>>(std::string("lambda_flat_map"), true) {

    DASSERT_MSG(source != NULL, "source cannot be NULL");
    logstream(LOG_DEBUG) << "lambda string" << lambda << std::endl;

    m_source = source;
    m_lambda = lambda;
    m_skip_undefined = skip_undefined;
    m_seed = seed;

    m_lambda_hash = (size_t)(-1);
    m_input_column_names = input_column_names;
    m_output_column_types = output_column_types;
    DASSERT_TRUE(!m_input_column_names.empty());
    DASSERT_TRUE(!m_output_column_types.empty());
  }

  ~le_lambda_flat_map() {
    if (m_lambda_hash != (size_t)(-1)) {
      lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
      evaluator.release_lambda(m_lambda_hash);
    }
  }

  virtual flex_type_enum get_type() const {
    return flex_type_enum::LIST;
  }

  virtual bool has_size() const {
    return false;
  };

  virtual size_t size() const {
    log_and_throw("Flat map operation needs to be materialized before size() can be calculated.");
  }

  std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const {
    std::vector<std::shared_ptr<lazy_eval_op_base>> children(1);
    children[0] = m_source;
    return children;
  }

  void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>>& children) {
    DASSERT_MSG(children.size() == 1, "There should only have one child.");
    m_source = std::dynamic_pointer_cast<lazy_eval_op_imp_base<std::vector<flexible_type>>>(children[0]);
  }

protected:
  virtual std::shared_ptr<lazy_eval_op_base> clone() {
    return std::make_shared<le_lambda_flat_map>(m_source, m_lambda,
                                                m_skip_undefined, m_seed,
                                                m_input_column_names, m_output_column_types);
  }

  /**
   * Set degree of parallelism, must be called before \ref get_next()
   **/
   virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) {
    m_source_iterator = parallel_iterator<std::vector<flexible_type>>::create(m_source, dop, segment_sizes);
    m_local_buffer = std::vector<std::queue<std::vector<flexible_type>>>(dop);

    if (m_lambda_hash == (size_t)(-1)) {
      lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
      m_lambda_hash = evaluator.make_lambda(m_lambda);
    }
  }

  virtual void stop() {
    m_local_buffer.clear();
    m_source_iterator.reset();
  }

  virtual size_t skip_rows(size_t segment_index, size_t num_items) {
    return get_next(segment_index, num_items).size();
  }

  /**
   * Get next batch of values from given segment identified by "index"
   *
   * \param index Indicates which "chunk" of data to read from. The number of "chunks" is the same
   *        as DOP specified through \ref start(). Index starts from 0
   * \param num_items Indicates number of items expected to be returned. The return number of items
   *        could be less than the expected items if it is the last batch in the chunk.
   *
   * Returns the vector of values. This function could return less items than expected.
  **/
  std::vector<std::vector<flexible_type>> get_next(size_t segment_index, size_t num_items) {
    std::vector<std::vector<flexible_type>> output_items;
    output_items.reserve(num_items);
    lambda::pylambda_master& evaluator = lambda::pylambda_master::get_instance();
    auto& local_buffer = m_local_buffer[segment_index];
    size_t items_got = 0;

    while(items_got < num_items) {
      // Try move items from local buffer into final return vectors.
      size_t items_to_move = std::min<size_t>(num_items - items_got, local_buffer.size());
      for (size_t i = 0; i < items_to_move; ++i) {
        output_items.push_back(std::move(local_buffer.front()));
        local_buffer.pop();
      }
      items_got += items_to_move;
      // check if we've got enough items
      if (items_got == num_items)
        break;

      // Nope, we still need more items
      // Let's fetch next set of inputs
      auto input = m_source_iterator->get_next(segment_index, (num_items - items_got));
      if (input.empty()) {
        // We are running out of inputs, and local buffer is emptied. Return.
        DASSERT_TRUE(local_buffer.empty());
        break;
      } else {
        auto lambda_output = evaluator.bulk_eval(m_lambda_hash,
                                                 m_input_column_names,
                                                 input, m_skip_undefined, m_seed);
        logstream(LOG_DEBUG) << "transform lambda done, output size " << lambda_output.size() << std::endl;


        // for each input, compute the lambda flat map output and insert into local buffer
        for(size_t i = 0; i < lambda_output.size(); i++) {
          // unpack the rows
          std::vector<flexible_type> rows = std::move(lambda_output[i]);
          for (auto& row : rows) {
            std::vector<flexible_type> row_unpack = std::move(row);
            if (row_unpack.empty())
              continue;
            if (row_unpack.size() != m_output_column_types.size()) {
              log_and_throw("Lambda output size must be the same as the output column size.");
            }
            for (size_t j = 0 ; j < row_unpack.size(); ++j) {
              row_unpack[j] = convert_value_to_output_type(row_unpack[j], m_output_column_types[j]);
            }
            local_buffer.push(std::move(row_unpack));
          }
        }
      }
    } // end of while loop

   DASSERT_EQ(output_items.size(), items_got);
   return output_items;
 }

private:
  std::string m_lambda;
  bool m_skip_undefined;
  int m_seed;
  std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> m_source;
  std::unique_ptr<parallel_iterator<std::vector<flexible_type>>> m_source_iterator;
  size_t m_lambda_hash;
  std::vector<std::string> m_input_column_names;
  std::vector<flex_type_enum> m_output_column_types;

  // buffer output elements for each paralle iterator
  std::vector<std::queue<std::vector<flexible_type>>> m_local_buffer;
};

}
#endif
