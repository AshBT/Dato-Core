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
#ifndef GRAPHLAB_UNITY_PI_INTERFACE
#define GRAPHLAB_UNITY_PI_INTERFACE

#include <tuple>
#include <logger/logger.hpp>
#include <flexible_type/flexible_type.hpp>

namespace graphlab {

template<typename T>
class lazy_eval_op_imp_base;

/**
* Iterator that defines an interface that supports iterating input in parallel.
* This iterator is backed up by a lazy evaluation operator. On creation of the iterator
* the iterator registers itself with the operator and on destruction, unregister
* itself from the operator
*
* The usage pattern is:
*     auto iterator = some_lazy_eval_operator->get_parallel_iterator(dop)
*     parallel_for(0, dop, [&](size_t idx) {
*        items = iterator->get_next(idx, num_items);
*        ...
*     }
**/
template<typename T>
class parallel_iterator {
public:
  ~parallel_iterator();

  /**
  * Create a new parallel iterator over the given lazy eval oprator with the specified DOP
  **/
  static std::unique_ptr<parallel_iterator<T>> create(
      std::shared_ptr<lazy_eval_op_imp_base<T>> source, size_t dop, const std::vector<size_t>& segment_sizes = {});

  /**
  *  Get next set of values from given sement
  * \param segment_index the index to which the next set of items need to be retrieved
  * \param num_items Number of items to retrieve.
  *
  * returns the vector of items, if last batch, the actual number of items returned may be less than num_items
  **/
  std::vector<T> get_next(size_t segment_index, size_t num_items);

  /**
  *  Skip next set of values from given sement
  * \param segment_index the index to which the next set of items need to be skipped
  * \param num_items Number of items to skipped
  *
  * returns the number of items skipped. The actual number skipped may be less than
  * the requested if it is the last chunk of data
  **/
  size_t skip_rows(size_t segment_index, size_t num_items);

private:
  parallel_iterator(std::shared_ptr<lazy_eval_op_imp_base<T>> owner, size_t dop);

  size_t m_dop;
  std::vector<size_t> m_next_item_index;
  std::shared_ptr<lazy_eval_op_imp_base<T>> m_owner;
};

/**
 * Base class for all parallel iterators
**/
class lazy_eval_op_base {
  friend class query_processor;

public:
  /**
   * Returns whether or not the size is readily available for this operator
  **/
  virtual bool has_size() const = 0;

  /**
   * Get total number of items in the iterator
  **/
  virtual size_t size() const = 0;

  /**
   * Get the output type of item
  **/
  virtual flex_type_enum get_type() const = 0;

protected:
  lazy_eval_op_base(std::string& name, bool is_pace_changing, bool is_children_same_pace) {
    m_node_id = 0;
    m_name = name;
    m_pace_changing = is_pace_changing;
    m_children_same_pace = is_children_same_pace;
  }

  /**
   * Get children
  **/
  virtual std::vector<std::shared_ptr<lazy_eval_op_base>> get_children() const = 0;

  /**
   * Set children
  **/
  virtual void set_children(std::vector<std::shared_ptr<lazy_eval_op_base>> & children) = 0;

  /**
   * Clone the iterator so that the cloned iterator can consume the data
   * source from beginning again disregard current iterator's location
  **/
  virtual std::shared_ptr<lazy_eval_op_base> clone() = 0;

private:
  bool is_pace_changing() const { return m_pace_changing; }
  bool is_children_same_pace() const { return m_children_same_pace; }

  void set_node_id(size_t node_id) {
    DASSERT_MSG(node_id > 0, "0 is not a valid node id");
    m_node_id = node_id;
  }

  void clear_node_id() { m_node_id = 0; }
  size_t get_node_id() const { return m_node_id; }
  std::string get_name() const { return m_name; }

  size_t m_node_id;
  std::string m_name;
  bool m_pace_changing;
  bool m_children_same_pace;
};

/**
* This class is the templated base for all lazy evaluation operator
**/
template <typename T>
class lazy_eval_op_imp_base : public lazy_eval_op_base {

  friend class parallel_iterator<T>;

  /**
  ** The structure to hold cached items for operators that can be shared by multiple
  ** consumers.
  ** The cached items can be in one of the following states:
  **   a. m_initialized = false: not initialized
  **   b. m_initialized = true, m_fulfilled = false: the cached entry is a place holder
  **      for some rows that are not actually read from the operator. m_item_count
  **      indicates how many items are actually skipped, but m_items don't actually
  **      hold any rows
  **   c. m_initialized = true, m_fulfilled = true: the cached entry actually holds
  **      a set of items.
  **/
  struct parallel_iterator_cached_item {
  public:
    bool m_initialized = false;
    bool m_fullfilled = false;
    size_t m_start_item_index = 0;
    size_t m_item_count = 0;
    std::vector<T> m_items;
  };

public:
  lazy_eval_op_imp_base(std::string name, bool is_pace_changing, bool is_children_same_pace=true):
      lazy_eval_op_base(name, is_pace_changing, is_children_same_pace) {
    logstream(LOG_DEBUG) << "new object created: " << typeid(*this).name() << std::endl;
    m_started = false;
    m_dop = 0;
  }

protected:
  /**
   * Prepare for iteration over the operator given a DOP, used when first iterator is
   * initiated on the operator.
   *
   * If an non-empty segment_sizes is provided, it will be used as the chunking
   * sizes for the parallel iterator. The size of segment_sizes must equal to dop,
   * and the sum of segment_sizes must equal size().
  **/
  virtual void start(size_t dop, const std::vector<size_t>& segment_sizes) = 0;

  /**
   * Finish iteration and do appropriate cleanup. used when all iterators on this operator
   * has been released. The operator can be re-started after been stopped.
  **/
  virtual void stop() = 0;

  /**
   * Get next batch of values from given segment identified by "segment_index"
   *
   * \param segment_index Indicates which "chunk" of data to read from. The number of "chunks" is the same
   *        as DOP specified through \ref start(). Index starts from 0
   * \param num_items Indicates number of items expected to be returned. The return number of items
   *        could be less than the expected items if it is the last batch in the chunk.
   *
   * Returns the vector of values. This function could return less items than expected.
  **/
  virtual std::vector<T> get_next(size_t segment_index, size_t num_items) = 0;

  /**
  ** Skip producing number of rows.
   * \param segment_index Indicates which "chunk" of data to skip from. The number of "chunks" is the same
   *        as DOP specified through \ref start(). Index starts from 0
   * \param num_items Indicates number of items expected to be skipped. The return number of items
   *        could be less than the expected items if it is the last batch in the chunk.
   *
   * Returns the number of items skipped
  **/
  virtual size_t skip_rows(size_t segment_index, size_t num_items) = 0;

  // move first "num_to_move" items from source to beginning of target, resize source, target has enough
  // space to hold the items
  void move_items(std::vector<T>& source, std::vector<T>& target, size_t num_to_move) {
    DASSERT_MSG(target.size() >= num_to_move, "not enough space to hold the moved items");
    DASSERT_MSG(source.size() >= num_to_move, "not enough items in source");

    auto end = std::next(source.begin(), num_to_move);
    std::move(source.begin(), end, target.begin());

    source.resize(source.size() - num_to_move);
  }

  // shared utility by sframe and sarray reader to get chunk size according to dop
  void compute_chunk_sizes(
    size_t dop,
    size_t size,
    std::vector<size_t>& iterator_begins,
    std::vector<size_t>& iterator_ends) {

    size_t chunk_size = size / dop;
    if (chunk_size * dop < size) {
      chunk_size++;
    }

    // divide the input by DOP
    iterator_begins = std::vector<size_t>(dop);
    iterator_ends = std::vector<size_t>(dop);

    iterator_begins[0] = 0;
    iterator_ends[dop - 1] = size;
    for(size_t i = 1; i < dop; i++) {
      iterator_ends[i - 1] = iterator_begins[i] = i * chunk_size;
      if (i * chunk_size > size) {
        iterator_ends[i - 1] = iterator_begins[i] = size;
      }
    }
  }

  // shared utility for lazy operators to compute iterator location given a segment size partition
  void compute_iterator_locations(
    const std::vector<size_t>& segment_sizes,
    std::vector<size_t>& iterator_begins,
    std::vector<size_t>& iterator_ends) {


    iterator_begins.resize(segment_sizes.size());
    iterator_ends.resize(segment_sizes.size());
    iterator_begins[0] = 0;
    iterator_ends[0] = segment_sizes[0];
    for (size_t i = 1; i < segment_sizes.size(); ++i) {
      iterator_begins[i] = iterator_ends[i-1];
      iterator_ends[i] = iterator_begins[i] + segment_sizes[i];
    }
  }

private:
  void register_iterator(uintptr_t iterator, size_t dop, const std::vector<size_t>& segment_sizes) {

    if (m_started) {
      log_and_throw(std::string("Trying to start iterator again when it is already started, Iterator type: ") + typeid(*this).name());
    }

    if (m_dop > 0 && dop != m_dop) {
      log_and_throw(std::string("Trying to start iterator with different dop. Iterator type: ") + typeid(*this).name());
    }

    m_active_iterators[iterator] = 1;

    // first time give out iterator, initialize my self
    if (m_dop == 0) {
      init(dop);  // initialize myself
      start(dop, segment_sizes); // initialize child
    }
  }

  void unregister_iterator(uintptr_t iterator) {

    if (m_active_iterators.find(iterator) == m_active_iterators.end()) {
      log_and_throw("Trying to return an iterator that doesnt't exist");
    }

    m_active_iterators.erase(iterator);

    // if last iterator, cleanup child
    if (m_active_iterators.size() == 0) {
      this->stop();  // let child cleanup
      clear(); // clean up myself
    }
  }

  void init(size_t dop) {
    logstream(LOG_DEBUG) << "starting dop: " << dop << "  " << typeid(*this).name() << std::endl;

    DASSERT_MSG(dop > 0, "DOP must be at least 1.");
    DASSERT_MSG(m_dop == 0, "DOP already set.");
    m_dop = dop;

    m_cached_items.resize(dop);
  }

  void clear() {
    // clean up cache
    m_cached_items.clear();
    m_dop = 0;
    m_started = false;
  }

  /**
   * Get next set of items from the iterator. This function will throw if items are not retrieved in order
   **/
  size_t skip_items(size_t segment_index, size_t start_item, size_t num_items) {
    DASSERT_TRUE(segment_index < m_dop);
    m_started = true;

    auto& cached_items = m_cached_items[segment_index];
    DASSERT_TRUE(start_item == 0 || cached_items.m_initialized);

    if (cached_items.m_initialized) {
      if (cached_items.m_start_item_index == start_item) {
        DASSERT_TRUE(num_items == cached_items.m_item_count);
        return num_items;
      } else {
        // Caller should consume the rows in order
        DASSERT_LT(cached_items.m_start_item_index, start_item);
      }
    }

    // move to next segment but do not read the value, mark m_fullfilled flag as false
    // but remember the actual number items skipped
    size_t num_skipped = this->skip_rows(segment_index, num_items);
    m_cached_items[segment_index].m_start_item_index = start_item;
    m_cached_items[segment_index].m_initialized = true;
    m_cached_items[segment_index].m_fullfilled = false;
    m_cached_items[segment_index].m_item_count = num_skipped;
    m_cached_items[segment_index].m_items.clear();

    return num_skipped;
  }

  /**
   * Get next set of items from the iterator. This function will throw if items are not retrieved in order
   **/
  std::vector<T> get_items(size_t segment_index, size_t start_item, size_t num_items) {
    DASSERT_TRUE(segment_index < m_dop);
    m_started = true;

    auto& cached_items = m_cached_items[segment_index];
    DASSERT_TRUE(start_item == 0 || cached_items.m_initialized);

    if (cached_items.m_initialized) {
      if (cached_items.m_start_item_index == start_item) {
        // There should never have cases where the items are skipped but is needed later
        DASSERT_TRUE(cached_items.m_fullfilled);
        DASSERT_TRUE(num_items >= cached_items.m_item_count);
        return cached_items.m_items;
      } else {
        // the read should be in order
        DASSERT_LT(cached_items.m_start_item_index, start_item);
      }
    }

    // fulfill next chunk
    auto items = this->get_next(segment_index, num_items);
    m_cached_items[segment_index].m_start_item_index = start_item;
    m_cached_items[segment_index].m_items = items;
    m_cached_items[segment_index].m_item_count = items.size();
    m_cached_items[segment_index].m_initialized = true;
    m_cached_items[segment_index].m_fullfilled = true;

    return items;
  }

  std::map<uintptr_t, uintptr_t> m_active_iterators;
  std::vector<parallel_iterator_cached_item> m_cached_items;
  bool m_started;
  size_t m_dop;
};

/******************************************************
*   Implementor of parallel iterator
*******************************************************/
template<typename T>
std::unique_ptr<parallel_iterator<T>> parallel_iterator<T>::create(
  std::shared_ptr<lazy_eval_op_imp_base<T>> source, size_t dop,
  const std::vector<size_t>& segment_sizes) {


  parallel_iterator<T>* iterator = new parallel_iterator<T>(source, dop);
  std::unique_ptr<parallel_iterator<T>> iterator_ptr(iterator);

  source->register_iterator((uintptr_t)iterator, dop, segment_sizes);

  return iterator_ptr;
}

template<typename T>
parallel_iterator<T>::parallel_iterator(std::shared_ptr<lazy_eval_op_imp_base<T>> owner, size_t dop) {
  m_owner = owner;
  m_dop = dop;
  m_next_item_index.resize(dop);
}

template<typename T>
parallel_iterator<T>::~parallel_iterator() {
  m_owner->unregister_iterator((uintptr_t)this);
  m_owner.reset();
}

template<typename T>
std::vector<T> parallel_iterator<T>::get_next(size_t segment_index, size_t num_items) {
  auto return_val = m_owner->get_items(segment_index, m_next_item_index[segment_index], num_items);
  m_next_item_index[segment_index] += return_val.size();
  return return_val;
}

template<typename T>
size_t parallel_iterator<T>::skip_rows(size_t segment_index, size_t num_items) {
  size_t num_skipped = m_owner->skip_items(segment_index, m_next_item_index[segment_index], num_items);
  m_next_item_index[segment_index] += num_skipped;
  return num_skipped;
}

}
#endif
