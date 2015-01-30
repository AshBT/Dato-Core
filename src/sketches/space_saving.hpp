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
#ifndef GRAPHLAB_SKETCHES_SPACE_SAVING_HPP
#define GRAPHLAB_SKETCHES_SPACE_SAVING_HPP
#include <vector>
#include <cmath>
#include <map>
#include <graphlab/util/hopscotch_map.hpp>
#include <logger/assertions.hpp>
#include <util/code_optimization.hpp>
namespace graphlab {
namespace sketches {

/**
 * This class implements the Space Saving Sketch as described in 
 * Ahmed Metwally † Divyakant Agrawal Amr El Abbadi. Efficient Computation of 
 * Frequent and Top-k Elements in Data Streams.
 *
 * It provides an efficient one pass scan of all the data and provides an 
 * estimate all the frequently occuring elements, with guarantees that all 
 * elements with occurances >= \epsilon N will be reported.
 *
 * \code
 *   space_saving ss;
 *   // repeatedly call
 *   ss.add(stuff);
 *   // will return an array containing all the elements tracked
 *   // not all elements may be truly frequent items
 *   ss.frequent_items() 
 *   // will return an array containing all the elements tracked which are
 *   // guaranteed to have occurances >= \epsilon N
 * \endcode
 */

template <typename T>
class space_saving {
 public:
  /**
   * Constructs a save saving sketch using 1 / epsilon buckets.
   * The resultant hyperloglog datastructure will 1 / epsilon memory, and
   * guarantees that all elements with occurances >= \epsilon N will be reported.
   */
  space_saving(double epsilon = 0.0001) {
    initialize(epsilon);
  }

  /**
   * Initalizes a save saving sketch using 1 / epsilon buckets.
   * The resultant hyperloglog datastructure will 1 / epsilon memory, and
   * guarantees that all elements with occurances >= \epsilon N will be reported.
   */
  void initialize(double epsilon = 0.0001) {
    for(auto entry: m_heap) {
      if (entry != NULL) delete entry;
    }
    m_value_to_heap_element.clear();
    // capacity = 1.0 / epsilon. add one to avoid rounding problems around the
    // value of \epsilon N
    m_max_capacity = std::ceil(1.0 / epsilon) + 1;
    m_heap.reserve(m_max_capacity + 1);
    m_heap.resize(1, NULL); // we maintain m_heap as 1-indexed.
    m_value_to_heap_element.rehash(m_max_capacity * 2);
    m_epsilon = epsilon;
    m_size = 0;
  }

  /**
   * Adds an item with a specified count to the sketch.
   */
  void add(const T& t, size_t count = 1) {
    add_impl(t, count);
  }
  /**
   * Returns the number of elements inserted into the sketch.
   */
  size_t size() const {
    return m_size;
  }

  /**
   * Returns all the elements tracked by the sketch as well as an 
   * estimated count. The estimated can be a large overestimate.
   */
  std::vector<std::pair<T, size_t> > frequent_items() const {
    std::vector<std::pair<T, size_t> > ret;
    for(auto count: m_heap) {
      if (count == NULL) continue;
      if (count->count >= m_epsilon * m_size) {
        ret.push_back(std::make_pair(count->element, count->count));
      }
    }
    return ret;
  }


  /**
   * Returns all the elements tracked by the sketch as well as an 
   * estimated count. All elements returned are guaranteed to have 
   * occurance >= epsilon * m_size
   */
  std::vector<std::pair<T, size_t> > guaranteed_frequent_items() const {
    std::vector<std::pair<T, size_t> > ret;
    for(auto count: m_heap) {
      if (count == NULL) continue;
      if (count->count - count->error >= m_epsilon * m_size) {
        ret.push_back(std::make_pair(count->element, count->count));
      }
    }
    return ret;
  }

  /**
   * Merges a second space saving sketch into the current sketch
   */
  void combine(const space_saving<T>& other) {
    /**
     * Pankaj K. Agarwal, Graham Cormode, Zengfeng Huang, 
     * Jeff M. Phillips, Zhewei Wei, and Ke Yi.  Mergeable Summaries.
     * 31st ACM Symposium on Principals of Database Systems (PODS). May 2012.
     */
    // bump up max capacity so we don't lose anything 
    m_max_capacity += other.m_max_capacity;
    for (auto count : other.m_heap) {
      if (count == NULL) continue;
      add_impl(count->element, count->count, count->error);
    }
    // now we need to trim back down to max_capacity
    m_max_capacity -= other.m_max_capacity;
    // nothing to do
    if (m_heap.size() <= m_max_capacity + 1) return;

    // this is the number if items I have to delete
    size_t items_to_delete = m_heap.size() - m_max_capacity;
    for (size_t i = 0;i < items_to_delete; ++i) {
      delete_heap_top();
    } 
  }

  ~space_saving() {
    for(auto entry: m_heap) {
      if (entry != NULL) delete entry;
    }
  }

 private:
  struct heap_entry{
    T element = T();
    size_t count = 0;
    size_t error = 0;
    size_t heap_position = 0;
    heap_entry* next = NULL;  // used to manage the linked list in the hopscotch map
    heap_entry() = default;
    heap_entry(const T& element, size_t count, size_t error) 
        :element(element), count(count), error(error) { }
  };


  struct identity_hash {
   public:
    uint64_t operator()(const uint64_t t) const {
      return t;
    }
  };
  std::vector<heap_entry*> m_heap;
  hopscotch_map<uint64_t, heap_entry*, identity_hash> m_value_to_heap_element;

  // number of unique values to track
  size_t m_max_capacity;

  // number of elements added
  size_t m_size = 0;

  double m_epsilon;

  heap_entry* value_to_heap_element(const T& t) {
    size_t hashval = std::hash<T>()(t);
    auto iter = m_value_to_heap_element.find(hashval);
    if (iter == m_value_to_heap_element.end()) {
      return NULL;
    } else {
      auto val = iter->second;
      // fastpath
      if (val != NULL && val->element == t) return val;
      // slowpath
      while(val != NULL) {
        if (LIKELY(val->element == t)) {
          return val;
        }
        val = val->next;
      }
    }
    return NULL;
  }

  void value_to_heap_element_erase(const T& t) {
    size_t hashval = std::hash<T>()(t);
    auto iter = m_value_to_heap_element.find(hashval);
    if (iter != m_value_to_heap_element.end()) {
      heap_entry* prev = NULL;
      auto val = iter->second;
      // fastpath
      if (val != NULL && val->element == t && val->next == NULL) {
        m_value_to_heap_element.erase(hashval);
        return;
      }
      while(val != NULL) {
        if (LIKELY(val->element == t)) {
          if (prev == NULL) {
            // we are deleting the first element in the list
            if (LIKELY(val->next == NULL)) m_value_to_heap_element.erase(hashval);
            else m_value_to_heap_element[hashval] = val->next;
            break;
          } else {
            // connect previous to next
            prev->next = val->next;
            break;
          }
        }
        prev = val;
        val = val->next;
      }
    }
  }


  void value_to_heap_element_erase(heap_entry* t) {
    size_t hashval = std::hash<T>()(t->element);
    auto iter = m_value_to_heap_element.find(hashval);
    if (iter != m_value_to_heap_element.end()) {
      heap_entry* prev = NULL;
      auto val = iter->second;
      // fastpath
      if (val != NULL && val == t && val->next == NULL) {
        m_value_to_heap_element.erase(hashval);
        return;
      }

      while(val != NULL) {
        if (LIKELY(val == t)) {
          if (LIKELY(prev == NULL)) {
            // we are deleting the first element in the list
            if (LIKELY(val->next == NULL)) m_value_to_heap_element.erase(hashval);
            else m_value_to_heap_element[hashval] = val->next;
            break;
          } else {
            // connect previous to next
            prev->next = val->next;
            break;
          }
        }
        prev = val;
        val = val->next;
      }
    }
  }

  void value_to_heap_element_insert(heap_entry* t) {
    // m_value_to_heap_element2[t->element] = t;
    size_t hashval = std::hash<T>()(t->element);
    auto iter = m_value_to_heap_element.find(hashval);
    if (iter == m_value_to_heap_element.end()) {
      t->next = NULL;
    } else {
      t->next = iter->second;
    }
    m_value_to_heap_element[hashval] = t;
  }

  void add_impl(const T& t, size_t count = 1, size_t error = 0) {
    heap_entry* found_elem = value_to_heap_element(t);
    if (found_elem != NULL) {
      // value is already in the heap. update the priorities
      found_elem->count += count;
      found_elem->error += error;
      heap_bubble_down(found_elem->heap_position);
    } else {
      // element not found!
      // are we full? Remember that m_heap is 1-indexed (to simplify the 
      // heap calculations)
      if (m_heap.size() <= m_max_capacity) {
        // we are not full. insert
        heap_entry* new_entry = new heap_entry(t, count, error);
        new_entry->heap_position = m_heap.size();
        m_heap.push_back(new_entry);
        value_to_heap_element_insert(new_entry);
        heap_bubble_up(new_entry->heap_position);
      } else {
        // yup. we are full. we are going to rename the one with the 
        // smallest count. Remember that m_heap is 1-indexed.
        heap_entry* heap_head = m_heap[1];
        value_to_heap_element_erase(heap_head);
        heap_head->element = t;
        heap_head->error = heap_head->count + error;
        heap_head->count += count;
        value_to_heap_element_insert(heap_head);
        heap_bubble_down(1);
      }
    }
    m_size += count;
  }
  
  void heap_bubble_up(size_t idx) {
    auto cur = m_heap[idx];
    auto cur_count = cur->count;
    while (idx > 1) {
      size_t compareidx = idx / 2;
      auto parent = m_heap[compareidx];
      // if parent is bigger than me, swap
      if (cur_count < parent->count) {
        m_heap[idx] = m_heap[compareidx];
        parent->heap_position = idx;
        idx = compareidx;
      } else {
        break;
      }
    }
    m_heap[idx] = cur;
    cur->heap_position = idx;
  }

  void heap_bubble_down(size_t idx) {
    auto cur = m_heap[idx];
    auto cur_count = cur->count;
    while (idx * 2 < m_heap.size()) {
      size_t left_child_idx = idx * 2;
      size_t right_child_idx = idx * 2 + 1;
      // find the smaller child if any
      size_t smaller_child = left_child_idx;

      if (right_child_idx < m_heap.size() && 
          m_heap[left_child_idx]->count > m_heap[right_child_idx]->count) {
        smaller_child = right_child_idx;
      } 

      auto child = m_heap[smaller_child];
      // if there exists a child, and if my count is larger than the child
      // we swap
      if (cur_count > child->count) {
        m_heap[idx] = child;
        child->heap_position = idx;
        idx = smaller_child;
      } else {
        break;
      }
    }
    m_heap[idx] = cur;
    cur->heap_position = idx;
  }

  void delete_heap_top() {
    if (m_heap.size() > 2) {
      // swap last element to top 
      std::swap(m_heap[m_heap.size() - 1], m_heap[1]);
      heap_entry* element_to_delete = m_heap[m_heap.size() - 1];
      m_heap[1]->heap_position = 1;
      value_to_heap_element_erase(element_to_delete);
      m_heap.pop_back();
      delete element_to_delete;
      heap_bubble_down(1);
    } else if (m_heap.size() == 2) {
      delete m_heap[1];
      m_heap.pop_back();
      m_value_to_heap_element.clear();
    }
  }

  void heap_check() {
    for (size_t i = 1;i < m_heap.size(); ++i) {
      ASSERT_EQ(m_heap[i]->heap_position, i);
      if (i > 1) ASSERT_GE(m_heap[i]->count, m_heap[i/2]->count);
    }
  }


};

} // sketch
} // namespace graphlab
#endif
