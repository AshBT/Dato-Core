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
#ifndef GRAPHLAB_UNITY_VPI_GENERATOR_HPP
#define GRAPHLAB_UNITY_VPI_GENERATOR_HPP

#include <mutex>
#include <sframe/sframe.hpp>
#include <sframe/sarray.hpp>
#include <unity/query_process/lazy_sarray.hpp>

namespace graphlab {

/**
 * Generator that produces a list of parallel block iterator for SFrame like operations
 * It mimicks functionalites that required for sframe but not actually materializing any
 * on disk sframe unless asked to do so
 *
 * Interanlly, the lazy_sframe could be in one of the following three states:
 *  1) backed up by a actual sframe and a collection of sarrays are extracted from the sframe
 *  2) backed up only by a collection of lazy sarrays without materialize
 *  3) backed up by a lazy operator that emits rows of vector<flexible_type>
 *
 * For some sframe operations, like select_column(s), the first two layout is a simple
 * operation that returns corresponding columns, but if sframe is in layout 3), then
 * we need to materialize the lazy sframe so that it goes to state 1)
 *
 * For query execution operation like get_iterator(), get_query_tree(), both layout
 * 2) and 3) works well.
 *
 * In the following cases the sframe will be materialized:
 *    a. when get_sframe_ptr() is called, that means caller really want to get access to an sframe object
 *    b. when the size of some of the iterator cannnot be determined by some operation requires the size to be available
 *       for example, a vector operation will require both size of the operation be the same size
 *    c. when the lazy sframe is backed by a lazy operator but column wise operations
 *       is needed (add_column, remove_column, select_column, swap_column)
*/
class lazy_sframe {
  typedef std::shared_ptr<lazy_sarray<flexible_type>> LazySArrayPtrType;

public:
  /**
   * Construct a parallel iterator generator that is backed by an sframe
  **/
  lazy_sframe(std::shared_ptr<sframe> sframe_ptr);

  /**
   * Construct a parallel iterator generator that is backed by a list of lazily evaluated operator tree
  **/
  lazy_sframe(
    const std::vector<LazySArrayPtrType>& lazy_sarrays,
    const std::vector<std::string>& column_names);

  lazy_sframe(
    std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> lazy_op,
    const std::vector<std::string>& column_names,
    const std::vector<flex_type_enum>& column_types);
  /**
    * Retrieve the set of iterators for all columns
  **/
  std::vector<LazySArrayPtrType> get_lazy_sarrays();

  /**
    * Returns number of rows in the sframe.
    * This could cause the tree to be materialized if not all size are available
  **/
  size_t size();

  /**
   * return the sarray iterator for a given column
  **/
  LazySArrayPtrType select_column(size_t col_id) const;

  /**
   * return the sarray iterator for a given column
  **/
  LazySArrayPtrType select_column(const std::string &name) const;

  /**
   * return a new lazy_sframe with selected columns
  **/
  std::shared_ptr<lazy_sframe> select_columns(const std::vector<std::string> &names) const;

  /**
   * return column index given column name
  **/
  size_t column_index(const std::string& column_name) const;

  /**
   * return number of columns in the sframe
  **/
  size_t num_columns() const { return m_column_names.size(); }

  /**
    * return column names for all columns in the iterator value
   **/
  std::vector<std::string> column_names() const { return m_column_names; }

  /**
    * return column types for all columns in the iterator value
   **/
  std::vector<flex_type_enum> column_types() const { return m_column_types; }

  /**
  * return the column name for the given column
  **/
  std::string column_name(size_t col) const;

  /**
  * return the column type for the given column
  **/
  flex_type_enum column_type(size_t col) const;

  /**
  * return the column type for the column with the given name
  **/
  flex_type_enum column_type(std::string name) const { return column_type(column_index(name)); }

  /**
   * Add one column to the sframe
  **/
  void add_column(LazySArrayPtrType iterator, std::string column_name);

  /**
   *  Check whether or not a given column exists in the sframe
   **/
  bool contains_column(std::string name) const;

  /**
   *  set name for a given column
   **/
  void set_column_name(size_t index, std::string& name);

  /**
   *  Remove a given column from the sframe
   **/
  void remove_column(size_t index);

  /**
   *  swap two columns in the sframe
   **/
  void swap_columns(size_t column_1, size_t column_2);

  /**
   * Append this with the other lazy sframe. The append is done lazily
   * on each column.
   */
  std::shared_ptr<lazy_sframe> append(std::shared_ptr<lazy_sframe> other);

  /**
   *  Get underneath query tree for this lazy sframe
   **/
  std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> get_query_tree();

  /**
   *  Get iterator that can iterate over rows in sframe
   **/
  std::unique_ptr<parallel_iterator<std::vector<flexible_type>>> get_iterator(size_t dop, bool to_materialize = true) {
    // for consistent result, we materilize the tree the moment some consumer wants to consume the data
    if (to_materialize) {
      materialize();
    }

    return query_processor::start_exec(get_query_tree(), dop);
  }

  /**
    * Wrap a lazy_sframe inside a lazy_sarray
    * A lazy_sframe emits value of type vector<flexible_type>, the resulting lazy_sarray would emit
    * value of flexible_type that is of type flex_type_enum::LIST
  **/
  std::shared_ptr<lazy_sarray<std::vector<flexible_type>>> to_lazy_sarray();

  /**
    * materialize the lazy sframe and get the real sframe
  **/
  inline std::shared_ptr<sframe> get_sframe_ptr() {
    if (!m_sframe_ptr) {
      materialize();
    }

    return m_sframe_ptr;
  }

  /**
   * Materialize SFrame. This is different from save_sframe in the sense that it is not
   * creating a completely new on-disk sframe. It simply materialize each sarray and
   * creates sframe wrapper around them
  **/
  void materialize() const;

  bool is_materialized() const {
    if (m_sframe_ptr != NULL) return true;
    if (m_lazy_sarrays.size() > 0) {
      for (auto& sa : m_lazy_sarrays) {
        if (!sa->is_materialized()) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  bool has_size() const {
    if (is_materialized()) {
      return true;
    }
    if (m_lazy_operator) {
      return m_lazy_operator->has_size();
    }
    for (auto& sa : m_lazy_sarrays) {
      if (!sa->has_size()) {
        return false;
      }
    }
    return true;
  }

private:

  // extrace lazy sarray from a materializes sframe, this makes sure we always
  // have a collection of sarrays for consumption
  void get_lazy_sarrays_from_sframe(std::shared_ptr<sframe> sframe_ptr) const;
  std::string generate_next_column_name() const;

  inline void invalidate_sframe_ptr() { m_sframe_ptr = NULL; }
  inline void materialize_if_lazy_op() const {
    if (m_lazy_operator) materialize();
  }

  std::vector<std::string> m_column_names;
  std::vector<flex_type_enum> m_column_types;

  mutable mutex m_lock;
  mutable std::vector<LazySArrayPtrType> m_lazy_sarrays;
  mutable std::shared_ptr<sframe> m_sframe_ptr;
  mutable std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> m_lazy_operator;
};

}
#endif
