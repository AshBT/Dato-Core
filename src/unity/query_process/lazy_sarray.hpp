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

#ifndef GRAPHLAB_UNITY_LAZY_SARRAY
#define GRAPHLAB_UNITY_LAZY_SARRAY

#include <logger/logger.hpp>
#include <flexible_type/flexible_type.hpp>
#include <unity/query_process/lazy_eval_op_imp.hpp>
#include <unity/query_process/query_processor.hpp>

namespace graphlab {
/**
 * Generator that produces the parallel block iterator for SArray like
 * operation.  Each produced iterator supports iterating the data source once.
 * When the need arises to re-consume the data source, a new iterator must be
 * generated.
 *
 * The data source that provides parallel iterator may be one of the following cases:
 *    * an actual disk-backed data source -- in this case, a new iterator will simply be created
 *      to wrap the data source
 *    * an operator tree -- When asked to produce data, the operator pipeline will start and
 *      data are pipelined from data source, through the operator tree and eventually get returned
 *      to consumer. This way we can avoid materializing intermediate data. But materialization
 *      will be triggered when the second iterator is asked for.
 *
 *  To materialize an operator tree, a new disk-based sarray is created and all data is pipelined to
 *  the sarray. The current operator tree is then released and the generator will get hold of the
 *  newly created disk-backed iterator.
*/
template <typename T>
class lazy_sarray {
public:
  /**
   * Construct a parallel iterator generator that is backed by a lazily evaluated operator tree
  **/
  lazy_sarray(
    std::shared_ptr<lazy_eval_op_imp_base<T>> query_tree,
    bool materialized,
    flex_type_enum type) {

    m_materialized = materialized;
    m_query_tree = query_tree;
    m_type = type;
  }

  /**
    * Get the actual query tree behind the lazy evaluated data
  **/
  std::shared_ptr<lazy_eval_op_imp_base<T>> get_query_tree() {
    Dlog_func_entry();

    return m_query_tree;
  }

  /**
    * Returns number of rows in the iterator
  **/
  size_t size() {
    Dlog_func_entry();

    if (m_query_tree->has_size()) {
      return m_query_tree->size();
    }

    materialize();

    DASSERT_MSG(m_query_tree->has_size(), "Materialized operation tree should have size available.");
    return m_query_tree->size();
  }

  /**
    * Returns the output type of the data source
  **/
  flex_type_enum get_type() const {
    return m_type;
  }

  /**
    * Returns the underline supporting sarray. This can cause the tree to be materialized if
    * it is currently not backed up by an sarray
  **/
  std::shared_ptr<sarray<T>> get_sarray_ptr() {
    if (!m_materialized) {
      materialize();
    }

    DASSERT_MSG(m_materialized, "The materialize process should materialize the tree");

    return (dynamic_cast<le_sarray<T>*>(m_query_tree.get()))->get_sarray_ptr();
  }

  /**
   * Returns a new lazy sarray which is the result of appending this lazy_sarray
   * with the other lazy_sarray.
   * The append operation assumes size, and will materialize either one if it does not have size.
   * If both lazy_sarrays are materialized,
   * the return lazy_sarray will also be materialized.
   */
  std::shared_ptr<lazy_sarray<T>> append(std::shared_ptr<lazy_sarray<T>> other) {

    std::shared_ptr<lazy_sarray<T>> ret;

    DASSERT_TRUE(other);
    DASSERT_EQ((int)this->get_type(), (int)other->get_type());

    // materialize if the lazy_sarray does not have size
    if (!get_query_tree()->has_size()) materialize();
    if (!other->get_query_tree()->has_size()) other->materialize();

    // construct lazy sarrays on top of the newly generated sarrays
    if (is_materialized() && other->is_materialized()) {
      sarray<flexible_type> new_sarray =
        get_sarray_ptr()->append(*other->get_sarray_ptr());
      ret = std::make_shared<lazy_sarray<T>>(
        std::make_shared<le_sarray<T>>(std::make_shared<sarray<T>>(new_sarray)),
        true,
        get_type());
    } else {
      ret = std::make_shared<lazy_sarray<flexible_type>>(
        std::make_shared<le_append<flexible_type>>(get_query_tree(), other->get_query_tree(), this->size() + other->size()),
        false,
        get_type());
    }
    return ret;
  }

  /**
   * Test hook for now to check if the tree is materialized
   **/
  bool is_materialized() const {
    return m_materialized;
  }

  /**
   * Return true if the size of the lazy sarray is known.
   */
  bool has_size() const {
    return m_query_tree->has_size();
  }

  /**
   * For an operator tree that lazily evaluates, we sometimes need to materialize the tree and store the results
   * in a file to facilitate efficient operation later. This usually happens when there is a corresponding client
   * side handle that user repeatly do operations on top this handle.
  **/
  void materialize();

  std::unique_ptr<parallel_iterator<T>> get_iterator(size_t dop, bool materialize = true);

private:
  std::shared_ptr<lazy_eval_op_imp_base<T>> m_query_tree;
  bool m_materialized;
  flex_type_enum m_type;
};

}
#endif
