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
#ifndef GRAPHLAB_UNITY_LAZY_SARRAY_CPP
#define GRAPHLAB_UNITY_LAZY_SARRAY_CPP

#include <logger/logger.hpp>
#include <unity/query_process/query_processor.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>

namespace graphlab {
  template<typename T>
  void lazy_sarray<T>::materialize() {
    Dlog_func_entry();

    if (!m_materialized)  {
      auto new_sarray_ptr = graphlab::save_sarray(*this, m_type);

      // re-adjust local state
      m_query_tree = std::make_shared<le_sarray<T>>(new_sarray_ptr);
      m_materialized = true;
    }
  }

  template<typename T>
  std::unique_ptr<parallel_iterator<T>> lazy_sarray<T>::get_iterator(size_t dop, bool to_materialize) {
    Dlog_func_entry();

    // for consistent result, we materilize the tree the moment some consumer wants to consume the data
    if (to_materialize) {
      materialize();
    }

    return query_processor::start_exec(m_query_tree, dop);
  }

  // force instantiation of the two functions above
  template
  void lazy_sarray<flexible_type>::materialize();

  template
  std::unique_ptr<parallel_iterator<flexible_type>> lazy_sarray<flexible_type>::get_iterator(size_t dop, bool to_materialize);

  template
  std::unique_ptr<parallel_iterator<std::vector<flexible_type>>> lazy_sarray<std::vector<flexible_type>>::get_iterator(size_t dop, bool to_materialize);
}

#endif
