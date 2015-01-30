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
#ifndef GRAPHLAB_UNITY_QUERY_EXEC_HPP
#define GRAPHLAB_UNITY_QUERY_EXEC_HPP

#include <unity/query_process/lazy_eval_op_base.hpp>

namespace graphlab {

/**
* This file implements functions required for process lazily evaluated query tree and get it ready for evaluation
**/
class query_processor {
public:
  /**
  * Prepare the query tree for execution.
  *
  * The function would goes through the query definition, create a new execution tree by doing
  * a smart cloning of the query definition tree. After the cloning is done, the function returns
  * a parallel iterator to allow caller to consume the query result
  **/
  template<typename T>
  static std::unique_ptr<parallel_iterator<T>> start_exec(
    std::shared_ptr<lazy_eval_op_imp_base<T>> root, size_t dop);

private:
  static void print_tree(std::shared_ptr<lazy_eval_op_base> root, std::ostream& out);
  static void print_node(std::shared_ptr<lazy_eval_op_base> root, std::ostream& out);

  static void assign_node_ids(std::shared_ptr<lazy_eval_op_base> root, size_t& next_id);
  static void clear_node_ids(std::shared_ptr<lazy_eval_op_base> root);

  static std::shared_ptr<lazy_eval_op_base> smart_clone(
    std::shared_ptr<lazy_eval_op_base> root,
    size_t pace_id,
    std::map<std::tuple<size_t, size_t>, std::shared_ptr<lazy_eval_op_base>>& object_dictionary,
    size_t& next_pace_id,
    size_t& next_node_id);

};

}

#endif