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
#ifndef GRAPHLAB_UNITY_VPI_GENERATOR_CPP
#define GRAPHLAB_UNITY_VPI_GENERATOR_CPP

#include <logger/logger.hpp>
#include <flexible_type/flexible_type.hpp>
#include <unity/query_process/lazy_eval_op_base.hpp>
#include <unity/query_process/lazy_sarray.hpp>
#include <unity/query_process/lazy_sframe.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>

namespace graphlab {

lazy_sframe::lazy_sframe(std::shared_ptr<sframe> sframe_ptr) {
  DASSERT_MSG(sframe_ptr, "sframe pointer is NULL in lazy_sframe constructor");

  m_sframe_ptr = sframe_ptr;
  get_lazy_sarrays_from_sframe(sframe_ptr);

  m_column_names = sframe_ptr->column_names();
  m_column_types = sframe_ptr->column_types();

  DASSERT_MSG(m_lazy_sarrays.size() == m_column_names.size(), "iterator length is not the same as column name length");
  DASSERT_MSG(m_lazy_sarrays.size() == m_column_types.size(), "iterator length is not the same as column type length");
}

lazy_sframe::lazy_sframe(
  const std::vector<std::shared_ptr<lazy_sarray<flexible_type>>>& lazy_sarrays,
  const std::vector<std::string>& column_names) {

  m_sframe_ptr.reset();

  DASSERT_MSG(lazy_sarrays.size() == column_names.size(), "iterator length is not the same as column name length");

  for(const auto &i : column_names) {
    if (i.empty()) {
      m_column_names.push_back(generate_next_column_name());
    } else {
      m_column_names.push_back(i);
    }
  }

  m_lazy_sarrays = lazy_sarrays;

  m_column_types.resize(lazy_sarrays.size());
  for (size_t i = 0; i < lazy_sarrays.size(); i++) {
    m_column_types[i] = lazy_sarrays[i]->get_type();
  }
}

lazy_sframe::lazy_sframe(
  std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> lazy_op,
  const std::vector<std::string>& column_names,
  const std::vector<flex_type_enum>& column_types) {
  m_column_names = column_names;
  m_column_types = column_types;
  m_lazy_operator = lazy_op;
}

std::vector<std::shared_ptr<lazy_sarray<flexible_type>>> lazy_sframe::get_lazy_sarrays() {
  materialize_if_lazy_op();
  return m_lazy_sarrays;
}

size_t lazy_sframe::size() {
  if (m_lazy_operator) {
    if (m_lazy_operator->has_size()) {
      return m_lazy_operator->size();
    }

    materialize();
    return size();
  }

  // sarray will materialize as necessary to get the size
  return m_lazy_sarrays.size() == 0 ? 0 : m_lazy_sarrays[0]->size();
}

std::shared_ptr<graphlab::lazy_sarray<flexible_type>> lazy_sframe::select_column(size_t col_id) const {
  materialize_if_lazy_op();

  if (col_id < num_columns()) {
    // get one column respresentation of the sframe
    return m_lazy_sarrays[col_id];
  } else {
    log_and_throw (std::string("Select column index out of bound. " +
                       std::to_string(col_id)));
  }
}

std::shared_ptr<graphlab::lazy_sarray<flexible_type>> lazy_sframe::select_column(const std::string &name) const {
  size_t col_idx = column_index(name);
  return select_column(col_idx);
}

std::shared_ptr<lazy_sframe> lazy_sframe::select_columns(
    const std::vector<std::string> &names) const {
  if (this->num_columns() < names.size()) {
    log_and_throw("The column does not exist in the sframe");
  }

  std::vector<std::shared_ptr<graphlab::lazy_sarray<flexible_type>>> lazy_sarrays(names.size());

  for(size_t i = 0; i < names.size(); i++) {
    lazy_sarrays[i] = select_column(names[i]);
  }

  return std::make_shared<lazy_sframe>(lazy_sarrays, names);
}

size_t lazy_sframe::column_index(const std::string& column_name) const {
  auto iter = std::find(m_column_names.begin(), m_column_names.end(), column_name);
  if (iter != m_column_names.end()) {
    return (iter) - m_column_names.begin();
  } else {
    throw (std::string("Column name " + column_name + " does not exist."));
  }
}

std::string lazy_sframe::column_name(size_t col) const {
  if (col < m_column_names.size()) {
    return m_column_names[col];
  } else {
    log_and_throw("Unknown column index " + std::to_string(col));
  }
}

/**
* return column types for all columns in the iterator value
**/
flex_type_enum lazy_sframe::column_type(size_t col) const {
  if (col < m_column_names.size()) {
    return m_column_types[col];
  } else {
    log_and_throw("Unknown column index " + std::to_string(col));
  }
}

void lazy_sframe::add_column(
  std::shared_ptr<graphlab::lazy_sarray<flexible_type>> lazy_sarray_ptr,
  std::string column_name) {

  Dlog_func_entry();

  if (column_name.empty()) {
    column_name = generate_next_column_name();
  }

  if (num_columns() > 0 && this->size() != lazy_sarray_ptr->size()) {
    log_and_throw(std::string("Column must have the same # of rows as sframe."));
  }

  if(contains_column(column_name)) {
    log_and_throw(std::string("Attempt to add a column with existing name: "
                      + column_name + ". All column names must be unique!"));
  }

  materialize_if_lazy_op();

  m_column_names.push_back(column_name);
  m_column_types.push_back(lazy_sarray_ptr->get_type());
  m_lazy_sarrays.push_back(lazy_sarray_ptr);
  invalidate_sframe_ptr();
}

bool lazy_sframe::contains_column(std::string name) const {
  auto iter = std::find(m_column_names.begin(), m_column_names.end(), name);
  return iter != m_column_names.end();
}

void lazy_sframe::set_column_name(size_t index, std::string& name) {
  Dlog_func_entry();

  if (index >= m_column_names.size()) {
    log_and_throw("column index is larger than column size.");
  }

  m_column_names[index] = name;

  invalidate_sframe_ptr();
}

void lazy_sframe::remove_column(size_t index) {
  Dlog_func_entry();

  if (index >= m_column_names.size()) {
   log_and_throw("column index is larger than column size.");
  }

  materialize_if_lazy_op();

  m_column_names.erase(m_column_names.begin() + index);
  m_column_types.erase(m_column_types.begin() + index);
  m_lazy_sarrays.erase(m_lazy_sarrays.begin() +index);
  invalidate_sframe_ptr();
}

void lazy_sframe::swap_columns(size_t column_1, size_t column_2) {
  Dlog_func_entry();
  ASSERT_LT(column_1, num_columns());
  ASSERT_LT(column_2, num_columns());

  materialize_if_lazy_op();

  std::swap(m_lazy_sarrays[column_1], m_lazy_sarrays[column_2]);
  std::swap(m_column_names[column_1], m_column_names[column_2]);
  std::swap(m_column_types[column_1], m_column_types[column_2]);
  invalidate_sframe_ptr();
}

std::string lazy_sframe::generate_next_column_name() const{
  std::string name = std::string("X") + std::to_string(num_columns() + 1);
  // Resolve conflicts if the name is already taken
  if(contains_column(name)) {
    name += ".";
    size_t number = 1;
    std::string non_conflict_name = name + std::to_string(number);
    while(contains_column(non_conflict_name) > 0) {
      ++number;
      non_conflict_name = name + std::to_string(number);
    }
    name = non_conflict_name;
  }

  return name;
}

std::shared_ptr<lazy_sframe> lazy_sframe::append(std::shared_ptr<lazy_sframe> other) {
  std::vector<LazySArrayPtrType> columns = get_lazy_sarrays();
  std::vector<LazySArrayPtrType> other_columns = other->get_lazy_sarrays();
  std::vector<LazySArrayPtrType> combined_columns;
  for (size_t i = 0; i < columns.size(); ++i) {
    combined_columns.push_back(columns[i]->append(other_columns[i]));
  }
  return std::make_shared<lazy_sframe>(combined_columns, column_names());
}

void lazy_sframe::get_lazy_sarrays_from_sframe(std::shared_ptr<sframe> sframe_ptr) const {
  m_lazy_sarrays.resize(sframe_ptr->num_columns());

  for(size_t i = 0; i < sframe_ptr->num_columns(); i++) {
    auto sarray_ptr = sframe_ptr->select_column(i);
    auto le_sarray_ptr = std::make_shared<le_sarray<flexible_type>>(sarray_ptr);
    m_lazy_sarrays[i] = std::make_shared<lazy_sarray<flexible_type>>(
      le_sarray_ptr,
      true,
      le_sarray_ptr->get_type());
  }
}

std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> lazy_sframe::get_query_tree() {
  if (m_lazy_operator) {
    return m_lazy_operator;
  } else {
    std::vector<std::shared_ptr<lazy_eval_op_imp_base<flexible_type>>> lazy_trees(m_lazy_sarrays.size());
    size_t i = 0;
    for(auto& lazy_sarray_ptr : m_lazy_sarrays) {
      lazy_trees[i++] = lazy_sarray_ptr->get_query_tree();
    }

    return std::make_shared<le_sframe>(lazy_trees);
  }
}

void lazy_sframe::materialize() const {
  Dlog_func_entry();
  std::lock_guard<mutex> lock(m_lock);

  // already materialized
  if (m_sframe_ptr) return;


  // backed up by a lazy operator
  if (m_lazy_operator) {
    auto sarray_ptr = std::make_shared<lazy_sarray<std::vector<flexible_type>>>(m_lazy_operator, false, flex_type_enum::LIST);
    m_sframe_ptr = graphlab::save_sframe(sarray_ptr, m_column_names, m_column_types);
    m_lazy_operator = NULL;

  } else {

    // backed up by a collection of lazy sarrays, materialize all of them
    std::vector<std::shared_ptr<sarray<flexible_type>>> sarrays(m_lazy_sarrays.size());
    for(size_t i = 0; i < m_lazy_sarrays.size(); i++) {
      sarrays[i] = m_lazy_sarrays[i]->get_sarray_ptr();
    }

    m_sframe_ptr = std::make_shared<sframe>(sarrays, m_column_names);
  }

  get_lazy_sarrays_from_sframe(m_sframe_ptr);
}

std::shared_ptr<lazy_sarray<std::vector<flexible_type>>> lazy_sframe::to_lazy_sarray() {
  return std::make_shared<lazy_sarray<std::vector<flexible_type>>>(
        this->get_query_tree(),
        false,
        flex_type_enum::LIST);
}

} // namespace graphla
#endif
