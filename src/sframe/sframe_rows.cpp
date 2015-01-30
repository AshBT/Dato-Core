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
#include <logger/assertions.hpp>
#include <sframe/sframe_rows.hpp>

namespace graphlab {

sframe_rows_range sframe_rows::get_range() {
  return sframe_rows_range(*this);
}

sframe_rows::~sframe_rows() {
  reset();
}

void sframe_rows::reset() {
  m_columns.clear();
}

void sframe_rows::add_decoded_rows(
    sframe_rows::decoded_rows_type decoded_rows) {
  if (m_columns.size() == 0) {
    // first column group
    m_num_rows = decoded_rows.first.size();
    m_num_columns = decoded_rows.second;
  } else {
    // not the first column group. make sure rows line up
    ASSERT_EQ(decoded_rows.first.size(), m_num_rows);
    m_num_columns += decoded_rows.second;
  }
  m_columns.push_back(column_group_type());
  m_columns.back() = std::move(decoded_rows);
}

void sframe_rows::add_decoded_column(
    sframe_rows::decoded_column_type decoded_column) {
  if (m_columns.size() == 0) {
    // first column group
    m_num_rows = decoded_column.size();
    m_num_columns = 1;
  } else {
    ASSERT_EQ(decoded_column.size(), m_num_rows);
    ++m_num_columns;
  }
  m_columns.push_back(column_group_type());
  m_columns.back() = std::move(decoded_column);
}

void sframe_rows::recalculate_size() const {
  m_num_columns = 0;
  m_num_rows = 0;
  for (size_t i = 0;i < m_columns.size(); ++i) {
    if (m_columns[i].m_contents == block_contents::DECODED_ROWS) {
      m_num_columns += m_columns[i].m_decoded_rows.second;
      m_num_rows = m_columns[i].m_decoded_rows.first.size();
    } else if (m_columns[i].m_contents == block_contents::DECODED_COLUMN) {
      ++m_num_columns;
      m_num_rows = m_columns[i].m_decoded_column.size();
    }

  }
}

sframe_rows_range::sframe_rows_range(sframe_rows& rows):m_source(rows) { 
  m_num_rows = m_source.num_rows();
  m_num_columns = m_source.num_columns();
  compute_column_pos();
}


void sframe_rows_range::compute_column_pos() {
  size_t i = 0;
  for (auto& column_group: m_source.m_columns) {
    switch(column_group.m_contents) {
     case sframe_rows::block_contents::DECODED_ROWS:
       for (size_t j = 0;j < column_group.m_decoded_rows.second; ++j) {
         m_column_pos.push_back({i, j});
       }
       break;
     case sframe_rows::block_contents::DECODED_COLUMN:
       m_column_pos.push_back({i, 0});
     default:
       break;
    }
    ++i;
  }
}

} // namespace graphlab
