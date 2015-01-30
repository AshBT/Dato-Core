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
#ifndef GRAPHLAB_UNITY_AUTO_CLOSE_SARRAY
#define GRAPHLAB_UNITY_AUTO_CLOSE_SARRAY

namespace graphlab {
/**
* helper class to auto open SArray for write in constructor and and then close
* the sarray as it is out of scope
**/
class auto_close_sarrays {
public:
  auto_close_sarrays(std::vector<flex_type_enum> column_types) {
    m_columns.resize(column_types.size());
    for (size_t col_idx  = 0; col_idx < column_types.size(); col_idx++) {
      m_columns[col_idx] = std::make_shared<sarray<flexible_type>>();
      m_columns[col_idx]->open_for_write();
      m_columns[col_idx]->set_type(column_types[col_idx]);
    }
  }

  ~auto_close_sarrays() {
    close();
  }

  void close() {
    if (!m_closed) {
      for(auto column: m_columns) {
        column->close();
      }
      m_closed = true;
    }
  }

  std::vector<std::shared_ptr<sarray<flexible_type>>> get_sarrays() {
    return m_columns;
  }

  std::vector<std::shared_ptr<lazy_sarray<flexible_type>>> get_lazy_sarrays() {
    close();
    std::vector<std::shared_ptr<lazy_sarray<flexible_type>>> ret_columns;
    ret_columns.reserve(m_columns.size());

    for(auto column: m_columns) {
      ret_columns.push_back(std::make_shared<lazy_sarray<flexible_type>>(
          std::make_shared<le_sarray<flexible_type>>(column),
          true,
          column->get_type()));
    }

    return ret_columns;
  }

private:
  std::vector<std::shared_ptr<sarray<flexible_type>>> m_columns;
  bool m_closed = false;
};

}
#endif
