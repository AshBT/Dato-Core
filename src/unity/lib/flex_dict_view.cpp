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
#include <vector>
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/flex_dict_view.hpp>

namespace graphlab {

  flex_dict_view::flex_dict_view(const flex_dict& value) {
    m_flex_dict_ptr = &value;
  }

  flex_dict_view::flex_dict_view(const flexible_type& value) {
    if (value.get_type() == flex_type_enum::DICT) {
      m_flex_dict_ptr = &(value.get<flex_dict>());
      return;
    }

    log_and_throw("Cannot construct a flex_dict_view object from type ");
  }

  const flexible_type& flex_dict_view::operator[](const flexible_type& key) const {
    for(const auto& value : (*m_flex_dict_ptr)) {
      if (value.first == key) {
        return value.second;
      }
    }

    std::stringstream s;
    s << "Cannot find key " << key << " in flex_dict.";
    log_and_throw(s.str());
  }

  bool flex_dict_view::has_key(const flexible_type& key) const {
    for(const auto& value : (*m_flex_dict_ptr)) {
      if (value.first == key) {
        return true;
      }
    }

    return false;
  }

  size_t flex_dict_view::size() const {
    return m_flex_dict_ptr->size();
  }

  const std::vector<flexible_type>& flex_dict_view::keys() {

    // lazy materialization of keys
    if (m_keys.size() != m_flex_dict_ptr->size()) {
      m_keys.reserve(m_flex_dict_ptr->size());
      for(const auto& value : (*m_flex_dict_ptr)) {
        m_keys.push_back(value.first);
      }
    }

    return m_keys;
  }

  const std::vector<flexible_type>& flex_dict_view::values() {
    // lazy materialization of values
    if (m_values.size() != m_flex_dict_ptr->size()) {
      m_values.reserve(m_flex_dict_ptr->size());
      for(const auto& value : (*m_flex_dict_ptr)) {
        m_values.push_back(value.second);
      }
    }

    return m_values;
  }

  flex_dict::const_iterator flex_dict_view::begin() const {
    return m_flex_dict_ptr->begin();
  }

  flex_dict::const_iterator flex_dict_view::end() const {
    return m_flex_dict_ptr->end();
  }
}


