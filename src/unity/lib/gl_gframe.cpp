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
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/gl_sgraph.hpp>
#include <unity/lib/gl_gframe.hpp>
#include <sgraph/sgraph.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/unity_sgraph.hpp>
namespace graphlab {

std::shared_ptr<unity_sframe> gl_gframe::get_proxy () const {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    return m_sgraph->get_edges();
  } else {
    return m_sgraph->get_vertices();
  }
}

gl_gframe::gl_gframe(gl_sgraph* g, gframe_type_enum t) :
  m_sgraph(g), m_gframe_type(t) {
  DASSERT_TRUE(m_sgraph != NULL);
}

gl_gframe::operator std::shared_ptr<unity_sframe>() const {
  return get_proxy();
}

gl_gframe::operator std::shared_ptr<unity_sframe_base>() const {
  return get_proxy();
}

size_t gl_gframe::size() const { 
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    return m_sgraph->num_edges();
  } else {
    return m_sgraph->num_vertices(); 
  }
}

size_t gl_gframe::num_columns() const { return column_names().size(); }

std::vector<std::string> gl_gframe::column_names() const {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    return m_sgraph->get_edge_fields();
  } else {
    return m_sgraph->get_vertex_fields();
  }
}

std::vector<flex_type_enum> gl_gframe::column_types() const {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    return m_sgraph->get_edge_field_types();
  } else {
    return m_sgraph->get_vertex_field_types();
  }
};

void gl_gframe::add_column(const flexible_type& data, const std::string& name) {
  add_column(gl_sarray::from_const(data, size()), name);
}

void gl_gframe::add_column(const gl_sarray& data, const std::string& name) {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    m_sgraph->add_edge_field(data, name);
  } else {
    m_sgraph->add_vertex_field(data, name);
  }
}

void gl_gframe::add_columns(const gl_sframe& data) {
  for (const auto& k: data.column_names()) {
    add_column(data[k], k);
  }
}

void gl_gframe::remove_column(const std::string& name) {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    if (name == sgraph::SRC_COLUMN_NAME) {
      throw(std::string("Cannot remove \"__src_id\" column"));
    } else if (name == sgraph::DST_COLUMN_NAME) {
      throw(std::string("Cannot remove \"__dst_id\" column"));
    }
    m_sgraph->remove_edge_field(name);
  } else {
    if (name == sgraph::VID_COLUMN_NAME) {
      throw(std::string("Cannot remove \"__id\" column"));
    } 
    m_sgraph->remove_vertex_field(name);
  }
}

void gl_gframe::swap_columns(const std::string& column_1, const std::string& column_2) {
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    m_sgraph->_swap_edge_fields(column_1, column_2);
  } else {
    m_sgraph->_swap_vertex_fields(column_1, column_2);
  }
}

void gl_gframe::rename(const std::map<std::string, std::string>& old_to_new_names) {
  std::vector<std::string> old_names;
  std::vector<std::string> new_names;
  for (const auto& kv : old_to_new_names) {
    old_names.push_back(kv.first);
    new_names.push_back(kv.second);
  }
  if (m_gframe_type == gframe_type_enum::EDGE_GFRAME) {
    m_sgraph->rename_edge_fields(old_names, new_names);
  } else {
    m_sgraph->rename_vertex_fields(old_names, new_names);
  }
}

} // end of graphlab
