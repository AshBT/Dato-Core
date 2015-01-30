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
#ifndef GRAPHLAB_SGRAPH_SGRAPH_COMPUTE_VERTEX_BLOCK_HPP
#define GRAPHLAB_SGRAPH_SGRAPH_COMPUTE_VERTEX_BLOCK_HPP
#include <map>
#include <vector>
#include <mutex>
#include <algorithm>
#include <sframe/sframe.hpp>
#include <flexible_type/flexible_type.hpp>
namespace graphlab {
namespace sgraph_compute {

/**
 * Represents a partition of vertices which is held in memory.
 */
template <typename SIterableType>
class vertex_block {
 public:
  /**
   * Loads an SFrame/SArray into memory (accessible directly via m_vertices)
   * if not already loaded.
   */
  void load_if_not_loaded(const SIterableType& sf) {
    if (!m_loaded) {
      load_impl(sf);
      m_loaded = true;
    }
  }

  /**
   * Loads an SFrame/SArray into memory (accessible directly via m_vertices)
   * reloading it if it has already been loaded.
   */
  void load(const SIterableType& sf) {
    load_impl(sf);
    m_loaded = true;
  }

  void flush(SIterableType& outputsf) {
    std::copy(m_vertices.begin(), m_vertices.end(), 
              outputsf.get_output_iterator(0));
    outputsf.close();
  }

  void flush(SIterableType& outputsf, const std::vector<size_t>& mutated_field_index) {
    auto out = outputsf.get_output_iterator(0);
    std::vector<flexible_type> temp(mutated_field_index.size());
    for (const auto& value: m_vertices) {
      for (size_t i = 0; i < mutated_field_index.size(); ++i) {
        temp[i] = value[mutated_field_index[i]];
      }
      *out = temp;
      ++out;
    }
    outputsf.close();
  }


  /**
   * Unloads the loaded data, releasing all memory used.
   */
  void unload() {
    m_loaded = false;
    m_vertices.clear();
    m_vertices.shrink_to_fit();
  }

  /**
   * Returns true if the SFrame is loaded. False otherwise.
   */
  bool is_loaded() {
    return m_loaded;
  }


  /**
   * Returns true if the SFrame is loaded. False otherwise.
   */
  bool is_modified() {
    return m_modified;
  }

  /**
   * Sets the modified flag
   */
  void set_modified_flag() {
    m_modified = true;
  }

  /**
   * Clears the modified flag
   */
  void clear_modified_flag() {
    m_modified = false;
  }


  typename SIterableType::value_type& operator[](size_t i) {
    return m_vertices[i];
  }

  const typename SIterableType::value_type& operator[](size_t i) const {
    return m_vertices[i];
  }
  /// The loaded data
  std::vector<typename SIterableType::value_type> m_vertices;

 private:
  /// Internal load implementation
  void load_impl(const SIterableType& sf) {
    if (m_last_index_file != sf.get_index_file() || !m_reader) {
      m_last_index_file = sf.get_index_file();
      m_reader = std::move(sf.get_reader());
    }
    m_reader->read_rows(0, m_reader->size(), m_vertices);
  }
  /// Flag denoting if the data has been loaded
  bool m_loaded = false;
  /// Flag denoting modification
  bool m_modified = false;
  // cache the reader
  std::string m_last_index_file;
  std::unique_ptr<typename SIterableType::reader_type> m_reader;
};


} // sgraph_compute
} // graphlab 
#endif
