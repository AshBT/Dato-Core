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
#ifndef GRAPHLAB_FILEIO_FILE_OWNERSHIP_HANDLE_HPP
#define GRAPHLAB_FILEIO_FILE_OWNERSHIP_HANDLE_HPP
#include <vector>
#include <string>
#include <logger/logger.hpp>
#include <fileio/fs_utils.hpp>
namespace graphlab {
namespace fileio {
/**
 * A simple RAII class which manages the lifespan of one file.
 * On destruction, this file is deleted if marked for deletion.
 */
struct file_ownership_handle {
  file_ownership_handle() = default;
  // deleted copy constructor
  file_ownership_handle(const file_ownership_handle& ) = delete;
  // deleted assignment operator
  file_ownership_handle& operator=(const file_ownership_handle&) = delete;

  /// move constructor
  file_ownership_handle(file_ownership_handle&& other) {
    m_file = other.m_file;
    other.m_file = std::string();
    m_delete_on_destruction = other.m_delete_on_destruction;
  }

  /// move assignment
  file_ownership_handle& operator=(file_ownership_handle&& other) {
    m_file = other.m_file;
    other.m_file = std::string();
    m_delete_on_destruction = other.m_delete_on_destruction;
    return (*this);
  }

  /// construct from one file
  inline file_ownership_handle(const std::string& file, bool delete_on_destruction = true) {
    m_file = file;
    this->m_delete_on_destruction = delete_on_destruction;
  }
  
  void delete_on_destruction() {
    m_delete_on_destruction =  true;
  }

  /// Destructor deletes the owned file if delete_on_destruction is true
  inline ~file_ownership_handle() {
    if (m_delete_on_destruction) {
      try {
        if (!m_file.empty()) {
          logstream(LOG_DEBUG) << "deleting file " << m_file << std::endl;
          fileio::delete_path_impl(m_file);
        }
      } catch (...) {
        logstream(LOG_ERROR) << "Exception on attempted deletion of " << m_file << std::endl;
      }
    }
  }

  std::string m_file;
  bool m_delete_on_destruction;
}; // file_ownership_handle
} // namespace fileio
} // namespace graphlab
#endif
