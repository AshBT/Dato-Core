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
#ifndef GRAPHLAB_FILEIO_FILE_HANDLE_POOL_CPP
#define GRAPHLAB_FILEIO_FILE_HANDLE_POOL_CPP

#include <boost/algorithm/string.hpp>
#include <fileio/file_ownership_handle.hpp>
#include <fileio/file_handle_pool.hpp>
#include <fileio/sanitize_url.hpp>
namespace graphlab {
namespace fileio {

file_handle_pool& file_handle_pool::file_handle_pool::get_instance() {
  static file_handle_pool instance;
  return instance;
}

std::shared_ptr<file_ownership_handle> file_handle_pool::register_file(const std::string& file_name) {
  std::lock_guard<std::mutex> guard(this->m_mutex);
  std::shared_ptr<file_ownership_handle> ret = get_file_handle(file_name);

  if (!ret) {
    logstream(LOG_DEBUG) << "register_file_handle for file " << sanitize_url(file_name) << std::endl;

    
    ret = std::make_shared<file_ownership_handle>(file_name, boost::algorithm::starts_with(file_name, "cache://"));
    m_file_handles[file_name] = ret;
  }

  /**  This seems to be the safest way to do this.  Ideally, we would
   *   like file_ownership_handle to take care of this.  However, it
   *   is not certain that the pool will be around when that object is
   *   destroyed, so that brings up a number of possible rare corner
   *   cases to check for.  Doing this is simplest for now. 
   */
  if( (++this->num_file_registers) % (16*1024) == 0) {
    for(auto it = m_file_handles.begin(); it != m_file_handles.end();) {
      if(it->second.expired()) {
        // Advances to the next element, or m_file_handles.end(); 
        it = m_file_handles.erase(it);
      } else {
        ++it;
      }
    }
  }

  return ret;
}

bool file_handle_pool::mark_file_for_delete(std::string file_name) {
  Dlog_func_entry();

  // file is not registered with global pool, let caller
  // do the actual deletion
  std::lock_guard<std::mutex> guard(this->m_mutex);

  std::shared_ptr<file_ownership_handle> handle_ptr = get_file_handle(file_name);
  if (!handle_ptr) {
    return false;
  } else {
    // Mark the file for deletion and the file will be deleted when going out-of-scope
    logstream(LOG_DEBUG) << "mark file " << file_name << " for deletion " << std::endl;
    handle_ptr->delete_on_destruction();
    return true;
  }
}

std::shared_ptr<file_ownership_handle> file_handle_pool::get_file_handle(const std::string& file_name) {
  std::shared_ptr<file_ownership_handle> ret;
  bool file_in_pool = m_file_handles.find(file_name) != m_file_handles.end();
  if (!file_in_pool) return ret;

  if (!(ret = m_file_handles[file_name].lock())) {
    m_file_handles.erase(file_name);
  }

  return ret;
}


} // namespace fileio
} // namespace graphlab
#endif
