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
#ifndef GRAPHLAB_FILEIO_FILE_HANDLE_POOL_HPP
#define GRAPHLAB_FILEIO_FILE_HANDLE_POOL_HPP
#include <map>
#include <string>
#include <atomic>
#include <mutex>
#include <fileio/file_ownership_handle.hpp>
namespace graphlab {
namespace fileio {
/**
 * A global file lifespan manager that manages life time for non temporary files
 * that are currently in use by any SArray(including the array index file and the
 * segment files).
 *
 * Permanent files used by SArray can be removed when user saves a SArray to a
 * directory where there is already a SArray saved there. In case there is some
 * SArray actively referencing the files in the directory, we will delay deletion
 * of those files until nobody is referencing those files. new files will be
 * created under the directory to save the new SArray and the directory index
 * will correctly point to the new files.
 *
 * A file_handle object is created for each file that is in use by SArray. All
 * SArrays referencing those files keep a shared pointer to the file handle object.
 * On reading from a directory, SAray registers the files with global file_handle_pool,
 * When SArray is out of scope, the corresponding ref to the file_handle is removed.
 * Once all ref of a given file_handle goes away, the files may or may not be deleted
 * depend on whether or not the files are overwritten.
 *
 * The pool itself keeps a weak pointer to the file_handle object so the files can
 * be deleted when all SArrays referencing the file are gone.
 */
class file_handle_pool {
public:
  /**
   * Singleton retriever
   **/
  static file_handle_pool& get_instance();

  /**
   * Register with file pool that a file is in use.
   * Returns a file_ownership_handle to the caller that can do auto deletion
   * of the file if it goes out of scope.
   *
   * \param file_name The name of the file to be registered
   *
   * Returns a shared pointer to the newly created file_ownership_handle
   **/
  std::shared_ptr<file_ownership_handle> register_file(const std::string& file_name);

  /**
   * Try to mark the file for deletion, returns success if the mark is
   * done successfuly, other wise, the global file pool doesn't know
   * about the file, caller is responsible for deleting the files
   * The marked files will be deleted when all users are out of scope
   *
   * \param file_name The name of the file to be marked for deletion
   **/
  bool mark_file_for_delete(std::string file_name);

private:
  file_handle_pool() {};
  file_handle_pool(file_handle_pool const&) = delete;
  file_handle_pool& operator=(file_handle_pool const&) = delete;

  std::shared_ptr<file_ownership_handle>  get_file_handle(const std::string& file_name);

private:
  std::mutex m_mutex;

  // We need to periodically clear out the map below in order to avoid
  // a memory leak.  Here we clear out all the expired weak pointers
  // every 16K times we register a new file.
  size_t num_file_registers = 0;
  std::map<std::string, std::weak_ptr<file_ownership_handle>> m_file_handles;
};

} // namespace fileio
} // namespace graphlab
#endif
