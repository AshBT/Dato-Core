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
#include <serialization/dir_archive_cache.hpp>
#include <serialization/dir_archive.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/s3_api.hpp>

namespace graphlab {

extern const char* DIR_ARCHIVE_INI_FILE;

dir_archive_cache::~dir_archive_cache() {
  for(auto p: url_to_dir) {
    dir_archive::delete_archive(p.second.directory);
  }
  url_to_dir.clear();
}

dir_archive_cache& dir_archive_cache::get_instance() {
  static dir_archive_cache cache;
  return cache;
}

std::string dir_archive_cache::get_directory(const std::string& url) {
  ASSERT_TRUE(fileio::get_protocol(url) == "s3");
  std::string ini_file = url  + "/" + DIR_ARCHIVE_INI_FILE;
  std::string last_modified = webstor::get_s3_file_last_modified(ini_file);

  // dir_archive.ini does not exist
  if (last_modified.empty()) {
    log_and_throw(std::string("Invalid directory archive. Please make sure the directory contains ") \
        + DIR_ARCHIVE_INI_FILE);
  }

  // directory is cached and up to date
  lock.lock();
  if (url_to_dir.count(url) && last_modified == url_to_dir[url].last_modified) {
    std::string ret = url_to_dir[url].directory;
    lock.unlock();
    return ret;
  }
  lock.unlock();

  //  we have to download the directory and update the cache entry
  std::string temp_dir = graphlab::get_temp_name();
  std::string error = webstor::download_from_s3_recursive(url, temp_dir).get();
  if (!error.empty()) {
    log_and_throw_io_failure(error);
  }
  lock.lock();
  url_to_dir[url].directory = temp_dir;
  url_to_dir[url].last_modified = last_modified;
  lock.unlock();

  return temp_dir;
}

}
