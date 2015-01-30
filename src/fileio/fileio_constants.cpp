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
#include <cstdlib>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fileio/fileio_constants.hpp>
#include <fileio/fs_utils.hpp>
#include <globals/globals.hpp>

namespace graphlab {
namespace fileio {


/**
 * Finds the system temp directory.
 *
 * Really, we should be using $TMPDIR or /tmp. But Fedora 18 figured that
 * /tmp should be on tmpfs and thus should only hold small files. Thus we
 * should use /var/tmp when available. But that means we are not following
 * best practices and using $TMPDIR. so.... aargh.
 *
 * This will emit one of the following in order of preference. It will return
 * the first directory which exists. exit(1) on failure.:
 *  - /var/tmp
 *  - $TMPDIR
 *  - /tmp
 */
std::string get_default_temp_directory() {
  boost::filesystem::path path;
  char* tmpdir = getenv("TMPDIR");
  // try $GRAPHLAB_TMPDIR first
  if (boost::filesystem::is_directory("/var/tmp")) {
    path = "/var/tmp";
  } else if(tmpdir && boost::filesystem::is_directory(tmpdir)) {
    path = tmpdir;
  } else {
    path = "/tmp";
  }
  return path.native();
}

static bool check_cache_file_location(std::string val) {
  boost::algorithm::trim(val);
  std::vector<std::string> paths;
  boost::algorithm::split(paths,
                          val, 
                          boost::algorithm::is_any_of(":"));
  if (paths.size() == 0) return false;
  for (std::string path: paths) {
    if (!boost::filesystem::is_directory(path)) return false;
  }
  return true;
}

const std::string CACHE_PREFIX = "cache://";
const std::string TMP_CACHE_PREFIX = "cache://tmp/";
std::string CACHE_FILE_LOCATIONS = get_default_temp_directory();
const size_t FILEIO_INITIAL_CAPACITY_PER_FILE = 1024;
size_t FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE = 128 * 1024 * 1024;
size_t FILEIO_MAXIMUM_CACHE_CAPACITY = 2LL * 1024 * 1024 * 1024;
size_t FILEIO_READER_BUFFER_SIZE = 96 * 1024;
size_t FILEIO_WRITER_BUFFER_SIZE = 96 * 1024;


REGISTER_GLOBAL(int64_t, FILEIO_MAXIMUM_CACHE_CAPACITY, true); 


REGISTER_GLOBAL(int64_t, FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE, true) 


REGISTER_GLOBAL(int64_t, FILEIO_READER_BUFFER_SIZE, false);


REGISTER_GLOBAL(int64_t, FILEIO_WRITER_BUFFER_SIZE, false); 

REGISTER_GLOBAL_WITH_CHECKS(std::string, 
                            CACHE_FILE_LOCATIONS, 
                            true,
                            check_cache_file_location); 
}
}
