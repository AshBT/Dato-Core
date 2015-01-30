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
#include <signal.h>
#include <unistd.h>
#include <pwd.h>
#include <sys/types.h>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <set>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <parallel/mutex.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <fileio/fileio_constants.hpp>

namespace graphlab {

static mutex lock;
static std::set<std::string> tempfile_history;
static size_t temp_file_counter = 0;

// import boost filesystem
namespace fs = boost::filesystem;
// forward declarations for some stuff the temp_file_deleter needs
static fs::path get_current_process_temp_directory(size_t idx);

namespace {

/**
 * Reaps orphaned temp directories on construction,
 * deletes all created temporary files on destruction.
 */
class temp_file_deleter {
 public:
   std::set<fs::path> process_temp_directories;
   ~temp_file_deleter() {
     // remove all if possible. Ignore exceptions
     for (auto current_temp_dir: process_temp_directories) {
       try {
         logstream(LOG_DEBUG) << "Recursive deletion of "
                             << current_temp_dir << std::endl;
         fs::remove_all(current_temp_dir);
       } catch (...) { }
     }
   }
};

  static temp_file_deleter deleter;
} // anonymous namespace



/**
 * Returns all the temp directories available.
 */
std::vector<std::string> get_temp_directories() {
  std::vector<std::string> paths;
  boost::algorithm::split(paths,
                          fileio::CACHE_FILE_LOCATIONS, 
                          boost::algorithm::is_any_of(":"));
  return paths;
}

/**
 * Returns the number of temp directories available.
 */
size_t num_temp_directories() {
  return get_temp_directories().size();
}
/**
 * Gets the 'idx''th GraphLab temp directory.
 * 
 * The temp directories are graphlab-[username] appended to the temp directory.
 *
 * idx can be any value in which case the indices will loop around.
 *
 * Ex: Say there are 2 temp directories. /tmp and /var/tmp.
 *
 * get_graphlab_temp_directory(0) will return "/tmp/graphlab-[username]"
 * get_graphlab_temp_directory(1) will return "/var/tmp/graphlab-[username]"
 * and get_graphlab_temp_directory(2) will return "/tmp/graphlab-[username]"
 */
static fs::path get_graphlab_temp_directory(size_t idx) {
  auto temp_dirs = get_temp_directories();
  ASSERT_GT(temp_dirs.size(), 0);

  struct passwd* p = getpwuid(getuid());
  std::string graphlab_name = "graphlab";
  if (p != NULL) graphlab_name = graphlab_name + "-" + p->pw_name;
  fs::path temp_path = temp_dirs[idx % temp_dirs.size()];
  fs::path path(temp_path / graphlab_name);
  return path;
}

/**
 * Finds the temp directory for the current process.
 *
 * The temp directories are graphlab-[username]/procname appended to the 
 * temp directory.
 *
 * idx can be any value in which case the indices will loop around.
 *
 * Ex: Say there are 2 temp directories. /tmp and /var/tmp.
 *
 * get_current_process_temp_directory(0) will return "/tmp/graphlab-[username]/[pid]"
 * get_current_process_temp_directory(1) will return "/var/tmp/graphlab-[username]/[pid]"
 * and get_current_process_temp_directory(2) will return "/tmp/graphlab-[username]/[pid]"
 */
static fs::path get_current_process_temp_directory(size_t idx) {
  return get_graphlab_temp_directory(idx) / std::to_string(getpid());
}



/**
 * Creates the current process's temp directory if it does not already exist.
 *
 * idx can be any value in which case the indices will loop around.
 */
static void create_current_process_temp_directory(size_t idx) {
  fs::path path(get_current_process_temp_directory(idx));
  try {
    if (!fs::is_directory(path)) fs::create_directories(path);
    deleter.process_temp_directories.insert(path);
  } catch (...) {
    logstream(LOG_FATAL) << "Unable to create temporary directories at "
                         << path << std::endl;
  }
}



static void delete_proc_directory(fs::path path) {
  // we could use remove_all but that causes problems, I suspect
  // when multiple processes are trying to reap simultaneously.
  std::vector<fs::path> files_to_delete;
  auto diriter = fs::directory_iterator(path);
  auto enditer = fs::directory_iterator();
  while(diriter != enditer) {
    if (fs::is_regular(diriter->path()) ||
        fs::status(diriter->path()).type() == fs::socket_file) {
      files_to_delete.push_back(diriter->path());
    }
    ++diriter;
  }
  // now delete all the files
  for (auto& p: files_to_delete) {
    // remove if possible. Ignore exceptions
    try {
      fs::remove(p);
      logstream(LOG_DEBUG) << "Deleting " << p << std::endl;
    } catch (...) {
      logstream(LOG_WARNING) << "Unable to delete " << p << std::endl;
    }
  }
  // delete the root
  // remove if possible. Ignore exceptions
  try {
    fs::remove(path);
    logstream(LOG_DEBUG) << "Deleting " << path << std::endl;
  } catch (...) { }
}
/**
 * we will store the temporary files in [tmp_directory]/graphlab/[procid]
 * This searches in graphlab's temp directory for unused temporary files
 * (what procids no longer exist) and deletes them.
 */
void reap_unused_temp_files() {
  // loop through all the subdirectories in get_graphlab_temp_directory()
  // and unlink if the pid does not exist
  size_t temp_dir_size = num_temp_directories();
  for (size_t idx = 0; idx < temp_dir_size; ++idx) {
    try {
      fs::path temp_dir(get_graphlab_temp_directory(idx));
      auto diriter = fs::directory_iterator(temp_dir);
      auto enditer = fs::directory_iterator();

      while(diriter != enditer) {
        auto path = diriter->path();
        if (fs::is_directory(path)) {
          try {
            long pid = std::stol(path.filename().string());
            if (kill(pid, 0) != 0) {
              // PID no longer exists.
              // delete it
              logstream(LOG_EMPH) << "Deleting orphaned temp directory found in "
                                  << path.string() << std::endl;

              delete_proc_directory(path);
            }
          } catch (...) {
            // empty catch. if the path does not parse as an
            // integer, ignore it.
            logstream(LOG_WARNING)
                << "Unexpcted file in GraphLab's temp directory: " << path
                << std::endl;
          }
        }
        ++diriter;
      }
    } catch (...) {
      // Failures are ok. we just stop.
    }
  }
}


std::string get_temp_name() {
  lock.lock();
  // create the directories if they do not exist
  fs::path path(get_current_process_temp_directory(temp_file_counter));
  create_current_process_temp_directory(temp_file_counter);
  // pad the counter with 0's so we get 6 digits
  std::stringstream strm;
  strm << std::setfill('0') << std::setw(6) << temp_file_counter;
  path /= strm.str();
  ++temp_file_counter;

  std::string ret = path.string();
  // write the tempfile into the history
  tempfile_history.insert(ret);
  lock.unlock();

  return ret;
};

/**
 * Deletes the file with the name s
 */
bool delete_temp_file(std::string s) {
  lock.lock();
  // search in the history of all tempfiles generated
  bool found = false;
  // I need to check lower_bound and lower_bound - 1
  auto iter = tempfile_history.lower_bound(s);
  auto begin = tempfile_history.begin();
  auto end = tempfile_history.end();
  // check lower_bound see if it is a prefix
  if (iter != end &&
      boost::starts_with(s, *iter)) {
    found = true;
    tempfile_history.erase(iter);
  }
  // check lower_bound - 1 see if it is a prefix
  else if (iter != begin) {
    iter--;
    if (boost::starts_with(s, *iter)) {
      found = true;
      tempfile_history.erase(iter);
    }
  }
  lock.unlock();

  if (found) {
    logstream(LOG_DEBUG) << "Deleting " << s << "\n";
    return unlink(s.c_str()) == 0;
  } else {
    return false;
  }
}



void delete_temp_files(std::vector<std::string> files) {
  lock.lock();
  // search in the history of all tempfiles generated
  // and unlink all matching files
  std::set<std::string> found_prefixes;
  for(std::string file: files) {
    bool found = false;
    // I need to check lower_bound and lower_bound - 1
    auto iter = tempfile_history.lower_bound(file);
    auto begin = tempfile_history.begin();
    auto end = tempfile_history.end();
    // check lower_bound see if it is a prefix
    if (iter != end && boost::starts_with(file, *iter)) {
      found = true;
      found_prefixes.insert(*iter);
    }
    // check lower_bound - 1 see if it is a prefix
    if (iter != begin) {
      iter--;
      if (boost::starts_with(file, *iter)) {
        found = true;
        found_prefixes.insert(*iter);
      }
    }
    if (found) {
      logstream(LOG_DEBUG) << "Deleting " << file << "\n";
      unlink(file.c_str());
    }
  }
  // now to clear the found prefixes
  for(std::string prefix: found_prefixes) {
    tempfile_history.erase(prefix);
  }
  lock.unlock();
}


} // namespace graphlab
