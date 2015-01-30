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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <regex>
#include <fileio/fs_utils.hpp>
#include <fileio/hdfs.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fileio/fixed_size_cache_manager.hpp>
#include <fileio/file_handle_pool.hpp>
#include <fileio/s3_api.hpp>
namespace graphlab {
namespace fileio {


// import boost filesystem
namespace fs = boost::filesystem;

/**
 * A helper function to parse the hdfs url.
 * Return a tuple of host, port, and path.
 */
std::tuple<std::string, std::string, std::string> parse_hdfs_url(std::string url) {
  if (!boost::starts_with(url, "hdfs://")) {
    return std::make_tuple("", "", "");
  };

  // get the string after hdfs://
  std::string base(url.begin()+7, url.end());
  // find the first '/' character
  if (base.find_first_of("/") == std::string::npos) {
    // Match hdfs://foo.txt
    return std::make_tuple("", "", base);
  } else {
    std::string first_section(base.begin(), base.begin() + base.find_first_of("/"));
    // check if the first section is ip address, with 3 dots. or
    // is a hostname with a : port number
    if (std::count(first_section.begin(), first_section.end(), '.') == 3 ||
        first_section.find_first_of(":") != std::string::npos) {
      std::string path(base.begin() + base.find_first_of("/"), base.end());
      // check if it contains port number
      if (first_section.find_first_of(":") != std::string::npos) {
        // Match hdfs://192.168.0.2:5000/foo.txt
        size_t split = first_section.find_first_of(":");
        std::string host(first_section.begin(), first_section.begin() + split);
        std::string port(first_section.begin() + split + 1, first_section.end());
        return std::make_tuple(host, port, path);
      } else {
        return std::make_tuple(first_section, "", path);
      }
    } else {
      // match hdfs://foo/bar/test
      return std::make_tuple("", "", base);
    }
  }
}

file_status get_file_status(const std::string& path) {
  if(boost::starts_with(path, "hdfs://")) {
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      // fail we we are unable to construct the HDFS object
      if (!hdfs.good()) return file_status::MISSING;
      // we are good. Use the HDFS accessors to figure out what to return
      if (!hdfs.path_exists(hdfspath)) return file_status::MISSING;
      else if (hdfs.is_directory(hdfspath)) return file_status::DIRECTORY;
      else return file_status::REGULAR_FILE;
    } catch(...) {
      // failure for some reason. fail with missing
      return file_status::MISSING;
    }
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    // this is a cache file. it is only REGULAR or MISSING
    try {
      fixed_size_cache_manager::get_instance().get_cache(path);
      return file_status::REGULAR_FILE;
    } catch (...) {
      return file_status::MISSING;
    }
  } else if (boost::starts_with(path, "s3://")) {
    std::pair<bool, bool> ret = webstor::is_directory(path);
    if (ret.first == false) return file_status::MISSING;
    else if (ret.second == false) return file_status::REGULAR_FILE;
    else if (ret.second == true) return file_status::DIRECTORY;
  } else if (is_web_protocol(get_protocol(path))) {
    return file_status::REGULAR_FILE;
    // some other web protocol?
  } else {
    // regular file
    struct stat statout;
    int ret = stat(path.c_str(), &statout);
    if (ret != 0) return file_status::MISSING;
    if (S_ISDIR(statout.st_mode)) return file_status::DIRECTORY;
    else return file_status::REGULAR_FILE;
  }
  return file_status::MISSING;
}


std::vector<std::pair<std::string, file_status>>
get_directory_listing(const std::string& path) {
  std::vector<std::pair<std::string, file_status> > ret;
  if(boost::starts_with(path, "hdfs://")) {
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    if (hdfspath.empty()) return ret;
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      auto dircontents = hdfs.list_files_and_stat(hdfspath);
      for (auto direntry: dircontents) {
        if (direntry.second) {
          ret.push_back({direntry.first, file_status::DIRECTORY});
        } else {
          ret.push_back({direntry.first, file_status::REGULAR_FILE});
        }
      }
      // fail we we are unable to construct the HDFS object
    } catch(...) {
      // failure for some reason. return with nothing
    }
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    // this is a cache file. There is no filesystem.
    // it is only REGULAR or MISSING
    return ret;
  } else if (boost::starts_with(path, "s3://")) {
    webstor::list_objects_response response = webstor::list_directory(path);
    for (auto dir: response.directories) {
      ret.push_back({dir, file_status::DIRECTORY});
    }
    for (auto obj: response.objects) {
      ret.push_back({obj, file_status::REGULAR_FILE});
    }
  } else {
    try {
      fs::path dir(path);
      auto diriter = fs::directory_iterator(path);
      auto enditer = fs::directory_iterator();
      while(diriter != enditer) {
        bool is_directory = fs::is_directory(diriter->path());
        if (is_directory) {
          ret.push_back({diriter->path().native(), file_status::DIRECTORY});
        } else {
          ret.push_back({diriter->path().native(), file_status::REGULAR_FILE});
        }
        ++diriter;
      }
    } catch(...) {
      // failure for some reason. return with nothing
    }
  }
  return ret;
}

bool create_directory(const std::string& path) {
  file_status stat = get_file_status(path);
  if (stat != file_status::MISSING) {
    return false;
  }
  if(boost::starts_with(path, "hdfs://")) {
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      return hdfs.create_directories(hdfspath);
      // fail we we are unable to construct the HDFS object
    } catch(...) {
      // failure for some reason. return with nothing
      return false;
    }
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    // this is a cache file. There is no filesystem.
    return true;
  } else if (boost::starts_with(path, "s3://")) {
    // S3 doesn't need directories
    return true;
  } else {
    try {
      create_directories(fs::path(path));
    } catch (...) {
      return false;
    }
    return true;
  }
  return false;
}

bool delete_path(const std::string& path) {
  file_status stat = get_file_status(path);
  if (stat == file_status::MISSING) {
    return false;
  }

  // For regular file, go through global file pool to make sure we don't
  // delete files that are in use by some SArray
  if (stat == file_status::REGULAR_FILE &&
    fileio::file_handle_pool::get_instance().mark_file_for_delete(path)) {
    logstream(LOG_INFO) << "Attempting to delete " << path 
                        << " but it is still in use. It will be deleted"
                        << " when all references to the file are closed"
                        << std::endl;
    return true;
  } else {
    return delete_path_impl(path);
  }
}

bool delete_path_impl(const std::string& path) {
  file_status stat = get_file_status(path);
  if (stat == file_status::MISSING) {
    return false;
  }
  if(boost::starts_with(path, "hdfs://")) {
    // hdfs only has a recursive deleter. we need to make this safe
    // if the current path is a non-empty directory, fail
    if (stat == file_status::DIRECTORY) {
      if (get_directory_listing(path).size() != 0) {
        return false;
      }
    }
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      return hdfs.delete_file_recursive(hdfspath);
      // fail we we are unable to construct the HDFS object
    } catch(...) {
      // failure for some reason. return with nothing
      return false;
    }
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    try {
      // we ignore recursive here. since the cache can't hold directories
      auto cache_entry = fixed_size_cache_manager::get_instance().get_cache(path);
      fixed_size_cache_manager::get_instance().free(cache_entry);
      return true;
    } catch (...) {
      return false;
    }
  } else if (boost::starts_with(path, "s3://")) {
    return webstor::delete_object(path).empty();
  } else {
    try {
      fs::remove(fs::path(path));
      return true;
    } catch (...) {
      return false;
    }
  }
  return true;
}

bool delete_path_recursive(const std::string& path) {
  file_status stat = get_file_status(path);
  if (stat == file_status::REGULAR_FILE) {
    delete_path(path);
  } else if (stat == file_status::MISSING) {
    return true;
  }
  if(boost::starts_with(path, "hdfs://")) {
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      return hdfs.delete_file_recursive(hdfspath);
      // fail we we are unable to construct the HDFS object
    } catch(...) {
      // failure for some reason. return with nothing
      return false;
    }
  } else if(boost::starts_with(path, "s3://")) {
    return webstor::delete_prefix(path).empty();
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    // recursive deletion not possible with cache
    return true;
  } else {
    try {
      fs::remove_all(fs::path(path));
      return true;
    } catch (...) {
      return false;
    }
  }
}

bool is_writable_protocol(std::string protocol) {
  return protocol == "hdfs" || protocol == "s3" ||
      protocol == "" || protocol == "file" || protocol == "cache";
}

bool is_web_protocol(std::string protocol) {
  return !is_writable_protocol(protocol);
}

std::string get_protocol(std::string path) {
  size_t proto = path.find("://");
  if (proto != std::string::npos) {
    return boost::algorithm::to_lower_copy(path.substr(0, proto));
  } else {
    return "";
  }
}

std::string remove_protocol(std::string path) {
  size_t proto = path.find("://");
  if (proto != std::string::npos) {
    return path.substr(proto + 3); // 3 is the "://"
  } else {
    return path;
  }
}


std::string get_filename(std::string path) {
  size_t lastslash = path.find_last_of("/");
  assert(lastslash != std::string::npos);
  return path.substr(lastslash + 1);
}

std::string get_dirname(std::string path) {
  size_t lastslash = path.find_last_of("/");
  assert(lastslash != std::string::npos);
  return path.substr(0, lastslash);
}


std::string make_relative_path(std::string root_directory, std::string path) {
  // normalize the root directory.
  // If it ends with a "/" drop it.
  // If it is "hdfs://" or "s3://" something like that, its ok even though it
  // ends with a "/"
  if (boost::algorithm::ends_with(root_directory, "://") == false &&
      boost::algorithm::ends_with(root_directory, "/")) {
    root_directory = root_directory.substr(0, root_directory.length() - 1);
  }

  root_directory = root_directory + "/";

  // at this point the root directory should be a proper absolute file path
  // with a "/" at the end;

  if (root_directory.length() > 0 &&
      boost::starts_with(path, root_directory)) {
    // we can relativize!
    return path.substr(root_directory.length());
  } else {
    return path;
  }
}

std::string make_absolute_path(std::string root_directory, std::string path) {
  // normalize the root directory.
  // If it ends with a "/" drop it.
  // If it is "hdfs://" or "s3://" something like that, its ok even though it
  // ends with a "/"
  if (boost::algorithm::ends_with(root_directory, "://") == false &&
      boost::algorithm::ends_with(root_directory, "/")) {
    root_directory = root_directory.substr(0, root_directory.length() - 1);
  }

  root_directory = root_directory + "/";

  // at this point the root directory should be a proper absolute file path
  // with a "/" at the end;

  if (path.empty() || boost::algorithm::contains(path, "://")
      || boost::algorithm::starts_with(path, "/")) {
    // if path "looks" like an absolute path, just return it
    return path;
  } else {
    return root_directory + path;
  }
}

std::regex glob_to_regex(const std::string& glob) { 
  // this is horribly incomplete. But works sufficiently
  std::string glob_pattern(glob);
  boost::replace_all(glob_pattern, "/", "\\/");
  boost::replace_all(glob_pattern, "?", ".");
  boost::replace_all(glob_pattern, "*", ".*");
  return std::regex(glob_pattern);
} 

/**
 * Behaves like python os.path.split.
 * - if url is a directory, return (directory path, "")
 * - if url is a file, return (directory path, filename)
 * - if url is a glob pattern, split into (directory, pattern)
 */
std::pair<std::string, std::string> split_path_elements(const std::string& url, 
                                                        file_status& status) {
  std::pair<std::string, std::string> res;
  if (status == file_status::DIRECTORY) {
    res = std::make_pair(url, "");
  } else {
    res = std::make_pair(get_dirname(url), get_filename(url));
  }
  return res;
}

/**
 * Collects contents of "url" path, testing the rest against the glob
 * return matching file(s) as (url, status) pairs
 */
std::vector<std::pair<std::string, file_status>> get_glob_files(const std::string& url) {
  auto trimmed_url = url;
  boost::algorithm::trim(trimmed_url);
  file_status status = get_file_status(trimmed_url);
  if (status == file_status::REGULAR_FILE) {
    // its a regular file. Ignore the glob and load it
    return {{url, file_status::REGULAR_FILE}};
  }
  std::pair<std::string, std::string> path_elements = split_path_elements(trimmed_url, status);
  std::vector<std::pair<std::string, file_status>> files;

  if (path_elements.second == "") {
    for (auto entry : get_directory_listing(trimmed_url)) {
      files.push_back(make_pair(entry.first, entry.second));
    }
  } else {
    auto glob_regex = glob_to_regex(path_elements.second);
    for (auto entry : get_directory_listing(path_elements.first)) {
      if (std::regex_match(get_filename(entry.first), glob_regex)) {
        files.push_back(make_pair(entry.first, entry.second));
      }
    }
  }
  // unable to glob anything. 
  if (files.size() == 0) files.push_back({url, file_status::MISSING});

  return files;
}

size_t get_io_parallelism_id(const std::string url) {
  std::string protocol = get_protocol(url);

  if (is_web_protocol(protocol) || protocol == "s3" || protocol == "hdfs") {
    // web protocols, s3 and hdfs will be read in parallel always.
    // Those tend to be remote server bound.
    return (size_t)(-1);
  } else if (protocol == "cache") {
    try {
      auto cache_entry = fixed_size_cache_manager::get_instance().get_cache(url);
      if (cache_entry) {
        if (cache_entry->is_pointer()) {
          // if its a cached pointer, we can read in parallel always
          return (size_t)(-1);
        } else if (cache_entry->is_file()) {
          // if it is on file, a bit more work is needed
          // get the temp directories and figure out which one I am a prefix of.
          // each prefix gets its own ID.
          std::string filename = cache_entry->get_filename();
          std::vector<std::string> temp_directories = get_temp_directories();
          for (size_t i = 0;i < temp_directories.size(); ++i) {
            if (boost::starts_with(filename, temp_directories[i])) {
              return i;
            }
          }
        }
      }
    } catch (...) { }
  }
  // all cases, failure cases, missing files, missing cache entries, unknown
  // protocols, local files, etc.
  // assume there is just one local disk.
  return 0;
}

bool try_to_open_file(const std::string url) {
  bool success = true;
  try {
    general_ifstream fin(url);
    success = !fin.fail();
  } catch(...) {
    success = false;
  }
  return success;
}

void copy(const std::string src, const std::string dest) {
  general_ifstream fin(src.c_str());
  general_ofstream fout(dest.c_str());
  std::vector<char> buffer(1024*1024); // 1MB
  while(fin) {
    fin.read(buffer.data(), buffer.size());
    fout.write(buffer.data(), fin.gcount());
  }
}

bool change_file_mode(const std::string path, short mode) {
  file_status stat = get_file_status(path);
  if (stat == file_status::MISSING) {
    return false;
  }

  if(boost::starts_with(path, "hdfs://")) {
    // hdfs
    std::string host, port, hdfspath;
    std::tie(host, port, hdfspath) = parse_hdfs_url(path);
    try {
      // get the HDFS object
      auto& hdfs = host.empty() ?
          graphlab::hdfs::get_hdfs() :
          graphlab::hdfs::get_hdfs(host, std::stoi(port));
      return hdfs.chmod(hdfspath, mode);
      // fail we we are unable to construct the HDFS object
    } catch(...) {
      // failure for some reason. return with nothing
      return false;
    }
  } else if (boost::starts_with(path, fileio::CACHE_PREFIX)) {
    // this is a cache file. There is no filesystem.
    return true;
  } else if (boost::starts_with(path, "s3://")) {
    // S3 doesn't need directories
    return true;
  } else {
    try {
      //permissions(fs::path(path), mode);
      return false;
    } catch (...) {
      return false;
    }
    return true;
  }
  return false;

}

} // namespace fileio
} // namespace graphlab
