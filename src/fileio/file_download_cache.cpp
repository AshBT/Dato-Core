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
#include <string>
#include <logger/logger.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <fileio/curl_downloader.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/file_download_cache.hpp>
#include <fileio/s3_api.hpp>
#include <fileio/sanitize_url.hpp>
namespace graphlab {

file_download_cache::~file_download_cache() {
  for(auto p: url_to_file) {
    delete_temp_file(p.second.filename);
  }
  url_to_file.clear();
}

std::string file_download_cache::get_file(const std::string& url) {
  // first check if the file has been downloaded.
  // if it has, return the downloaded location
  lock.lock();
  if (url_to_file.count(url)) {
    bool cache_dirty = false;
    if (boost::starts_with(url, "s3://")) {
      std::string last_modified = "";
      try {
        last_modified = webstor::get_s3_file_last_modified(url);
      } catch (...) {
        lock.unlock();
        throw;
      }
      if (last_modified != url_to_file[url].last_modified) {
        cache_dirty = true;
      }
    }
    if (!cache_dirty) {
      std::string ret = url_to_file[url].filename;
      lock.unlock();
      return ret;
    }
  }
  lock.unlock();

  // ok. we need to download the file
  if (boost::starts_with(url, "s3://")) {
    // if it is s3.
    std::string localfile = get_temp_name();
    std::string message = webstor::download_from_s3(url, localfile, "").get();
    size_t i = 0;
    // if message contains a permanentredirect error code, we need to try other endpoints.
    while (boost::algorithm::icontains(message, "PermanentRedirect") && i < webstor::S3_END_POINTS.size()) {
      message = webstor::download_from_s3(url, localfile, "", webstor::S3_END_POINTS[i]).get();
      ++i;
    }
    if (!message.empty()) {
      // OK, we failed.  Let's clean up anything that was downloaded
      // since the real file lives in S3.
      if(std::remove(localfile.c_str()) != 0) {
        logstream(LOG_WARNING) << "Could not delete failed cached file: " << localfile << std::endl;
      }
      log_and_throw_io_failure("Fail to download from " + webstor::sanitize_s3_url(url) + ". "
                                   + webstor::get_s3_error_code(message));
    }
    lock.lock();
    url_to_file[url].filename = localfile;
    try {
      url_to_file[url].last_modified = webstor::get_s3_file_last_modified(url);
    } catch (...) {
      lock.unlock();
      throw;
    }
    lock.unlock();
    return localfile;
  } else {
    // Ok, it is either local regular file, file:///, or remote urls http://.
    // For remote urls, download_url download it into to local file.
    // For local urls, download_url return as is.
    std::string localfile;
    int status; bool is_temp;
    std::tie(status, is_temp, localfile) = download_url(url);
    if (status) {
      log_and_throw_io_failure("Fail to download from " + url + 
                               ". " + get_curl_error_string(status));
    }
    if (is_temp) {
      // if it is a remote file, we check the download status code
      lock.lock();
      url_to_file[url].filename = localfile;
      url_to_file[url].last_modified = "";
      lock.unlock();
      return localfile;
    } else {
      // purely a local file. just return it
      return localfile;
    }
  }
}

void file_download_cache::release_cache(const std::string& url) {
  // look for the file in the url_to_file map and delete it.
  lock.lock();
  if (url_to_file.count(url)) {
    delete_temp_file(url_to_file[url].filename);
    url_to_file.erase(url);
  }
  lock.unlock();
}

file_download_cache& file_download_cache::get_instance() {
  static file_download_cache cache;
  return cache;
}

} // namespace graphlab
