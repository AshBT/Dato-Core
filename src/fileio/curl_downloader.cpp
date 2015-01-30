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
#include <tuple>
#include <iostream>
#include <logger/logger.hpp>
#include <fileio/curl_downloader.hpp>
#include <fileio/temp_files.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <cppipc/server/cancel_ops.hpp>
extern "C" {
#include <curl/curl.h>
}

namespace fs = boost::filesystem;

namespace graphlab {

size_t download_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
  FILE *output_file = (FILE *)userdata;
  size_t ret = fwrite(ptr, size, nmemb, output_file);

  if(cppipc::must_cancel()) {
    ret = 0;
    logprogress_stream << "Download cancelled by user.\n";
  }

  return ret;
}

int download_url(std::string url, std::string output_file) {
  // source modified from libcurl code example
  CURL *curl = curl_easy_init();
  logprogress_stream << "Downloading " << url << " to " << output_file << "\n";
  if(curl) {
    FILE* f = fopen(output_file.c_str(), "w");
    if (f == NULL) return errno;
    CURLcode res;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    /* example.com is redirected, so we tell libcurl to follow redirection */ 
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &download_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)(f));
    /* Perform the request, res will get the return code */ 
    res = curl_easy_perform(curl);
    /* Check for errors */ 
    if(res != CURLE_OK) {
      logprogress_stream << "Failed to download " << url << ": " << curl_easy_strerror(res);
    }
    /* always cleanup */ 
    curl_easy_cleanup(curl);
    fclose(f);
    return res;
  }
  return -1;
}

std::tuple<int, bool, std::string> download_url(std::string url) {
  // If it is a native path, then return immediately.
  if (!boost::algorithm::contains(url, "://")) {
    return std::make_tuple(0, false, url);
  }

  std::ifstream fin(url.c_str());
  // now, check for the file:// protocol header and see if we can access
  // it as a local file
  if (boost::starts_with(url, "file://")) {
    std::string stripped = url.substr(strlen("file://"));
    if (fs::is_directory(fs::path(stripped))) {
      return std::make_tuple(0, false, stripped);
    } else {
      fin.open(stripped.c_str());
      if (fin.good()) {
        return std::make_tuple(0, false, stripped); // strip the file://
      }
      // if we cannot open, it could simply be because the file name has escape 
      // characters in it which we do not understand. For instance:
      // file:///home/ylow/test%20x.txt (where %20 should be a space character).
      // In this case, curl will still be able to understand that it.
      // Thus even if we can't interpret the file, we should try handing it
      // off to curl to see what happens.
    }
  }

  // All local access failed. Now get curl to download it
  std::string tempname = get_temp_name();
  if (tempname == "") return std::make_tuple(-1, false, "");
  // attach the trailing file extension if any
  size_t lastdot = url.find_last_of(".");
  if (lastdot != std::string::npos) {
    tempname = tempname + url.substr(lastdot);
  }
  int status = download_url(url, tempname);
  // failed to download
  // delete the temporary file and return failure
  if (status != 0) {
    delete_temp_file(tempname);
    return std::make_tuple(status, false, "");
  } else {
    return std::make_tuple(status, true, tempname);
  }
}

std::string get_curl_error_string(int status) {
  return curl_easy_strerror(CURLcode(status));
}

} // namespace graphlab
