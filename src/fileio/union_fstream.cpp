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
#include <logger/logger.hpp>
#include <fileio/union_fstream.hpp>
#include <fileio/cache_stream.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/curl_downloader.hpp>
#include <fileio/s3_api.hpp>
#include <fileio/file_download_cache.hpp>
#include <fileio/sanitize_url.hpp>
#include <boost/algorithm/string.hpp>
#include <fileio/fs_utils.hpp>
#include <fileio/hdfs.hpp>
namespace graphlab {

/**
 * Returns a callback which uploads from local source url to s3 target url.
 *
 * Returns the local file name.
 */
std::function<void(void)> get_s3_upload_callback (std::string source,
                                                  std::string target,
                                                  std::string proxy) {
  return [source, target, proxy]() {
          logstream(LOG_INFO) << "Union fstream::s3_upload_callback: " << "local = " << source
            << " remote = " << sanitize_url(target) << " proxy = " << proxy << std::endl;
          std::string msg = webstor::upload_to_s3(source, target, proxy).get();
          size_t i = 0;
          // if message contains a permanentredirect error code, we need to try other endpoints.
          while (boost::algorithm::icontains(msg, "PermanentRedirect") && i < webstor::S3_END_POINTS.size()) {
            msg = webstor::upload_to_s3(source, target, proxy, webstor::S3_END_POINTS[i]).get();
            ++i;
          }
          if (!msg.empty()) {
            std::string error_message = "Fail uploading to " + sanitize_url(target)
                + ". " + webstor::get_s3_error_code(msg);
            logstream(LOG_WARNING) << error_message << std::endl;
            log_and_throw_io_failure(error_message);
          }
        };
}

/**
 * A simple union of std::fstream and graphlab::hdfs::fstream, and graphlab::fileio::cache_stream.
 */
union_fstream::union_fstream(std::string url,
                             std::ios_base::openmode mode,
                             std::string proxy) : localfile(url) {
  input_stream = NULL;
  output_stream = NULL;

  if ((mode & std::ios_base::in) && (mode & std::ios_base::out)) {
    // If the mode is both in and out, raise exception.
    log_and_throw_io_failure("Invalid union_fstream open mode: cannot be both in and out");
  }
  else if (!(mode & std::ios_base::in) && !(mode & std::ios_base::out)) {
    // If the mode is neither in nor out, raise exception.
    log_and_throw_io_failure("Invalid union_fstream open mode: cannot be neither in nor out");
  }

  bool is_output_stream = (mode & std::ios_base::out);

  if(boost::starts_with(url, "hdfs://")) {
    type = HDFS;
    std::string host, port, path;
    std::tie(host, port, path) = fileio::parse_hdfs_url(url);
    logstream(LOG_INFO) << "HDFS URL parsed: Host: " << host << " Port: " << port
                        << " Path: " << path << std::endl;
    if (host.empty() && port.empty() && path.empty()) {
      log_and_throw_io_failure("Invalid hdfs url: " + url);
    }
    try {
      auto& hdfs = host.empty() ? graphlab::hdfs::get_hdfs() : graphlab::hdfs::get_hdfs(host, std::stoi(port));
      ASSERT_TRUE(hdfs.good());
      if (is_output_stream) {
        output_stream = new graphlab::hdfs::fstream(hdfs, path, true);
      } else {
        input_stream = new graphlab::hdfs::fstream(hdfs, path, false);
      }
    } catch(...) {
      log_and_throw_io_failure("Unable to open " + url);
    }
  } else if (boost::starts_with(url, fileio::CACHE_PREFIX)) {
    type = CACHE;
    if (is_output_stream) {
      output_stream = new fileio::ocache_stream(url);
    } else {
      input_stream = new fileio::icache_stream(url);
    }
  } else {
    type = STD;
    // Check if it is an s3 url.
    if (boost::starts_with(url, "s3://")) {
      if (!is_output_stream) {
        // input file stream. download it
        localfile = file_download_cache::get_instance().get_file(url);
      } else {
        // output file stream.
        localfile = get_temp_name();
        register_close_callback(get_s3_upload_callback(localfile, url, proxy));
      }
    } else {
      if (!is_output_stream) {
        localfile = file_download_cache::get_instance().get_file(url);
        // check if we can open it
        std::ifstream fin(localfile);
        if (!fin.good()) {
          fin.close();
          log_and_throw_io_failure("Cannot open " + localfile + " for reading");
        }
        fin.close();
      } else {
        // Output stream must be a local openable file.
        std::ofstream fout(localfile.c_str());
        if (!fout.good()) {
          log_and_throw_io_failure("Cannot open " + url + " for writing");
        }
      }
    }
    if (is_output_stream) {
      output_stream = new std::fstream(localfile, mode);
    } else {
      input_stream = new std::fstream(localfile, mode);
    }
  }

  if (is_output_stream) {
    ASSERT_TRUE(output_stream->good());
  } else {
    ASSERT_TRUE(input_stream->good());
  }
}

/// Destructor
union_fstream::~union_fstream() {
  if (input_stream)
    delete input_stream;
  if (output_stream)
    delete output_stream;
}

/// Returns the current stream type. Whether it is a HDFS stream or a STD stream
union_fstream::stream_type union_fstream::get_type() const {
  return type;
}

std::istream* union_fstream::get_istream () {
  ASSERT_TRUE(input_stream != NULL);
  return input_stream;
}

std::ostream* union_fstream::get_ostream() {
  ASSERT_TRUE(output_stream != NULL);
  return output_stream;
}

/**
 * Returns the filename used to construct the union_fstream
 */
std::string union_fstream::get_name() const {
  return localfile;
}

/**
 * Register a callback function that will be called on close.
 */
void union_fstream::register_close_callback(std::function<void()> fn) {
  close_callback = fn;
}

/**
 * Closes the file stream, and call all the registered call back functions.
 *
 * Throws exception if call back function fails.
 */
void union_fstream::close() {
  switch (get_type()) {
    case HDFS:
      if (input_stream)
        dynamic_cast<hdfs::fstream*>(input_stream)->close();
      if (output_stream)
        dynamic_cast<hdfs::fstream*>(output_stream)->close();
      break;
    case STD:
      if (input_stream)
        dynamic_cast<std::fstream*>(input_stream)->close();
      if (output_stream)
        dynamic_cast<std::fstream*>(output_stream)->close();
      break;
    case CACHE:
      if (input_stream)
        dynamic_cast<fileio::icache_stream*>(input_stream)->close();
      if (output_stream)
        dynamic_cast<fileio::ocache_stream*>(output_stream)->close();
      break;
    default:
      logstream(LOG_WARNING) << "Unknown stream type for file: " << localfile << std::endl;;
  }
  if (close_callback) {
    close_callback();
  }
}

/**
 * Returns the file size of the opened file.
 * Returns (size_t)(-1) if there is no file opened, or if there is an
 * error obtaining the file size.
 */
size_t union_fstream::file_size() {
  // gets the file size. But lets do so without changing the current stream
  // positions.
  // With HDFS we have a HDFS wrapper object which can get the file size for
  // us. With standard file streams, we do so via ifstream.
  std::ifstream fin;
  switch (get_type()) {
    case HDFS:
        {
        std::string host, port, path;
        std::tie(host, port, path) = fileio::parse_hdfs_url(localfile);
        logstream(LOG_INFO) << "HDFS URL parsed: Host: " << host << " Port: " << port
                            << " Path: " << path << std::endl;
        if (host.empty() && port.empty() && path.empty()) {
          log_and_throw_io_failure("Invalid hdfs url: " + localfile);
        }
        try {
          auto& hdfs = host.empty() ? graphlab::hdfs::get_hdfs() : graphlab::hdfs::get_hdfs(host, std::stoi(port));
          size_t ret = hdfs.file_size(path);
          if (ret == (size_t)(-1)) log_and_throw_io_failure("Unable to open " + localfile);
          return ret;
        } catch(...) {
          log_and_throw_io_failure("Unable to open " + localfile);
        }
      }
    case STD:
      fin.open(localfile.c_str());
      if (fin.fail()) return (size_t)(-1);
      fin.seekg(0, std::ios::end);
      return fin.tellg();
    case CACHE:
      ASSERT_TRUE(input_stream != NULL);
      return (*dynamic_cast<fileio::icache_stream*>(input_stream))->file_size();
    default:
      logstream(LOG_WARNING) << "Unknown stream type for file: "
        << localfile << std::endl;
      return (size_t)(-1);
  }
}

/**
 * Returns the number of bytes read from the file so far.
 * Note that due to buffering, this may be an overestimate of the current
 * file position.
 */
size_t union_fstream::get_bytes_read() {
  ASSERT_TRUE(input_stream != NULL);
  switch (get_type()) {
    case HDFS:
      {
        graphlab::hdfs::fstream* hdfs_fstream = dynamic_cast<graphlab::hdfs::fstream*>(input_stream);
        return (*hdfs_fstream)->get_bytes_read();
      }
    case STD:
      return dynamic_cast<std::fstream*>(input_stream)->tellg();
    case CACHE:
      return (*dynamic_cast<fileio::icache_stream*>(input_stream))->get_bytes_read();
    default:
      logstream(LOG_WARNING) << "Unknown stream type for file: "
        << localfile << std::endl;
      return (size_t)(-1);
  }
}


/**
 * Returns the number of bytes written to the file so far.
 * Note that due to buffering, this may be an underestimate of the current
 * file position.
 */
size_t union_fstream::get_bytes_written() {
  ASSERT_TRUE(output_stream != NULL);
  switch (get_type()) {
    case HDFS:
      {
        graphlab::hdfs::fstream* hdfs_fstream = dynamic_cast<graphlab::hdfs::fstream*>(output_stream);
        return (*hdfs_fstream)->get_bytes_written();
      }
    case STD:
      return dynamic_cast<std::fstream*>(output_stream)->tellp();
    // case CACHE:
      // return dynamic_cast<fileio::ocache_stream*>(output_stream)->get_bytes_written();
    default:
      logstream(LOG_WARNING) << "Unknown stream type for file: "
        << localfile << std::endl;
      return (size_t)(-1);
  }
}

} // namespace graphlab

