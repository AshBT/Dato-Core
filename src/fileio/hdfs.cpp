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
/*
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <fileio/hdfs.hpp>
#include <map>
#include <memory>
#include <system_error>
#include <parallel/mutex.hpp>

namespace graphlab {

/**************************************************************************/
/*                                                                        */
/*              Static method to get a connection to hdfs                 */
/*                                                                        */
/**************************************************************************/
static mutex mtx;

hdfs& hdfs::get_hdfs() {
  mtx.lock();
  static hdfs fs;
  mtx.unlock();
  return fs;
}

hdfs& hdfs::get_hdfs(std::string host, size_t port) {
  mtx.lock();
  static std::map< std::pair<std::string, size_t>, std::shared_ptr<hdfs> > fs_pool;
  if (fs_pool.count({host, port}) == 0) {
    std::shared_ptr<hdfs> shptr = std::make_shared<hdfs>(host, port);
    fs_pool[{host, port}] = shptr;
  }
  mtx.unlock();
  return *(fs_pool[{host, port}]);
}

#ifdef HAS_HADOOP
/**************************************************************************/
/*                                                                        */
/*                    Implentation of graphlab::hdfs                     */
/*                                                                        */
/**************************************************************************/
hdfs::hdfs(const std::string& host, tPort port) {
  logstream(LOG_INFO) << "Connecting to HDFS. Host: " << host << " Port: " << port << std::endl;
  filesystem =  hdfsConnect(host.c_str(), port);
  if (filesystem == NULL) {
    logstream(LOG_ERROR) << "Fail connecting to hdfs" << std::endl;
  }
}

std::vector<std::string> hdfs::list_files(const std::string& path) {
  std::vector<std::pair<std::string, bool> > files = list_files_and_stat(path);
  std::vector<std::string> ret;
  for(auto& file : files) {
    ret.push_back(file.first);
  }
  return ret;
}


std::vector<std::pair<std::string, bool> > hdfs::list_files_and_stat(const std::string& path) {
  ASSERT_TRUE(good());

  std::vector<std::pair<std::string, bool> > files;

  if (!is_directory(path)) {
    return files;
  }

  int num_files = 0;
  hdfsFileInfo* hdfs_file_list_ptr =
    hdfsListDirectory(filesystem, path.c_str(), &num_files);
  // copy the file list to the string array
  for(int i = 0; i < num_files; ++i)  {
    files.push_back(std::make_pair(std::string(hdfs_file_list_ptr[i].mName) ,
                                   hdfs_file_list_ptr[i].mKind == kObjectKindDirectory));
  }
  // free the file list pointer
  hdfsFreeFileInfo(hdfs_file_list_ptr, num_files);
  return files;
}

size_t hdfs::file_size(const std::string& path) {
  ASSERT_TRUE(good());
  hdfsFileInfo *file_info = hdfsGetPathInfo(filesystem, path.c_str());
  if (file_info == NULL) return (size_t)(-1);
  size_t ret_file_size = file_info->mSize;
  hdfsFreeFileInfo(file_info, 1);
  return ret_file_size;
}

bool hdfs::path_exists(const std::string& path) {
  ASSERT_TRUE(good());
  return hdfsExists(filesystem, path.c_str()) == 0;
}

bool hdfs::is_directory(const std::string& path) {
  ASSERT_TRUE(good());
  hdfsFileInfo *file_info = hdfsGetPathInfo(filesystem, path.c_str());
  if (file_info == NULL) return false;
  bool ret = (file_info->mKind == kObjectKindDirectory);
  hdfsFreeFileInfo(file_info, 1);
  return ret;
}



bool hdfs::create_directories(const std::string& path) {
  return hdfsCreateDirectory(filesystem, path.c_str()) == 0;
}

bool hdfs::chmod(const std::string& path, short mode) {
  return hdfsChmod(filesystem, path.c_str(), mode) == 0;
}

bool hdfs::delete_file_recursive(const std::string& path) {
  return hdfsDelete(filesystem, path.c_str(), 1) == 0;
}
/**************************************************************************/
/*                                                                        */
/*             Implementation of graphlab::hdfs::hdfs_device              */
/*                                                                        */
/**************************************************************************/
hdfs::hdfs_device::hdfs_device(const hdfs& hdfs_fs, const std::string& filename, const bool write) :
  filesystem(hdfs_fs.filesystem), bytes_read(0), bytes_written(0) {
  if (!hdfs_fs.good()) {
    return;
  }
  // open the file
  const int flags = write? O_WRONLY : O_RDONLY;
  const int buffer_size = 0; // use default
  const short replication = 0; // use default
  const tSize block_size = 0; // use default;
  file = hdfsOpenFile(filesystem, filename.c_str(), flags, buffer_size,
      replication, block_size);
  logstream(LOG_INFO) << "HDFS open " << filename << " write = " << write << std::endl;
  if (file == NULL) {
    logstream(LOG_ERROR) << "Fail opening file." << std::endl;
    log_and_throw_io_failure("Error opening file.");
  }
}

void hdfs::hdfs_device::close(std::ios_base::openmode mode) {
  if(file == NULL) return;
  if(file->type == OUTPUT && mode == std::ios_base::out) {
    const int flush_error = hdfsFlush(filesystem, file);
    if (flush_error != 0) {
      log_and_throw_io_failure("Error on flush.");
    };
    const int close_error = hdfsCloseFile(filesystem, file);
    file = NULL;
    if (close_error != 0) {
      log_and_throw_io_failure("Error on close.");
    };
  } else if (file->type == INPUT && mode == std::ios_base::in) {
    const int close_error = hdfsCloseFile(filesystem, file);
    if (close_error != 0) {
      log_and_throw_io_failure("Error on close.");
    };
    file = NULL;
  } else {
    return;
  }
}

std::streamsize hdfs::hdfs_device::read(char* strm_ptr, std::streamsize n) {
  std::streamsize ret = hdfsRead(filesystem, file, strm_ptr, n);
  if (ret == -1) {
    log_and_throw_io_failure("Read Error.");
  }
  bytes_read += ret >= 0 ? ret : 0;
  return ret;
}

std::streamsize hdfs::hdfs_device::write(const char* strm_ptr, std::streamsize n) {
  std::streamsize ret = hdfsWrite(filesystem, file, strm_ptr, n);
  if (ret == -1) {
    log_and_throw_io_failure("Write Error.");
  }
  bytes_written += ret >= 0 ? ret : 0;
  return ret;
}


std::streampos hdfs::hdfs_device::seek(std::streamoff off,
                                       std::ios_base::seekdir way,
                                       std::ios_base::openmode) {
  if (way == std::ios_base::beg) {
    hdfsSeek(filesystem, file, off);
  } else if (way == std::ios_base::cur) {
    tOffset offset = hdfsTell(filesystem, file);
    hdfsSeek(filesystem, file, offset + off);
  } else if (way == std::ios_base::end) {
    tOffset offset = hdfsTell(filesystem, file);
    hdfsSeek(filesystem, file, offset + off);
  }
  return hdfsTell(filesystem, file);
}
#endif

}
