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
/**  
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

#ifndef GRAPHLAB_HDFS_HPP
#define GRAPHLAB_HDFS_HPP

// Requires the hdfs library
#ifdef HAS_HADOOP
extern "C" {
  #include <hdfs.h>

// Define structs not defined in hdfs.h

  /*
   * The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream .
   */
  enum hdfsStreamType
  {
    UNINITIALIZED = 0,
    INPUT = 1,
    OUTPUT = 2,
  };

  /**
   * The 'file-handle' to a file in hdfs.
   */ 
  struct hdfsFile_internal {
    void* file;
    enum hdfsStreamType type;
    int flags;
  };

}
#endif

#include <vector>
#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <logger/assertions.hpp>


namespace graphlab {

#ifdef HAS_HADOOP
  class hdfs {
  private:
    /** the primary filesystem object */
    hdfsFS filesystem;
  public:
    /** hdfs file source is used to construct boost iostreams */
    class hdfs_device {
    public: // boost iostream concepts
      typedef char                                          char_type;
      struct category : 
        public boost::iostreams::device_tag, 
        public boost::iostreams::multichar_tag,
        public boost::iostreams::closable_tag,
        public boost::iostreams::bidirectional_seekable { };
        // while this claims to be bidirectional_seekable, that is not true
        // it is only read seekable. Will fail when seeking on write
    private:
      hdfsFS filesystem;

      hdfsFile file;

      size_t bytes_read;
      size_t bytes_written;

    public:
      hdfs_device() : filesystem(NULL), file(NULL), 
                      bytes_read(0), bytes_written(0) { }

      hdfs_device(const hdfs& hdfs_fs, const std::string& filename, const bool write = false);

      //      ~hdfs_device() { if(file != NULL) close(); }

      // Because the device has bidirectional tag, close will be called 
      // twice, one with the std::ios_base::in, followed by out.
      // Only close the file when the close tag matches the actual file type.
      void close(std::ios_base::openmode mode = std::ios_base::openmode());

      /** the optimal buffer size is 0. */
      inline std::streamsize optimal_buffer_size() const { return 0; }

      std::streamsize read(char* strm_ptr, std::streamsize n);

      std::streamsize write(const char* strm_ptr, std::streamsize n);

      bool good() const { return file != NULL; }

      /**
       * Seeks to a different location. 
       */
      std::streampos seek(std::streamoff off, 
                          std::ios_base::seekdir way, 
                          std::ios_base::openmode);

      inline size_t get_bytes_read() const { return bytes_read; }

      inline size_t get_bytes_written() const { return bytes_written; }
    }; // end of hdfs device

    /**
     * The basic file type has constructor matching the hdfs device.
     */
    typedef boost::iostreams::stream<hdfs_device> fstream;

    /**
     * Open a connection to the filesystem. The default arguments
     * should be sufficient for most uses 
     */
    hdfs(const std::string& host = "default", tPort port = 0);

    bool good() const { return filesystem != NULL; } 

    ~hdfs() { 
      if (good()) {
        const int error = hdfsDisconnect(filesystem);
        ASSERT_EQ(error, 0);
      }
    } // end of ~hdfs

    /**
     * Returns the contents of a directory
     */
    std::vector<std::string> list_files(const std::string& path);

    /**
     * Returns the contents of a directory as well as a boolean for every
     * file identifying whether the file is a directory or not.
     */
    std::vector<std::pair<std::string, bool> > list_files_and_stat(const std::string& path);

    /**
     * Returns the size of a given file. Returns (size_t)(-1) on failure.
     */
    size_t file_size(const std::string& path);

    /**
     * Returns true if the given path exists
     */
    bool path_exists(const std::string& path);

    /**
     * Returns true if the given path is a directory, false if it
     * does not exist, or if is a regular file
     */
    bool is_directory(const std::string& path);


    /**
     * Creates a subdirectory and all parent required directories (like mkdir -p)
     * Returns true on success, false on failure.
     */
    bool create_directories(const std::string& path);

    /**
     * Change the permissions of the file.
     */
    bool chmod(const std::string& path, short mode);


    /**
     * Deletes a single file / empty directory.
     * Returns true on success, false on failure.
     */
    bool delete_file_recursive(const std::string& path);

    inline static bool has_hadoop() { return true; }

    static hdfs& get_hdfs();

    static hdfs& get_hdfs(std::string host, size_t port);
  }; // end of class hdfs
#else



  class hdfs {
  public:
    /** hdfs file source is used to construct boost iostreams */
    class hdfs_device {
    public: // boost iostream concepts
      typedef char                                          char_type;
      typedef boost::iostreams::bidirectional_device_tag    category;
    public:
      hdfs_device(const hdfs& hdfs_fs, const std::string& filename,
                  const bool write = false) { 
        logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                             << std::endl;
      }
      void close() { }
      std::streamsize read(char* strm_ptr, std::streamsize n) {
        logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                             << std::endl;
        return 0;
      } // end of read
      std::streamsize write(const char* strm_ptr, std::streamsize n) {
        logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                             << std::endl;
        return 0;
      }
      bool good() const { return false; }

      size_t get_bytes_read() const {
        return 0;
      }

      size_t get_bytes_written() const {
        return 0;
      }
    }; // end of hdfs device

    /**
     * The basic file type has constructor matching the hdfs device.
     */
    typedef boost::iostreams::stream<hdfs_device> fstream;

    /**
     * Open a connection to the filesystem. The default arguments
     * should be sufficient for most uses 
     */
    hdfs(const std::string& host = "default", int port = 0) {
      logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                           << std::endl;
    } // end of constructor

    inline std::vector<std::string> list_files(const std::string& path) {
      logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                           << std::endl;
      return std::vector<std::string>();;
    } // end of list_files

    inline std::vector<std::pair<std::string, bool> > list_files_and_stat(const std::string& path) {
      logstream(LOG_FATAL) << "Libhdfs is not installed on this system." 
                           << std::endl;
      return std::vector<std::pair<std::string, bool>>();
    }

    inline size_t file_size(const std::string& path) {
      return (size_t)(-1);
    }

    /**
     * Returns true if the given path exists
     */
    inline bool path_exists(const std::string& path) {
      return false;
    }

    inline bool is_directory(const std::string& path) {
      return false;
    }

    bool create_directories(const std::string& path) {
      return false;
    }

    bool delete_file_recursive(const std::string& path) {
      return false;
    }

    bool good() const { return false; }

    // No hadoop available
    inline static bool has_hadoop() { return false; }

    static hdfs& get_hdfs();

    static hdfs& get_hdfs(std::string host, size_t port);
  }; // end of class hdfs


#endif

}; // end of namespace graphlab
#endif
