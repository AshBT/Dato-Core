/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef FILEIO_GENERAL_FSTREAM_SINK_HPP
#define FILEIO_GENERAL_FSTREAM_SINK_HPP
#include <memory>
#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <fileio/union_fstream.hpp>
#include <fileio/fileio_constants.hpp>
#include <boost/iostreams/filter/gzip.hpp>
namespace graphlab {
namespace fileio_impl {

/**
 * Implements a general file stream sink device which wraps the
 * union_fstream, and provides automatic gzip decompression capabilities.
 *
 * The general_fstream_sink is NOT thread-safe.
 */
class general_fstream_sink {

  /// The sink device must be copyable; thus the shared_ptr.
  std::shared_ptr<union_fstream> out_file;
  /// The sink device must be copyable; thus the shared_ptr.
  std::shared_ptr<boost::iostreams::gzip_compressor> compressor;

  /// The underlying stream inside the in_file (std stream or hdfs stream)
  std::ostream* underlying_stream = NULL;

  /// Set by the constructor. whether it is gzip compressed.
  bool is_gzip_compressed = false;
 public:
  typedef char        char_type;
  struct category: public boost::iostreams::sink_tag, 
    boost::iostreams::closable_tag, 
    boost::iostreams::multichar_tag,
    boost::iostreams::optimally_buffered_tag {};

  /**
   * Constructs a fstream sink which write to a file. This file can be
   * of any protocol supported by the union_fstream, and may also be 
   * gzip compressed. Gzip compression detection is automatic based on the file
   * extension. (Whether it ends in .gz)
   */
  explicit general_fstream_sink(std::string file);

  /**
   * Constructs a fstream sink which writes to a file. This file can be
   * of any protocol supported by the union_fstream, and may also be 
   * gzip compressed. Gzip compression detection is not performed, and the 
   * gzip_compressed flag is used to enable/disable gzip decompression.
   */
  general_fstream_sink(std::string file, bool gzip_compressed);

  /**
   * Default copy constructor. copied object shares handles with the original
   * object. Required because boost streams requires devices to be copyable.
   * This should really not be used otherwise.
   */
  general_fstream_sink(const general_fstream_sink&) = default;

  /**
   * Default move constructor
   */
  general_fstream_sink(general_fstream_sink&&) = default;


  /// Destructor. Closes the file
  ~general_fstream_sink();


  inline std::streamsize optimal_buffer_size() const {
    return graphlab::fileio::FILEIO_WRITER_BUFFER_SIZE;
  }
  
  /**
   * Returns true if the file is opened
   */
  bool is_open() const;

  /**
   * Attempts to write bufsize bytes into the stream from the buffer.
   * Returns the actual number of bytes read. Returns -1 on failure.
   */
  std::streamsize write(const char* c, std::streamsize bufsize);

  /**
   * Closes all file handles
   */
  void close();

  /**
   * Returns true if the stream is good. See std::ios_base
   */
  bool good() const;

  /**
   * Returns true if the stream is bad. See std::ios_base
   */
  bool bad() const;

  /**
   * Returns true if a stream operation failed. See std::ios_base
   */
  bool fail() const;

  /**
   * Returns the number of physical bytes written so far. This is an estimate,
   * especially if the file is gzip compressed.
   * Returns (size_t)(-1) if there is no file opened.
   */
  size_t get_bytes_written() const;

 private:
  /**
   * The constructors redirect to this function to open the stream.
   */
  void open_file(std::string file, bool gzip_compressed);
};

} // namespace fileio_impl
} // namespace graphlab
#endif
