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
#ifndef GRAPHLAB_SFRAME_SARRAY_V2_BLOCK_WRITER_HPP
#define GRAPHLAB_SFRAME_SARRAY_V2_BLOCK_WRITER_HPP
#include <stdint.h>
#include <vector>
#include <fstream>
#include <tuple>
#include <parallel/pthread_tools.hpp>
#include <parallel/atomic.hpp>
#include <fileio/general_fstream.hpp>
#include <sframe/sarray_index_file.hpp>
#include <flexible_type/flexible_type.hpp>
#include <util/buffer_pool.hpp>
#include <sframe/sarray_v2_block_types.hpp>

namespace graphlab {
namespace v2_block_impl {

/**
 * Provides the file writing implementation for the v2 block format.
 * See the sarray_v2_block_manager for details on the format.
 *
 * Basic usage is:
 * \code
 * block_writer writer;
 * writer.init("index", #segments, #columns);
 * for i = 0 to  #segments:
 *   writer.open_segment(i, filename)
 * 
 * // now you perform repeat calls to the following functions
 * // to write blocks to the segments/columns
 * writer.write_block(...)
 * writer.write_typed_block(...)
 *
 * // close all writes   
 * for i = 0 to  #segments:
 *   writer.close_segment(i)
 * 
 * // output the array group index file
 * writer.write_index_file()
 * \endcode
 */
class block_writer {
 public:

  /**
   * Opens a block writer with a target index file, the number of segments
   * to write, and the number of columns to write.
   */
  void init(std::string group_index_file,
            size_t num_segments, 
            size_t num_columns);

  /**
   * Opens a segment, using a given file name.
   */
  void open_segment(size_t segment_id,
                    std::string filename);

  /**
   * Writes a block of data into a segment.
   *
   * \param segmentid The segment to write to
   * \param data Pointer to the data to write
   * \param block_info Metadata about the block. 
   *
   * The only fields in block_info which *must* be filled is block_size and
   * num_elem. 
   * Returns the actual number of bytes written.
   */
  size_t write_block(size_t segment_id,
                   size_t column_id,
                   char* data,
                   block_info block);

  /**
   * Writes a block of data into a segment.
   *
   * \param segment_id The segment to write to
   * \param data Reference to the data to write
   * \param block_info Metadata about the block. 
   *
   * No fields of block_info are required at the moment.
   * Returns the actual number of bytes written.
   */
  size_t write_typed_block(size_t segment_id,
                         size_t column_id,
                         const std::vector<flexible_type>& data,
                         block_info block);


  /**
   * Writes a block of arbitrary contents. Direct serialization is used.
   */
  template <typename T>
  size_t write_block(size_t segment_id,
                   size_t column_id,
                   const std::vector<T>& data,
                   block_info block) {
    auto serialization_buffer = m_buffer_pool.get_new_buffer();
    oarchive oarc(*serialization_buffer);
    oarc << data;
    block.block_size = oarc.off;
    block.num_elem = data.size();
    size_t ret = write_block(segment_id, column_id, serialization_buffer->data(), block);
    m_buffer_pool.release_buffer(std::move(serialization_buffer));
    return ret;
  }
  /**
   * Closes the segment file
   */
  void close_segment(size_t segment_id);

  /**
   * Gets a modifiable reference to the index information for the data written.
   * If the segment is not closed yet, partial results will be provided.
   */
  group_index_file_information& get_index_info();

  /**
   * Writes the index file
   */
  void write_index_file();
 private:

  /// Pool of buffers used for compression, etc.
  buffer_pool<std::vector<char> > m_buffer_pool;
  /// The output files for each open segment
  std::vector<std::shared_ptr<general_ofstream> > m_output_files;
  /// Locks on the output segments
  std::vector<graphlab::mutex> m_output_file_locks;
  /// Number of bytes written to each output segments
  std::vector<size_t> m_output_bytes_written;

  group_index_file_information m_index_info;

  /** 
   * A vector of all the block information is stuck in the footer of the file
   * for each segment, for each column, the collection of blocks.
   * Once inited, this array is never modified and is safe for concurrent 
   * reads.
   * blocks[segment_id][column_id][block_id]
   */
  std::vector<std::vector<std::vector<block_info> > > m_blocks;

  /// For each segment, for each column the number of rows written so far
  std::vector<std::vector<size_t> > m_column_row_counter;

  /// Writes the file footer
  void emit_footer(size_t segment_id);
};

} // namespace v2_block_impl
} // namespace graphlab
#endif
