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
#ifndef GRAPHLAB_SFRAME_CONSTANTS_HPP
#define GRAPHLAB_SFRAME_CONSTANTS_HPP
#include <cstddef>
#include <string>
namespace graphlab {
/**
 * The default number of segments created when an SFrame/SArray is
 * opened for write.
 * (i.e. \ref sarray::open_for_write and \ref sframe::open_for_write).
 * This is default is used in numerous places. For instance the default
 * number of output segments from the sframe_csv_parser, and the dataframe to
 * sframe converter.
 */
extern size_t SFRAME_DEFAULT_NUM_SEGMENTS;

/**
 * The default parsed buffer size used in the \ref sarray_reader_buffer.
 * The iterators returned by \ref sarray_reader::begin() , \ref sarray_reader::end(),
 * \ref sframe_reader::begin() and \ref sframe_reader::end() also use this
 * as the default parsed buffer size.
 */
extern const size_t DEFAULT_SARRAY_READER_BUFFER_SIZE;

/**
 * The number of rows read from a file in a batch when loading a file into
 * an SArray. (a single column. NOT an sframe).
 */
extern const size_t SARRAY_FROM_FILE_BATCH_SIZE;

/**
 * The minimum number of entries we want inside a segment
 * (only used by join right now).
 */
extern const size_t MIN_SEGMENT_LENGTH;

/*
 * The number of rows to buffer before trying to flush the buffer to disk.
 * Used in shuffle operation.
 */
extern const size_t SFRAME_WRITER_BUFFER_SOFT_LIMIT;

/**
  * The number of rows to buffer before forcing to flush the buffer to disk.
 * Used in shuffle operation.
 */
extern const size_t SFRAME_WRITER_BUFFER_HARD_LIMIT;

/**
 * The default number of handles in the v2 block manager pool.
 */
extern size_t SFRAME_FILE_HANDLE_POOL_SIZE;


/**
 * The default number of block buffers in the v0 block manager pool.
 */
extern const size_t SFRAME_BLOCK_MANAGER_BLOCK_BUFFER_COUNT;

/**
 * If the post compression size is less than this fraction of the
 * pre-compression size. compression is disabled.
 */
extern const float COMPRESSION_DISABLE_THRESHOLD;


/**
 * The default size of each block in the file. This is not strict. the
 * sarray_group_format_writer_v2 will try to target blocks to be of this size,
 * but the actual sizes may vary.
 */
extern size_t SFRAME_DEFAULT_BLOCK_SIZE;

/**
 * The initial number of elements in a block.
 * This is used in sarray_group_format_writer_v2. This is the number of rows
 * the writer will buffer at the start before issuing the first block write.
 * After which, it will use the actual number of bytes written to try to
 * estimate the number of rows to buffer before the next write. (essentially
 * SFRAME_DEFAULT_BLOCK_SIZE / (average bytes per element)).
 */
extern const size_t SARRAY_WRITER_INITAL_ELEMENTS_PER_BLOCK;
/**
 * The minimum number of elements per block. Used in
 * sarray_group_format_writer_v2. It will never write less than this
 * number of elements into a block.
 */
extern const size_t SARRAY_WRITER_MIN_ELEMENTS_PER_BLOCK;

/**
 * The maximum number of elements per block. Used in
 * sarray_group_format_writer_v2. It will never write more than this
 * number of elements into a block.
 */
extern size_t SFRAME_WRITER_MAX_BUFFERED_CELLS_PER_BLOCK;

/**
 * The maximum number of elements cached across all columns of the
 * sarray_group writer. Once this is exceeded, flushes will happen even
 * if the block size is still too small. This is maintained approximately.
 * Essentially, this has the effect of setting
 * SFRAME_WRITER_MAX_BUFFERED_CELLS_PER_BLOCK to
 * SFRAME_WRITER_MAX_BUFFERED_CELLS / (#columns * #segments)
 */
extern size_t SFRAME_WRITER_MAX_BUFFERED_CELLS;

/**
 * The maximum number of data blocks that can be maintained in a reader's
 * decoded cache
 */
extern size_t SFRAME_MAX_BLOCKS_IN_CACHE;

/**
 * The amount to read from the file each time by the CSV parser. (this block
 * is then parsed in parallel by a collection of threads)
 */
extern size_t SFRAME_CSV_PARSER_READ_SIZE;



/**
 * The number of elements to accumulate in a groupby batch until it has to flush.
 */
extern size_t SFRAME_GROUPBY_BUFFER_NUM_ROWS;


/**
 * The number of bytes that a join algorithm is allowed to use during execution.
 */
extern size_t SFRAME_JOIN_BUFFER_NUM_CELLS;

/**
 * Whether locks are used when reading from SFrames on local storage. Good
 * for spinning disks, bad for SSDs.
 */
extern size_t SFRAME_IO_READ_LOCK;


/**
 * If SFRAME_IO_READ_LOCK is set, then the IO LOCK is only used when the
 * file size is greater than this value.
 */
extern const size_t SFRAME_IO_LOCK_FILE_SIZE_THRESHOLD;

/**
 * Number of samples used to estimate the pivot positions to partition the
 * data for sorting.
 */
extern size_t SFRAME_SORT_PIVOT_ESTIMATION_SAMPLE_SIZE;

/**
 * The maximum number of segments we will try to partition the input SFrame
 * into for external sort.  Number kept low initially to be sensitive of open
 * file handle limits.
 */
extern size_t SFRAME_SORT_MAX_SEGMENTS;

/**
 * A variable the user can set to look for libodbc.so
 */
extern std::string LIBODBC_PREFIX;

/**
 * Amount of memory (in bytes) the odbc connector will use when reading/writing
 * in bulk.
 */
extern size_t ODBC_BUFFER_SIZE;

/**
 * Maximum number of rows to read/write at one time.
 *
 * This competes with ODBC_BUFFER_SIZE.  The ODBC connector will either use all
 * of the memory ODBC_BUFFER_SIZE specifies OR be limited by the number of rows
 * in this constant.  This prevents asking the ODBC driver for millions of tiny
 * rows.
 */
extern size_t ODBC_BUFFER_MAX_ROWS;
} // namespace graphlab
#endif
