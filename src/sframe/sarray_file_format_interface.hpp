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
#ifndef GRAPHLAB_UNITY_SARRAY_FILE_FORMAT_INTERFACE_HPP
#define GRAPHLAB_UNITY_SARRAY_FILE_FORMAT_INTERFACE_HPP

#define BOOST_SPIRIT_THREADSAFE

#include <cstdlib>
#include <string>
#include <sstream>
#include <map>
#include <fileio/general_fstream.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <sframe/sarray_index_file.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe_rows.hpp>
namespace graphlab {

/**
 * A generic \ref sarray file format reader interface. File format
 * implementations should extend this.
 * 
 * The sarray file layout should generally be a file set (collection of files)
 * with a common prefix. File format implementations can create or use as many
 * prefixes as required. There must be an [prefix].sidx in the the Microsoft
 * INI format with the following sections
 *
 * \verbatim
 * [sarray]
 * ; The version of the file format. Required.
 * version=0
 * \endverbatim
 */
template <typename T>
class sarray_format_reader_common_base {
 public:
  virtual ~sarray_format_reader_common_base() {}


  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the file set, or if there
   * is a format error in the sarray.
   *
   * Will throw an exception if a file set is already open.
   */
  virtual void open(index_file_information index) = 0;

  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the file set, or if there
   * is a format error in the sarray.
   *
   * Will throw an exception if a file set is already open.
   */
  virtual void open(std::string sidx_file) = 0;

  /**
   * Closes an sarray file set. No-op if the array is already closed.
   */
  virtual void close() = 0;

  /**
   * Return the number of segments in the sarray.
   * Throws an exception if the array is not open.
   */
  virtual size_t num_segments() const = 0;

  /**
   * Returns the number of elements in a given segment.
   * should throw an exception if the segment ID does not exist,
   */
  virtual size_t segment_size(size_t segmentsid) const = 0;

  /**
   * Reads a collection of rows, storing the result in out_obj.
   * This function is independent of the open_segment/read_segment/close_segment
   * functions, and can be called anytime. This function is also fully 
   * concurrent.
   * \param row_start First row to read
   * \param row_end one past the last row to read (i.e. EXCLUSIVE). row_end can
   *                be beyond the end of the array, in which case, 
   *                fewer rows will be read.
   * \param out_obj The output array
   * \returns Actual number of rows read. Return (size_t)(-1) on failure.
   */
  virtual size_t read_rows(size_t row_start, 
                           size_t row_end, 
                           std::vector<T>& out_obj) = 0;

  /**
   * Returns the file index of the array (the argument in \ref open)
   */
  virtual std::string get_index_file() const = 0;

  /**
   * Gets the contents of the index file information read from the index file
   */
  virtual const index_file_information& get_index_info() const = 0;
};

template <typename T>
class sarray_format_reader : public sarray_format_reader_common_base<T> {
 public:
  virtual ~sarray_format_reader() {}
};

template <>
class sarray_format_reader<flexible_type> 
      : public sarray_format_reader_common_base<flexible_type> {
 public:
  virtual ~sarray_format_reader() {}

  /**
   * Reads a collection of rows, storing the result in out_obj.
   * This function is independent of the open_segment/read_segment/close_segment
   * functions, and can be called anytime. This function is also fully 
   * concurrent.
   * \param row_start First row to read
   * \param row_end one past the last row to read (i.e. EXCLUSIVE). row_end can
   *                be beyond the end of the array, in which case, 
   *                fewer rows will be read.
   * \param out_obj The output array
   * \returns Actual number of rows read. Return (size_t)(-1) on failure.
   */
  using sarray_format_reader_common_base::read_rows;

  virtual size_t read_rows(size_t row_start, 
                           size_t row_end, 
                           sframe_rows& out_obj) {
    size_t ret = 0;
    if (out_obj.get_columns().size() == 1 && 
        out_obj.get_columns()[0].m_contents == sframe_rows::block_contents::DECODED_COLUMN) {
      // if the sframe_rows has only one column and is already of the right 
      // type, just reuse it instead of allocated a new vector
      ret = read_rows(row_start, row_end, out_obj.get_columns()[0].m_decoded_column);
    } else {
      // allocate a new vector
      std::vector<flexible_type> out;
      ret = read_rows(row_start, row_end, out);
      out_obj.reset();
      out_obj.add_decoded_column(std::move(out));
    }
    return ret;
  }
};




/**
 * A generic \ref sarray group file format writer interface. File format
 * implementations should extend this.
 *
 * The sarray_group is a collection of sarrays in a single file set.
 *
 * The writer is assumed to always to writing to new file sets; we are 
 * never modifying an existing file set. 
 */
template <typename T>
class sarray_group_format_writer {
 public:
  virtual ~sarray_group_format_writer() {}

  /**
   * Open has to be called before any of the other functions are called.
   * No files are actually opened at this point.
   */
  virtual void open(std::string index_file, 
                    size_t segments_to_create,
                    size_t columns_to_create) = 0;

  /**
   * Gets a modifiable reference to the index file information which will 
   * be written to the index file. Can only be called after close()
   */
  virtual group_index_file_information& get_index_info() = 0;


  /** Closes all segments.
   */
  virtual void close() = 0;

  /**
   * Flushes the index_file_information to disk
   */
  virtual void write_index_file() = 0;


  /**
   * Writes a row to the array group
   */
  virtual void write_segment(size_t segmentid, 
                             const std::vector<T>&) = 0;

  /**
   * Writes a row to the array group
   */
  virtual void write_segment(size_t segmentid, 
                             std::vector<T>&&) = 0;

  /**
   * Writes a row to the array group
   */
  virtual void write_segment(size_t columnid, 
                             size_t segmentid,
                             const T&) = 0;

  /**
   * Writes a row to the array group
   */
  virtual void write_segment(size_t columnid, 
                             size_t segmentid,
                             T&&) = 0;


  /**
   * Return the number of segments in the sarray.
   * Throws an exception if the array is not open.
   */
  virtual size_t num_segments() const = 0;


  /**
   * Return the number of columns in the sarray.
   * Throws an exception if the array is not open.
   */
  virtual size_t num_columns() const = 0;
};

} // namespace graphlab

#endif


