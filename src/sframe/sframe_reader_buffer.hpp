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
#ifndef GRAPHLAB_SFRAME_READER_BUFFER
#define GRAPHLAB_SFRAME_READER_BUFFER
#include <memory>
#include <vector>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sframe_constants.hpp>
namespace graphlab {
class sframe;
/**
 * A buffered reader reading from a range of an sframe<T>.
 *
 * \code
 * sframe<flexible_type> mysframe = ...;
 *
 * // Reader for the first thousand lines
 * sframe_reader_buffer<flexible_type> reader(mysframe, 0, 1000);
 *
 * while(reader.has_next()) {
 *  flexible_type val = reader.next();
 *  ... do some thing with val ...
 * }
 *
 * // Reader for the entire sframe 
 * reader = sframe_reader_buffer<flexible_type>(mysframe, 0, (size_t)(-1));
 * ...
 * \endcode
 *
 * Internally, the reader maintains a vector as buffer, and when reading
 * reaches the end of the buffer, refill the buffer by reading from sframe.
 */
class sframe_reader_buffer {
 public:
  typedef std::vector<flexible_type> value_type;

  sframe_reader_buffer() = default;

  /// Construct from sframe reader with begin and end row.
  sframe_reader_buffer(
      std::shared_ptr<typename sframe::reader_type> reader,
      size_t row_start, size_t row_end,
      size_t buffer_size = DEFAULT_SARRAY_READER_BUFFER_SIZE) {
    init(reader, row_start, row_end, buffer_size);
  }

  void init(std::shared_ptr<typename sframe::reader_type>& reader,
            size_t row_start, size_t row_end,
            size_t internal_buffer_size = DEFAULT_SARRAY_READER_BUFFER_SIZE) {
    m_reader = reader;
    m_buffer_pos = 0;
    m_iter = row_start;
    m_original_row_start = row_start;
    m_row_start = row_start;
    m_row_end = std::min(row_end, m_reader->size());
    m_buffer_size = internal_buffer_size;
    m_buffer.clear(); 
  }

  /// Return the next element in the reader.
  value_type&& next();

  /// Return true if the reader has more element.
  bool has_next();

  /// Return the buffer.
  inline std::vector<value_type>& get_buffer() {return m_buffer;}

  /// Return the Number of elements between row_start and row_end.
  inline size_t size() {return m_row_end - m_original_row_start;}

  /** Resets the buffer to the initial starting conditions. Reading
   *  from the buffer again will start from row_start.
   */
  void clear();

 private:
  /// Refill the chunk buffer form the sframe reader.
  void refill();

  typedef sframe::reader_type reader_type;

  /// Buffer the prefetched elements.
  std::vector<value_type> m_buffer;

  /// The underlying reader as a data source.
  std::shared_ptr<reader_type> m_reader;

  /// Current position of the buffer reader.
  size_t m_buffer_pos = 0;
  /// The initial starting point. clear() will reset row_start to here.
  size_t m_original_row_start = 0;
  /// Start row of the remaining chunk.
  size_t m_row_start = 0;
  /// End row of the chunk.
  size_t m_row_end = 0;
  /// The size of the buffer vector
  size_t m_buffer_size = 0;
  /// The current iterator location
  size_t m_iter = 0;
};

/// Return the next element in the chunk.
inline std::vector<flexible_type>&& sframe_reader_buffer::next() {
  if (m_buffer_pos == m_buffer.size()) {
    refill();
    m_buffer_pos = 0;
  }
  DASSERT_LT(m_buffer_pos, m_buffer.size());
  ++m_iter;
  return std::move(m_buffer[m_buffer_pos++]);
}

/// Return true if the chunk has remaining element.
inline bool sframe_reader_buffer::has_next() {
  return m_iter < m_row_end;
}

/// Refill the chunk buffer form the sframe reader.
inline void sframe_reader_buffer::refill() {
  size_t size_of_refill = std::min<size_t>(m_row_end - m_row_start, m_buffer_size);
  m_reader->read_rows(m_row_start, m_row_start + size_of_refill, m_buffer);
  m_row_start += size_of_refill;
}


inline void sframe_reader_buffer::clear() {
  m_buffer.clear(); 
  m_row_start = m_original_row_start;
  m_iter = m_original_row_start;
  m_buffer_pos = 0;
}
}

#endif
