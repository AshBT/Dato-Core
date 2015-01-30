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
#ifndef GRAPHLAB_SFRAME_sframe_rows_HPP
#define GRAPHLAB_SFRAME_sframe_rows_HPP
#include <vector>
#include <map>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sarray_v2_encoded_block.hpp>
#include <util/code_optimization.hpp>
namespace graphlab {
class sframe_rows_range;
/**
 *
 * sframe_rows class.
 * 
 * This is designed as a semi-opaque wrapper around a collection of rows of an 
 * SFrame / SArray. The objective is to allow the underlying representation
 * to change (be represented either row-wise, or column-wise), while allowing
 * keeping the higher level iterator interface constant.
 *
 * The sframe_rows internally support 3 representations (you can get the
 * current representation with sframe_rows::content_type(). It returns a member
 * of the sframe_rows::block_contents enum)
 *
 * Decoded Rows
 * ------------
 *  content_type:  sframe_rows::block_contents::DECODED_ROWS
 *  data type:     sframe_rows::decoded_rows_type 
 *                 (std::pair<vector<vector<flexible_type>>, size_t>)
 *                 .first is the actual data
 *                 .second is the number of columns.
 *
 *  where x is of the type decoded_rows_type, x.size() is the number of rows,
 *  and when available x[0].size() is the number of columns. 
 *
 * Decoded Column 
 * --------------
 * Note that this is a decoded column (in the singular).
 *  content_type:  sframe_rows::block_contents::DECODED_COLUMN
 *  data type:     sframe_rows::decoded_columns_type (vector<flexible_type>)
 *
 *  where x is of the type decoded_column_type, x.size() is the number of rows,
 *  and there is always only 1 column.
 *
 * These representations can then be combined column-wise, so we can have
 * an sframe_rows with the first 3 columns being represented row-wise,
 * the 4th column being a decoded column and the 5th column being an encoded
 * column, etc.
 */
class sframe_rows {
 public:
  /// The data type of decoded rows (block_contents::DECODED_ROWS)
  typedef std::pair<std::vector<std::vector<flexible_type>>, size_t> decoded_rows_type;
  /// The data type of decoded column (block_contents::DECODED_COLUMN)
  typedef std::vector<flexible_type> decoded_column_type;

  inline sframe_rows() { }
  ~sframe_rows();

  /**
   * An enumeration over the internal representation of the sframe_rows
   */
  enum class block_contents {
    NONE,
    DECODED_ROWS,
    DECODED_COLUMN,
  };

  /// Returns the number of columns 
  inline size_t num_columns() const {
    recalculate_size();
    return m_num_columns;
  }

  /// Returns the number of rows
  inline size_t num_rows() const {
    recalculate_size();
    return m_num_rows;
  }

  /**
   * Clears the contents of the sframe_rows datastructure.
   */
  void reset();

  /** Returns a range iterator over the contents of sframe_rows.
   *  The lifespan of the range iterator must generally not exceed the life span
   *  of the underlying sframe_rows object.
   */
  sframe_rows_range get_range();

  /**
   * Adds to the right of the sframe_rows, a collection of decoded rows.
   * 
   * \code
   * add_decoded_rows(std::move(source))
   * \endcode
   *
   * Note that decoded_rows_type is a pair of the actual vector of vectors,
   * and a size_t which is the number of columns. (this is to avoid ambiguity
   * in the situation where I have 0 rows but potentially N columns).
   *
   * Note that the shape of the actual vector of vectors is *not* validated.
   */
  void add_decoded_rows(decoded_rows_type decoded_rows);

  /**
   * Adds to the right of the sframe_rows, a collection of decoded columns
   * 
   * \code
   * add_decoded_column(std::move(source))
   * \endcode
   */
  void add_decoded_column(decoded_column_type decoded_column);

  /**
   * The column group type.
   */
  struct column_group_type {
    column_group_type() { }

    column_group_type(const column_group_type& other) {
      (*this) = other;
    }
    column_group_type(column_group_type&& other) {
      (*this) = std::move(other);
    }

    ~column_group_type() {
      release();
    }

    column_group_type& operator=(const column_group_type& other) {
      init(other.m_contents);
      switch(m_contents) {
       case block_contents::DECODED_ROWS:
         m_decoded_rows = other.m_decoded_rows;
         break;
       case block_contents::DECODED_COLUMN:
         m_decoded_column = other.m_decoded_column;
         break;
       case block_contents::NONE:
       default:
         break;
      }
      return *this;
    }

    column_group_type& operator=(column_group_type&& other) {
      init(other.m_contents);
      switch(m_contents) {
       case block_contents::DECODED_ROWS:
         m_decoded_rows = std::move(other.m_decoded_rows);
         break;
       case block_contents::DECODED_COLUMN:
         m_decoded_column = std::move(other.m_decoded_column);
         break;
       case block_contents::NONE:
       default:
         break;
      }
      return *this;
    }

    /**
     * Construction from decoded_rows
     */
    column_group_type& operator=(decoded_rows_type other) {
      init(block_contents::DECODED_ROWS);
      m_decoded_rows = std::move(other);
      return *this;
    }

    /**
     * Construction from decoded_rows
     */
    column_group_type& operator=(decoded_column_type other) {
      init(block_contents::DECODED_COLUMN);
      m_decoded_column = std::move(other);
      return *this;
    }
    /**
     * Initializes the contents to a particular type
     */
    void init(block_contents content_type) {
      release();
      m_contents = content_type;
      switch(m_contents) {
       case block_contents::DECODED_ROWS:
         new (&m_decoded_rows) decoded_rows_type();
         break;
       case block_contents::DECODED_COLUMN:
         new (&m_decoded_column) decoded_column_type();
         break;
       case block_contents::NONE:
       default:
         break;
      }
    }
    /**
     * Releases the contents of the column_group
     */
    void release() {
      switch(m_contents) {
       case block_contents::DECODED_ROWS:
         m_decoded_rows.~decoded_rows_type();
         break;
       case block_contents::DECODED_COLUMN:
         m_decoded_column.~decoded_column_type();
         break;
       case block_contents::NONE:
       default:
         break;
      }
      m_contents = block_contents::NONE;
    }

    block_contents m_contents = block_contents::NONE;
    union { 
      decoded_rows_type m_decoded_rows;
      decoded_column_type m_decoded_column;
    };
  };

  /**
   * Returns a modifiable reference to the set of column groups
   */
  std::vector<column_group_type>& get_columns() {
    return m_columns;
  }

  /**
   * Const version of get_columns
   */
  const std::vector<column_group_type>& get_columns() const {
    return m_columns;
  }


 private:
  void recalculate_size() const;
  mutable size_t m_num_rows = 0;
  mutable size_t m_num_columns = 0;
  friend class sframe_rows_range;
  std::vector<column_group_type> m_columns;
};  // class sframe_rows

/**
 * The range iterator over the sframe_rows. This range iterator must not have a
 * life span exceeding the originating sframe_rows. 
 *
 * \code
 * for (auto& i : sf_rows.get_range()) {
 *   // i is a sframe_rows_range::row object
 *   // i can be indexed, like i[5] 
 *   // it also has an implcit cast to std::vector<flexible_type>
 * }
 * \endcode
 */
class sframe_rows_range {
 public:
  explicit sframe_rows_range(sframe_rows& rows);

  /**
   * The opaque row obect which mimics a std::vector<flexible_type>.
   */
  struct row {
    row() = default;
    row(const row&) = default;
    row(row&&) = default;
    row& operator=(const row&) = default;
    row& operator=(row&&) = default;

    inline row(sframe_rows_range* owner, 
               const std::vector<std::pair<size_t, size_t> >& column_pos):
        m_owner(owner),
        m_column_pos(&column_pos) { }

    /**
     * Implicit cast to std::vector<flexible_type>
     */
    inline operator std::vector<flexible_type>() const {
      std::vector<flexible_type> ret(m_column_pos->size());
      for (size_t i = 0;i < ret.size(); ++i)  ret[i] = (*this)[i];
      return ret;
    }

    inline const flexible_type& at(size_t i) const {
      if (i < m_column_pos->size()) return (*this)[i];
      else throw "Index out of bounds";
    }

    /**
     * Relatively efficient direct indexing is allowed
     */
    inline const flexible_type& operator[](size_t i) const{
      const size_t colgroupid = (*m_column_pos)[i].first;
      const size_t subcolid = (*m_column_pos)[i].second;
      const auto& colgroup = m_owner->m_source.m_columns[colgroupid];
      if (colgroup.m_contents == sframe_rows::block_contents::DECODED_COLUMN) {
        return colgroup.m_decoded_column[m_owner->m_current_row_number];
      } else {
        return colgroup.m_decoded_rows.first[m_owner->m_current_row_number][subcolid];
      }
    }

    inline size_t size() const {
      return m_column_pos->size();
    }

    const sframe_rows_range* m_owner = NULL;
    const std::vector<std::pair<size_t, size_t> >* m_column_pos = NULL;
  };

  /**
   * Internal Iterator type.
   */
  struct iterator: 
      public boost::iterator_facade<iterator, 
                                    const row, 
                                    boost::single_pass_traversal_tag> {
    /// Pointer to the input range. NULL if end iterator.
    sframe_rows_range* m_owner = NULL; 
    row m_row;
    /// default constructor
    iterator() {}
    explicit iterator(sframe_rows_range* owner):
        m_owner(owner),m_row(owner, m_owner->m_column_pos) { };
   private:
    friend class boost::iterator_core_access;
    /// advances the iterator. See boost::iterator_facade
    void increment() { 
      if (m_owner && !m_owner->skip(1)) {
        m_owner = NULL;
        m_row = row();
      }
    }
    /// advances the iterator. See boost::iterator_facade
    void advance(size_t n) { 
      if (m_owner && !m_owner->skip(n)) {
        m_owner = NULL;
        m_row = row();
      }
    }

    /// Tests for iterator equality. See boost::iterator_facade
    bool equal(const iterator& other) const {
        return this->m_owner == other.m_owner;
    }

    /// Dereference. See boost::iterator_facade
    const row& dereference() const { 
      return m_row;
    }
  };

  typedef iterator const_iterator;
 
  /** 
   * Returns the start iterator to the range.
   * This returns a *single pass* iterator. And hence multiple calls 
   * to begin will return multiple iterators to the *current value* and not the
   * first value. Also, multiple iterators returned by begin() are not 
   * safe for concurrent use.
   */
  inline iterator begin() const {
    if (m_current_row_number >= num_rows()) return iterator();
    return iterator(const_cast<sframe_rows_range*>(this));
  }

  /// Returns the end iterator to the range.
  inline iterator end() const {
    return iterator();
  }

  /// Returns the number of rows
  inline size_t num_rows() const {
    return m_num_rows;
  }

  /// Returns the number of columns
  inline size_t num_columns() const {
    return m_num_columns;
  }

 private:
  /**************************************************************************/
  /*                                                                        */
  /*                              Data Source                               */
  /*                                                                        */
  /**************************************************************************/
  sframe_rows& m_source;

  /**************************************************************************/
  /*                                                                        */
  /*                          Iterator Management                           */
  /*                                                                        */
  /**************************************************************************/
  /**
   * for each column, the column_group, and if it is a DECODED_ROW type, 
   * the sub-column within the group
   */
  std::vector<std::pair<size_t, size_t> > m_column_pos;
  /// Current row number in the sframe_rows
  size_t m_current_row_number = 0;
  size_t m_num_rows = 0;
  size_t m_num_columns = 0;

  /** 
   * Computes the column positions
   */
  void compute_column_pos();

  /** 
   * Skips a certain number of values, and 
   * fetches the next value after that storing it in m_current_value.
   * Returns true on success, false on failure.
   */
  inline GL_HOT_INLINE_FLATTEN bool skip(size_t skip = 1) {
    // if we have already exceed the limit quit
    m_current_row_number += skip;
    // if we have exceeded the limit quit
    if (m_current_row_number >= num_rows()) {
      return false;
    }
    return true;
  }
}; // sframe_rows_range

} // namespace graphlab
#endif
