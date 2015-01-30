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
#ifndef GRAPHLAB_SYMMETRIC_ARRAY_H_
#define GRAPHLAB_SYMMETRIC_ARRAY_H_

#include <vector>
#include <serialization/serialization_includes.hpp>
#include <logger/assertions.hpp> 

namespace graphlab { 

/** A 2d symmetric n by n array that stores the elements in a
 * triangular array.  The amount of storage required is only n*(n+1) /
 * 2.  Individual manipulation of elements is all that is supported. 
 */
template <typename T>
class symmetric_2d_array {
public:
  
  symmetric_2d_array()
      : n(0)
  {}
  
  /// Init the 2d n x n symmetric array with default_value. 
  symmetric_2d_array(size_t _n, const T& default_value = T())
      : n(_n)
      , data(n*(n+1) / 2, default_value)
  {}

  /// Return the size of either the rows or the columns. (aka n)
  size_t size() const { return n; }

  /// Return the number of rows. (aka n)
  size_t rows() const { return n; }

  /// Return the number of columns. (aka n)
  size_t cols() const { return n; }

  /// Access item (i, j) -- same as (j, i) -- by reference.
  inline T& operator()(size_t i, size_t j) {
    return data[_index(i,j)];
  }

  /// Access item (i, j) -- same as (j, i) -- by const reference.
  inline const T& operator()(size_t i, size_t j) const {
    return data[_index(i,j)];
  }

  /// save to disk
  void save(graphlab::oarchive& oarc) const {
    oarc << n << data; 
  }
  
  /// load from disk
  void load(graphlab::iarchive& iarc) {
    iarc >> n >> data; 
  }
  
 private:
  size_t n;
  std::vector<T> data;

  /// Gives the index in the data matrix 
  inline size_t _index(size_t r, size_t c) const {
    DASSERT_LT(r, n);
    DASSERT_LT(c, n);

    // have that r > c
    if(r < c)
      std::swap(r, c);

    // with r >= c, we first compute the number of entries in a
    // triangular matrix of size r -- this is r*(r+1) / 2.  The value
    // in data is c beyond that point.  E.g.: element (3, 2) is stored in block 8
    //
    // | 0
    // | 1 | 2
    // | 3 | 4 | 5
    // | 6 | 7 | 8 | 9   <-- r = 3 -- there are 3 * 4 / 2 = 6
    //                                elements before this row.
    //
    //       ^
    //       c = 1 --> the index in this row is 1, 6 is the offset to the row start.
    size_t idx = r*(r + 1) / 2 + c;
    
    DASSERT_LT(idx, data.size());
    return idx; 
  }
};

}
#endif
