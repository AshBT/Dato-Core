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
#ifndef GRAPHLAB_SFRAME_SHUFFLE_HPP
#define GRAPHLAB_SFRAME_SHUFFLE_HPP

#include <vector>
#include <sframe/sframe.hpp>

namespace graphlab {

/**
 * Shuffle the rows in one sframe into a collection of n sframes.
 * Each output SFrame contains one segment.
 *
 * \code
 * std::vector<sframe> ret(n);
 * for (auto& sf : ret) {
 *   INIT_WITH_NAMES_COLUMNS_AND_ONE_SEG(sframe_in.column_names(), sframe_in.column_types());
 * }
 * for (auto& row : sframe_in) {
 *   size_t idx = hash_fn(row) % n;
 *   add_row_to_sframe(ret[idx], row); // the order of addition is not guaranteed.
 * }
 * \endcode
 *
 * The result sframes have the same column names and types (including
 * empty sframes). A result sframe can have 0 rows if non of the
 * rows in the input sframe is hashed to it. (If n is greater than
 * the size of input sframe, there will be at (n - sframe_in.size())
 * empty sframes in the return vector.
 *
 * \param n the number of output sframe.
 * \param hash_fn the hash function for each row in the input sframe.
 * 
 * \return A vector of n sframes.
 */
std::vector<sframe> shuffle(
     sframe sframe_in,
     size_t n,
     std::function<size_t(const std::vector<flexible_type>&)> hash_fn);
}
#endif
