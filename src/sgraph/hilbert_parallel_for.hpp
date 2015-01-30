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
#ifndef GRAPHLAB_SGRAPH_HILBERT_PARALLE_FOR_HPP
#define GRAPHLAB_SGRAPH_HILBERT_PARALLE_FOR_HPP
#include <utility>
#include <functional>
#include <parallel/thread_pool.hpp>
#include <parallel/lambda_omp.hpp>
#include <sgraph/hilbert_curve.hpp>
namespace graphlab {
namespace sgraph_compute {

/**
 * This performs a parallel sweep over an n*n grid following the Hilbert
 * curve ordering. The parallel sweep is broken into two parts. A "preamble"
 * callback which is called sequentially, which contains a list of all the
 * coordinates to be executed in the next pass, and a function which is 
 * executed on every coordinate in the pass.
 *
 * The function abstractly implements the following:
 *
 * \code
 * for i = 0 to n*n step parallel_limit
 *   // collect all the coordinates to be run in this pass
 *   std::vector<pair<size_t, size_t> > coordinates
 *   for j = i to min(i + parallel_limit, n*n)
 *      coordinates.push_back(convert_hilbert_curve_to_coordinates(j))
 *   // run the preamble   
 *   preamble(coordinates)
 *
 *   parallel for over coordinate in coordinates:
 *      fn(coordinate)
 * \endcode  
 *
 * n must be at least 2 and a power of 2.
 */
inline void hilbert_blocked_parallel_for(size_t n,
                                  std::function<void(std::vector<std::pair<size_t, size_t> >) > preamble, 
                                  std::function<void(std::pair<size_t, size_t>)> fn,
                                  size_t parallel_limit = thread_pool::get_instance().size()) {
  for (size_t i = 0;i < n*n; i += parallel_limit) {
    std::vector<std::pair<size_t, size_t> >  coordinates;
    // accumulate the list of coordinates to run
    size_t lastcoord_this_pass = std::min(i + parallel_limit, n*n);
    for(size_t j = i; j < lastcoord_this_pass; ++j) {
      coordinates.push_back(hilbert_index_to_coordinate(j, n));
    }
    preamble(coordinates);
    parallel_for(coordinates.begin(), coordinates.end(), fn);
  }
}

}  // sgraph_compute
} // graphlab
#endif // GRAPHLAB_SGRAPH_HILBERT_PARALLE_FOR_HPP
