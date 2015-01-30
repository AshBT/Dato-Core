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
#include <unity/lib/toolkit_function_specification.hpp>
#ifndef GRAPHLAB_UNITY_TRIANGLE_COUNTING
#define GRAPHLAB_UNITY_TRIANGLE_COUNTING

namespace graphlab {
namespace triangle_counting {
/**
 * Obtains the registration for the Triangle Counting Toolkit
 *
 * Counts the number of undirected triangles in the graph.
 * <b> Toolkit Name: triangle_counting </b>
 *
 * Accepted Parameters: None
 *
 * Returned Parameters:
 * \li \b training_time (flexible_type: float). The training time of the algorithm in seconds 
 * excluding all other preprocessing stages.
 *
 * \li \b num_triangles (flexible_type: int) The total number of triangles found
 * 
 * \li \b __graph__ (unity_graph). The graph object with the field "triangle_count",
 * The triangle_count field (integer) on each vertex contains the number of 
 * triangles each vertex is involved in.
 */
std::vector<toolkit_function_specification> get_toolkit_function_registration();

} // namespace triangle_counting 
} // namespace graphlab
#endif
