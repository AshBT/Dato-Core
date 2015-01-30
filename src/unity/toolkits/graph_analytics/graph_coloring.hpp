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
#ifndef GRAPHLAB_UNITY_GRAPH_COLORING
#define GRAPHLAB_UNITY_GRAPH_COLORING

namespace graphlab {
namespace graph_coloring{
/**
 * Obtains the registration for the Graph Coloring Toolkit
 *
 * Colors the graph such that adjacent vertices do not have the same color. 
 * This implements a heuristic coloring and there are no guarantees that it will
 * find the smallest coloring; just a minimal coloring (i.e. there are no 
 * local single vertex color changes that will improve the coloring). 
 * Consecutive executions may return different colorings.
 *
 * <b> Toolkit Name: graph_coloring </b>
 *
 * Accepted Parameters: None
 *
 * Returned Parameters:
 * \li \b training_time (flexible_type: float). The training time of the algorithm in seconds 
 * excluding all other preprocessing stages.
 *
 * \li \b num_colors (flexible_type: int) The total number of colors found
 * 
 * \li \b __graph__ (unity_graph). The graph object with the field "component_id",
 * The component_id field (integer) on each vertex contains the component ID
 * of the vertex. All vertices with the same component ID are connected.
 * component IDs are not sequential and can be arbitrary integers.
 */
std::vector<toolkit_function_specification> get_toolkit_function_registration();

} // namespace graph_coloring 
} // namespace graphlab
#endif
