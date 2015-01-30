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
#ifndef GRAPHLAB_UNITY_SSSP
#define GRAPHLAB_UNITY_SSSP

namespace graphlab {
  namespace sssp {
  /**
   * Obtains the registration for the SSSP Toolkit.
   *
   * <b> Toolkit Name: sssp </b>
   *
   * Accepted Parameters:
   * \li \b source_vid (flexible_type). The source vertex to compute SSSP from.
   *
   * \li \b max_dist (flexible_type: float). The largest distance to expand to.
   * (Default 1E30)
   *
   * \li \b edge_attr(flexible_type: string). The attribute to use for 
   * edge weights. If empty string, uniform weights are used (every edge has a 
   * weight of 1). Otherwise, edge_weight must refer to an edge field with a 
   * integer or float value. If any edge does not contain the field, it
   * is assumed to have an an infinite weight.  (Default "")
   *
   * Returned Parameters:
   * \li \b training_time (flexible_type: float). The training time of the algorithm in seconds 
   * excluding all other preprocessing stages.
   *
   * \li \b __graph__ (unity_graph). The graph object with the field "distance" 
   * on each vertex. The "distance" field (float) corresponds to the 
   * distance of the vertex from the source_vid. If the vertex was unreachable,
   * it has weight infinity.
   */
    std::vector<toolkit_function_specification> get_toolkit_function_registration();

  } // namespace sssp 
} // namespace graphlab
#endif
