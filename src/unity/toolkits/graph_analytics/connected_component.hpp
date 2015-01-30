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
#ifndef GRAPHLAB_UNITY_CONNECTED_COMPONENT
#define GRAPHLAB_UNITY_CONNECTED_COMPONENT
#include <unity/lib/toolkit_function_specification.hpp>

namespace graphlab {
namespace connected_component {
/**
 * Obtains the registration for the Connected Component Toolkit.
 *
 * Computes Weakly connected components on the graph
 *
 * <b> Toolkit Name: connected_component </b>
 *
 * Accepted Parameters: None
 *
 * Returned Parameters:
 * \li \b training_time (flexible_type: float). The training time of the algorithm in seconds 
 * excluding all other preprocessing stages.
 *
 * \li \b num_of_components (flexible_type: int) The number of components of the graph.
 * 
 * \li \b __graph__ (unity_graph). The graph object with the field "component_id",
 * The component_id field (integer) on each vertex contains the component ID
 * of the vertex. All vertices with the same component ID are connected.
 * component IDs are not sequential and can be arbitrary integers.
 */
std::vector<toolkit_function_specification> get_toolkit_function_registration() ;

} // namespace connected_component
} // namespace graphlab
#endif
