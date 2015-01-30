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
#ifndef GRAPHLAB_UNITY_KCORE
#define GRAPHLAB_UNITY_KCORE

namespace graphlab {
namespace kcore {


/**
 * Obtains the registration for the KCore Toolkit.
 *
 * Performs kcore decomposition on the graph. 
 *
 * <b> Toolkit Name: kcore </b>
 *
 * Accepted Parameters:
 * \li \b kmin (flexible_type: integer). The lowest KCore value to compute
 * (inclusive).  Defaults to 0. All vertices with a core value out of this
 * range will have a core ID of -1.
 * 
 * \li \b kmax (flexible_type: integer). The highest KCore value to compute
 * (inclusive). Defaults to 100. All vertices with a core value out of this
 * range will have a core ID of -1.
 *
 * Returned Parameters:
 * \li \b training_time (flexible_type: float). The training time of the algorithm in seconds 
 * excluding all other preprocessing stages.
 *
 * \li \b max_core The largest core value encountered.
 * 
 * \li \b __graph__ (unity_graph). The graph object with the field "core_id",
 * The core_id field (integer) contains the core number of the vertex. 
 * This number will be in between kmin amd kmax (inclusive). All vertices with
 * core values outside of this range will have core_id of -1.
 */
std::vector<toolkit_function_specification> get_toolkit_function_registration();

} // namespace kcore 
} // namespace graphlab
#endif
