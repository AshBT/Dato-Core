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
#ifndef GRAPHLAB_WARP_EVENT_LOG_HPP
#define GRAPHLAB_WARP_EVENT_LOG_HPP
#include <rpc/distributed_event_log.hpp>
namespace graphlab {
namespace warp{

/// Total number of MapReduce neighborhood calls
extern DECLARE_EVENT(EVENT_WARP_MAPREDUCE_COUNT)
/// Total number of broadcast neighborhood calls
extern DECLARE_EVENT(EVENT_WARP_BROADCAST_COUNT)
/// Total number of transform neighborhood calls
extern DECLARE_EVENT(EVENT_WARP_TRANSFORM_COUNT)
/// Total number of vertices evaluated in a parfor_vertices
extern DECLARE_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT)
/// Total number of scheduler signals issued in a warp engine
extern DECLARE_EVENT(EVENT_WARP_ENGINE_SIGNAL)
/// Total number of update functions executed
extern DECLARE_EVENT(EVENT_WARP_ENGINE_UF_COUNT)
/// Update Function runtime
extern DECLARE_EVENT(EVENT_WARP_ENGINE_UF_TIME)

/**
 * Initializes the counters to be used by the warp functions.
 * This function is called automatically by the parfor_all_vertices and 
 * on warp_engine construction. However, if you are calling the warp functions
 * directly, it is important to call this function otherwise really bad things
 * may happen.
 */
void initialize_counters();

} // warp
} // graphlab

#endif
