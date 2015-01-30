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
#include <rpc/dc.hpp>
#include <rpc/distributed_event_log.hpp>

namespace graphlab {
namespace warp {

DECLARE_EVENT(EVENT_WARP_MAPREDUCE_COUNT)
DECLARE_EVENT(EVENT_WARP_BROADCAST_COUNT)
DECLARE_EVENT(EVENT_WARP_TRANSFORM_COUNT)
DECLARE_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT)
DECLARE_EVENT(EVENT_WARP_ENGINE_SIGNAL)
DECLARE_EVENT(EVENT_WARP_ENGINE_UF_COUNT)
DECLARE_EVENT(EVENT_WARP_ENGINE_UF_TIME)

void initialize_counters() {
  static bool is_initialized = false;

  if (!is_initialized) {
    INITIALIZE_EVENT_LOG;
    graphlab::estimate_ticks_per_second();
    ADD_CUMULATIVE_EVENT(EVENT_WARP_MAPREDUCE_COUNT, "Warp::MapReduce", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_BROADCAST_COUNT, "Warp::Broadcast", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_TRANSFORM_COUNT, "Warp::Transform", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT, "Warp::Parfor", "Vertices");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_SIGNAL, "Warp::Engine::Signal", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_UF_COUNT, "Warp::Engine::Update", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_UF_TIME, "Warp::Engine::UpdateTotalTime", "ms");
    ADD_AVERAGE_EVENT(EVENT_WARP_ENGINE_UF_TIME, EVENT_WARP_ENGINE_UF_COUNT,
                      "Warp::Engine::UpdateAverageTime", "ms");
    is_initialized = true;
  }
}

} // namespace warp

} // namespace graphlab
