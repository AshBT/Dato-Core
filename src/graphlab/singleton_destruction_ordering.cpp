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
#include <fiber/fiber_control.hpp>
#include <rpc/distributed_event_log.hpp>
#include <metric/metrics_server.hpp>

namespace graphlab {
/**
 * There are 4 important singletons in GraphLab.
 *  - The RPC controller (distributed_control)
 *  - The fiber controller (fiber_control)
 *  - The distributed event log (distributed_event_logger)
 *  - The metrics server (metrics_server)
 *
 * The issue is that while these objects can be constructed in arbitrary 
 * orderings, they must be destroyed in a very specific ordering due to
 * cross dependencies between them.
 *
 * For instance the RPC controller construction can be delayed until it is 
 * actually accessed, in which case it will spawn itself as a single-machine 
 * instance. The fiber controller can be started at any time, depending on
 * whether RPC is used at all, etc. However, the fiber controller must be
 * destroyed only after the RPC controller since the RPC uses the fiber control.
 *
 * The solution is to use a classical C++ pattern for static variable 
 * initialization: returning a static variable in a function.
 * However, these functions will allocate these variables on the heap thus
 * preventing automatic destruction. Destruction is then
 * delayed until this global struct is deleted, whereby it will control the
 * deletion based on which classes have or have not been instantiated.
 */

struct destruction_order {
  destruction_order(){};
  ~destruction_order() {
    // first stop the metric server if any. However,
    // we can't destroy the metric server datastructures yet because
    // various destructors may still try to unregister callbacks.
    stop_metric_server();

    // stop the event logger. But don't destroy it yet.
    // Stuff may still be writing into the logs from RPC, and from fiber_control.
    // Here, we are just detaching the event_logger from the RPC system,
    // and from the thread timers. This prevents new logs from being written.
    distributed_event_logger::destroy_instance();

    // stop RPC if any
    distributed_control::delete_instance();

    // stop the fibers
    fiber_control::delete_instance();

    // The event log counter system will surprisingly continue to work fine.
    distributed_event_logger::delete_instance();

    // finally clean up the callback datastructures
    delete_all_metric_server_callbacks();
  }
};

#ifdef __APPLE__
static destruction_order destruction_object;
#else
static destruction_order  __attribute__((init_priority (101))) destruction_object;
#endif

}
