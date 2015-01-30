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
#ifndef FAULT_QUERY_OBJECT_STATE_MACHINE_HPP
#define FAULT_QUERY_OBJECT_STATE_MACHINE_HPP
namespace libfault {

/*
 This is the state machine describing the states of a running query object.

 Masters are simple.
 masters are created in the MASTER_ACTIVE state
 MASTER_ACTIVE  --- (on deactivation)  --->  MASTER_INACTIVE


 Replicas are somewhat messier.
 Replicas are created in the REPLICA_INACTIVE state.
 And it sends a message to the current master to ask for a snapshot.

 REPLICA_INACTIVE -- (on receive snapshot) --> REPLICA_ACTIVE

 At any point if

 */
enum object_state {
  MASTER_INACTIVE,
  MASTER_ACTIVE

};

} // fault
#endif
