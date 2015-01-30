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
#include <cppipc/server/comm_server.hpp>
#include <cppipc/client/comm_client.hpp>
#include <cppipc/common/ipc_deserializer.hpp>
namespace cppipc {
namespace detail {
static __thread comm_server* thlocal_server = NULL;
static __thread comm_client* thlocal_client = NULL;
void set_deserializer_to_server(comm_server* server) {
 thlocal_server = server; 
 thlocal_client = NULL;
}
void set_deserializer_to_client(comm_client* client) {
  thlocal_client = client;
  thlocal_server = NULL;
}


void get_deserialization_type(comm_server** server, comm_client** client) {
  (*server) = thlocal_server;
  (*client) = thlocal_client;
}


std::shared_ptr<void> get_server_object_ptr(comm_server* server, size_t object_id) {
  return server->get_object(object_id);
}

} // detail
} // cppipc
