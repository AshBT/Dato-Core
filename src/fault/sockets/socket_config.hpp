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
#ifndef LIBFAULT_SOCKET_CONFIG_HPP
#define LIBFAULT_SOCKET_CONFIG_HPP

namespace libfault {
extern int SEND_TIMEOUT;
extern int RECV_TIMEOUT;

void set_send_timeout(int ms);
void set_recv_timeout(int ms);

void set_conservative_socket_parameters(void* socket);
};

#endif
