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
#include <fault/sockets/get_next_port_number.hpp>
#include <cstdlib>
namespace libfault {
#define ZSOCKET_DYNFROM     0xc000
#define ZSOCKET_DYNTO       0xffff
static size_t cur_port = ZSOCKET_DYNFROM;

size_t get_next_port_number() {
  size_t ret = cur_port;
  cur_port = (cur_port + 1) <= ZSOCKET_DYNTO ? 
                                (cur_port + 1) : ZSOCKET_DYNFROM;
  return ret;
}

} // libfault
