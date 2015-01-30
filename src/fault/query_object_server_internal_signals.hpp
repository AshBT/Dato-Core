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
#ifndef QUERY_OBJECT_SERVER_INTERNAL_SIGNALS_HPP
#define QUERY_OBJECT_SERVER_INTERNAL_SIGNALS_HPP
// this defines the set of messages that are passed from the manager to the
// server processes.

#define QO_SERVER_FAIL -1
#define QO_SERVER_STOP 0
#define QO_SERVER_PROMOTE 1
#define QO_SERVER_PRINT 2


#define QO_SERVER_FAIL_STR "-1\n"
#define QO_SERVER_STOP_STR "0\n"
#define QO_SERVER_PROMOTE_STR "1\n"
#define QO_SERVER_PRINT_STR "2\n"


#endif
