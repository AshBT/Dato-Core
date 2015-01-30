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
#include <string>
#include <vector>
#include <cstdio>
#include <boost/algorithm/string.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/sframe.hpp>
#include <sframe/join_impl.hpp>

namespace graphlab {

sframe join(sframe& sf_left,
            sframe& sf_right,
            std::string join_type,
            const std::map<std::string,std::string> join_columns,
            size_t max_buffer_size = SFRAME_JOIN_BUFFER_NUM_CELLS);

} // end of graphlab
