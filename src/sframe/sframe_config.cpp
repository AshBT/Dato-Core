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
#include <cmath>
#include <cstddef>
#include <globals/globals.hpp>
namespace graphlab {

/**
** Global configuration for sframe, keep them as non-constants because we want to
** allow user/server to change the configuration according to the environment
**/
namespace sframe_config {
  size_t SFRAME_SORT_BUFFER_SIZE = size_t(2*1024*1024)*size_t(1024);
  size_t SFRAME_READ_BATCH_SIZE = 128;

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_SORT_BUFFER_SIZE,
                            true, 
                            +[](int64_t val){ return (val >= 1024) &&
                            // Check against overflow...no more than an exabyte
                            (val <= size_t(1024*1024*1024)*size_t(1024*1024*1024)); });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_READ_BATCH_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });

}
}
