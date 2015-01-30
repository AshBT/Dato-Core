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
#include <sgraph/sgraph_constants.hpp>
#include <graphlab/util/bitops.hpp>
#include <globals/globals.hpp>
namespace graphlab {

size_t SGRAPH_TRIPLE_APPLY_LOCK_ARRAY_SIZE = 1024 * 1024;
size_t SGRAPH_BATCH_TRIPLE_APPLY_LOCK_ARRAY_SIZE = 1024 * 1024;
size_t SGRAPH_TRIPLE_APPLY_EDGE_BATCH_SIZE = 1024;
size_t SGRAPH_DEFAULT_NUM_PARTITIONS = 8;
size_t SGRAPH_INGRESS_VID_BUFFER_SIZE = 1024 * 1024;

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_TRIPLE_APPLY_LOCK_ARRAY_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_BATCH_TRIPLE_APPLY_LOCK_ARRAY_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_TRIPLE_APPLY_EDGE_BATCH_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_DEFAULT_NUM_PARTITIONS, 
                            true, 
                            +[](int64_t val){ return val >= 1 && is_power_of_2((uint64_t)val); });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_INGRESS_VID_BUFFER_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });
}
