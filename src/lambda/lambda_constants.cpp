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
#include <lambda/lambda_constants.hpp>
#include <graphlab/util/bitops.hpp>
#include <globals/globals.hpp>

namespace graphlab {

size_t DEFAULT_NUM_PYLAMBDA_WORKERS = 16;

size_t DEFAULT_NUM_GRAPH_LAMBDA_WORKERS = 16;

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            DEFAULT_NUM_PYLAMBDA_WORKERS,
                            true, 
                            +[](int64_t val){ return val >= 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            DEFAULT_NUM_GRAPH_LAMBDA_WORKERS,
                            true, 
                            +[](int64_t val){ return val >= 1; });
}
