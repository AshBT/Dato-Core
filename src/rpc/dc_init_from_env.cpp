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
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <cstdio>
#include <cstdlib>
#include <string>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_env.hpp>
#include <util/stl_util.hpp>
#include <logger/logger.hpp>
namespace graphlab {

bool init_param_from_env(dc_init_param& param) {
  char* nodeid = getenv("SPAWNID");
  if (nodeid == NULL) {
    return false;
  }
  param.curmachineid = atoi(nodeid);

  char* nodes = getenv("SPAWNNODES");
  std::string nodesstr = nodes;
  if (nodes == NULL) {
    return false;
  }

  param.machines = strsplit(nodesstr, ",");
  for (size_t i = 0;i < param.machines.size(); ++i) {
    param.machines[i] = param.machines[i] + ":" + tostr(10000 + i);
  }
  // set defaults
  param.numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS;
  param.commtype = RPC_DEFAULT_COMMTYPE;
  return true;
}

} // namespace graphlab

