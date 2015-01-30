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


#include <rpc/net_util.hpp>
#include <rpc/mpi_tools.hpp>

namespace graphlab {
  namespace mpi_tools {
    void get_master_ranks(std::set<size_t>& master_ranks) {
      uint32_t local_ip = get_local_ip();
      std::vector<uint32_t> all_ips;
      all_gather(local_ip, all_ips);
      std::set<uint32_t> visited_ips;
      master_ranks.clear();
      for(size_t i = 0; i < all_ips.size(); ++i) {
        if(visited_ips.count(all_ips[i]) == 0) {
          visited_ips.insert(all_ips[i]);
          master_ranks.insert(i);
        }
      }
    }
  } 
}

