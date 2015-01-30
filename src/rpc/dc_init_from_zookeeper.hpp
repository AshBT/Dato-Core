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


#ifndef GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP
#define GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP
#include <rpc/dc.hpp>
namespace graphlab {
  /**
   * \ingroup rpc
   * initializes parameters from ZooKeeper. Returns true on success.
   * To initialize from Zookeeper, the following environment variables must be set
   *
   * ZK_SERVERS: A comma separated list of zookeeper servers. Port
   *             number must be included.
   * ZK_JOBNAME: The name of the job to use. This must be unique to the cluster.
   *             i.e. no other job with the same name must run at the same time
   * ZK_NUMNODES: The number of processes to wait for
   *
   */
  bool init_param_from_zookeeper(dc_init_param& param);
}

#endif // GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP

