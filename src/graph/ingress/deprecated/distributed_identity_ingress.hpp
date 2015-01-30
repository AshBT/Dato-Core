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

#ifndef GRAPHLAB_DISTRIBUTED_IDENTITY_INGRESS_HPP
#define GRAPHLAB_DISTRIBUTED_IDENTITY_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <rpc/buffered_exchange.hpp>
#include <graph/graph_basic_types.hpp>
#include <graph/ingress/distributed_ingress_base.hpp>
#include <graph/distributed_graph.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {
  template<typename VertexData, typename EdgeData>
  class distributed_graph;

  /**
   * \brief Ingress object assigning edges to the loading machine itself.
   */
  template<typename VertexData, typename EdgeData>
  class distributed_identity_ingress : 
    public distributed_ingress_base<VertexData, EdgeData> {
  public:
    typedef distributed_graph<VertexData, EdgeData> graph_type;
    /// The type of the vertex data stored in the graph 
    typedef VertexData vertex_data_type;
    /// The type of the edge data stored in the graph 
    typedef EdgeData   edge_data_type;

    typedef distributed_ingress_base<VertexData, EdgeData> base_type;

  public:
    distributed_identity_ingress(distributed_control& dc, graph_type& graph) :
    base_type(dc, graph) {
    } // end of constructor

    ~distributed_identity_ingress() { }

    /** Add an edge to the ingress object and assign the edge to itself. */
    void add_edge(vertex_id_type source, vertex_id_type target,
                  const EdgeData& edata) {
      typedef typename base_type::edge_buffer_record edge_buffer_record;
      const procid_t owning_proc = base_type::rpc.procid();
      const edge_buffer_record record(source, target, edata);
      base_type::edge_exchange.send(owning_proc, record);
    } // end of add edge
  }; // end of distributed_identity_ingress
}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>


#endif
