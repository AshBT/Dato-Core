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
#ifndef GRAPHLAB_SGRAPH_SGRAPH_SYNCHRONIZE_INTERFACE_HPP
#define GRAPHLAB_SGRAPH_SGRAPH_SYNCHRONIZE_INTERFACE_HPP

#include<sgraph/sgraph_types.hpp>

namespace graphlab {
namespace sgraph_compute {

/**
 * Defines the intereface for serializing vertex data and edge data
 * of an sgraph.
 *
 * \ref vertex_partition_exchange is the struct that can hold
 * a subset data of a subset of vertices from a sgraph partition.
 *
 * The choice for sparse vertices packing is motivated by the
 * "triple_apply" computation pattern:
 * as we process each edge partition, the associated vertex partition
 * are sparsely visited and updated.
 *
 * \ref sgraph_synchronize_interface is used for both ends of the communication 
 * to deal with initialization, sending and receiving the vertex and edge exchange data.
 *
 * Example:
 * \code
 *
 * ////  Worker side ////
 * sgraph_synchronize_interface* worker_graph_sync; // a client side implementation
 *
 * // initialize vertex data in partition 0, using all_vertices sent from server.
 * worker_graph_sync.load_vertex_partition(0, all_vertices_from_server);
 *
 * // recevie a vertex_exchange from server, let's update the local vertices 
 * worker_graph_sync->update_vertex_partition(vexchange_from_server);
 *
 * // do some work to update vertex data in partition 0, and obtain a set of changed vertex data..
 * vertex_exchange updated_vertices = worker_graph_sync->worker_graph_sync->get_vertex_partition_exchange(0);
 *
 * // now we can send updated_vertices to the server.
 *
 * //// Server side ////
 * sgraph_synchronize_interface* server_graph_sync; // a server side implementation
 *
 * // call to worker to initialize vertex partition.
 *
 * // recevie a vertex_exchange server, let's update the local vertices
 * server_graph_sync->update_vertex_partition(vexchange_from_client);
 *
 * // do some work to update vertex data in partition 0, and obtain a set of changed vertex data..
 * vertex_exchange updated_vertices = server_graph_sync->get_vertex_partition_exchange(0);
 *
 * // now we can send updated_vertices to the worker.
 *
 * \endcode
 */


/**
 * Storing a subset of vertex data of a subset of vertices from an sgraph partition.
 *
 * The vertex data can be a subset of fields, but all vertices
 * in the same exchange object must contain the same set of fields. 
 */
struct vertex_partition_exchange {

  /// id of the partition that vertices belong to.
  size_t partition_id;

  /**
   * index and data pair of the vertices to be exchanged.
   * vertices[i] : (vindex, vdata)
   * where \ref vindex is the local_id of the vertex in the partition;
   * and \ref vdata contains the subset of vertex data.
   * The subset is defined by \ref field_ids.
   */
  std::vector<std::pair<size_t, sgraph_vertex_data>> vertices;

  /// A subset of field ids the vertex data contain.
  std::vector<size_t> field_ids;

  void save(oarchive& oarc) const {
    oarc << vertices << field_ids << partition_id;
  }
  void load(iarchive& iarc) {
    iarc >> vertices >> field_ids >> partition_id;
  }
};

class sgraph_synchronize_interface {
 public:
  // Vertex sync
  /// Given a vector of all vertices of partition, initialize the local vertex storage.
  virtual void load_vertex_partition(size_t partition_id, std::vector<sgraph_vertex_data>& all_vertices) = 0;

  /// Given a vertex exchange object, update the local vertex storage.
  virtual void update_vertex_partition(vertex_partition_exchange& vpartition_exchange) = 0;

  /// Obtain a vertex exchange object containing a subset of vertices and fields.
  virtual vertex_partition_exchange get_vertex_partition_exchange(size_t partition_id, const std::unordered_set<size_t>& vertex_ids, const std::vector<size_t>& field_ids) = 0;

  virtual ~sgraph_synchronize_interface() { }
};

}
}
#endif
