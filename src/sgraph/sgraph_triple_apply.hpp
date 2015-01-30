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
#ifndef GRAPHLAB_SGRAPH_SGRAPH_TRIPLE_APPLY
#define GRAPHLAB_SGRAPH_SGRAPH_TRIPLE_APPLY

#include<flexible_type/flexible_type.hpp>
#include<sgraph/sgraph.hpp>
#include<sgraph/sgraph_compute_vertex_block.hpp>

namespace graphlab {
namespace sgraph_compute {

typedef std::vector<flexible_type> vertex_data;
typedef std::vector<flexible_type> edge_data;
typedef sgraph::vertex_partition_address vertex_partition_address;
typedef sgraph::edge_partition_address edge_partition_address;

/**
 * Provide access to an edge scope (Vertex, Edge, Vertex);
 * The scope object permits read, modify both vertex data
 * and the edge data.
 */
class edge_scope {
 public:
  /// Provide vertex data access
  vertex_data& source() { return *m_source; }

  const vertex_data& source() const { return *m_source; }

  vertex_data& target() { return *m_target; }

  const vertex_data& target() const { return *m_target; }

  /// Provide edge data access
  edge_data& edge() { return *m_edge; }

  const edge_data& edge() const { return *m_edge; }

  /// Lock both source and target vertices
  // The lock pointers can be null in which case the function has no effect.
  void lock_vertices() {
    if (m_lock_0 && m_lock_1) {
      if (m_lock_0 == m_lock_1) {
        m_lock_0->lock();
      } else {
        m_lock_0->lock();
        m_lock_1->lock();
      }
    }
  };

  /// Unlock both source and target vertices
  void unlock_vertices() {
    if (m_lock_0 && m_lock_1) {
      if (m_lock_0 == m_lock_1) {
        m_lock_0->unlock();
      } else {
        m_lock_0->unlock();
        m_lock_1->unlock();
      }
    }
  };

  /// Do not construct edge_scope directly. Used by triple_apply_impl.
  edge_scope(vertex_data* source, vertex_data* target, edge_data* edge,
             graphlab::mutex* lock_0=NULL,
             graphlab::mutex* lock_1=NULL) :
      m_source(source), m_target(target), m_edge(edge),
      m_lock_0(lock_0), m_lock_1(lock_1) { }

 private:
  vertex_data* m_source;
  vertex_data* m_target;
  edge_data* m_edge;
  // On construction, the lock ordering is gauanteed: lock_0 < lock_1.
  graphlab::mutex* m_lock_0;
  graphlab::mutex* m_lock_1;
};

typedef std::function<void(edge_scope&)> triple_apply_fn_type;

typedef std::function<void(std::vector<edge_scope>&)> batch_triple_apply_fn_type;

/**
 * Apply a transform function on each edge and its associated source and target vertices in parallel.
 * Each edge is visited once and in parallel. The modification to vertex data will be protected by lock.
 *
 * The effect of the function is equivalent to the following pesudo-code:
 * \code
 * parallel_for (edge in g) {
 *  lock(edge.source(), edge.target())
 *  apply_fn(edge.source().data(), edge.data(), edge.target().data());
 *  unlock(edge.source(), edge.target())
 * }
 * \endcode
 *
 * \param g The target graph to perform the transformation.
 * \param apply_fn The user defined function that will be applied on each edge scope. 
 * \param mutated_vertex_fields A subset of vertex data columns that the apply_fn will modify.
 * \param mutated_edge_fields A subset of edge data columns that the apply_fn will modify.
 *
 * The behavior is undefined when mutated_vertex_fields, and mutated_edge_fields are
 * inconsistent with the apply_fn function.
 */
void triple_apply(sgraph& g,
                  triple_apply_fn_type apply_fn,
                  const std::vector<std::string>& mutated_vertex_fields,
                  const std::vector<std::string>& mutated_edge_fields = {});


/**
 * Overload. Uses python lambda function.
 */
void triple_apply(sgraph& g, const std::string& lambda_str,
                  const std::vector<std::string>& mutated_vertex_fields,
                  const std::vector<std::string>& mutated_edge_fields = {});




/**************************************************************************/
/*                                                                        */
/*                        Internal. Test Only API                         */
/*                                                                        */
/**************************************************************************/

/**
 * Overload. Take the apply function that processes a batch of edges at once.
 * Used for testing the building block of lambda triple apply.
 */
void triple_apply(sgraph& g,
                  batch_triple_apply_fn_type batch_apply_fn,
                  const std::vector<std::string>& mutated_vertex_fields,
                  const std::vector<std::string>& mutated_edge_fields = {});

/**
 * Mock the single triple apply using batch_triple_apply implementation.
 * Used for testing only.
 */
void batch_triple_apply_mock(sgraph& g,
                  triple_apply_fn_type apply_fn,
                  const std::vector<std::string>& mutated_vertex_fields,
                  const std::vector<std::string>& mutated_edge_fields = {});
}
}

#endif
