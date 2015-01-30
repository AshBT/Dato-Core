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
#ifndef GRAPHLAB_GRAPH_TRANSFORM_EDGES_HPP
#define GRAPHLAB_GRAPH_TRANSFORM_EDGES_HPP
#include <rpc/dc.hpp>
#include <parallel/lambda_omp.hpp>
namespace graphlab {


/**
 * \brief Performs a transformation operation on each edge in the graph.
 *
 * Given a mapfunction, transform_edges() calls mapfunction on
 * every edge in graph. The map function may make modifications
 * to the data on the edge. transform_edges() must be called on
 * all machines simultaneously.
 *
 * ### Basic Usage
 * For instance, if the graph has integer vertex data, and integer edge
 * data:
 * \code
 *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
 * \endcode
 *
 * To set each edge value to be the number of out-going edges
 * of the target vertex, we may write the following:
 * \code
 * void set_edge_value(graph_type::edge_type& edge) {
 *   edge.data() = edge.target().num_out_edges();
 * }
 * \endcode
 *
 * Calling transform_edges():
 * \code
 *   graph.transform_edges(set_edge_value);
 * \endcode
 * will run the <code>set_edge_value()</code> function
 * on each edge in the graph, setting its new value.
 *
 * The two optional arguments vset and edir may be used to restrict
 * the set of edges operated upon.
 *
 * \tparam EdgeMapperType The type of the map function.
 *                          Not generally needed.
 *                          Can be inferred by the compiler.
 * \param graph The distributed graph to run on
 * \param mapfunction The map function to use. Must take an
 *                   \ref icontext_type& as its first argument, and
 *                   a \ref edge_type, or a reference to a
 *                   \ref edge_type as its second argument.
 *                   Returns void.
 */

template <typename GraphType, typename TransformType>
void transform_edges(GraphType& g, TransformType transform_functor) {
  typedef typename GraphType::local_edge_type local_edge_type;
  typedef typename GraphType::edge_type edge_type;
  g.dc().barrier();
  parallel_for(0, g.num_local_vertices(), [&](size_t i) {
      for(const local_edge_type& e: g.l_vertex(i).in_edges()) {
        edge_type edge(e);
        transform_functor(edge);
      }
    });
  g.dc().barrier();
};
} // graphlab
#endif

