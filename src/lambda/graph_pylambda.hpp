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
#ifndef GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_EVALUATOR_HPP
#define GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_EVALUATOR_HPP
#include<lambda/graph_lambda_interface.hpp>
#include<flexible_type/flexible_type.hpp>
#include<vector>

// Forward delcaration
namespace boost {
  namespace python {
    namespace api {
      class object;
    }
  }
}

namespace graphlab {

namespace lambda {

/**
 * Implements sgraph synchronizing interface for graph pylambda worker.
 *
 * In the pylambda environment, vertex data are stored locally as python dictionary object. 
 * This class provides the following synchronization functionality:
 * 1) Load a python object vertex partition given the flexible_type vertex partition. 
 * 2) Update the python object vertex partition given a vertex_partition_exchange. 
 * 3) Create a vertex_partition_exchange object from the stored python object vertex partition.
 */
class pysgraph_synchronize : public sgraph_compute::sgraph_synchronize_interface {
 public:
  void init(size_t num_partitions, const std::vector<std::string>& vertex_keys);

  /**
   * Load a python object vertex partition given the flexible_type vertex partition. 
   */
  void load_vertex_partition(size_t partition_id, std::vector<sgraph_vertex_data>& vertices);
  
  /**
   * Update the python object vertex partition given a vertex_partition_exchange. 
   */
  void update_vertex_partition(vertex_partition_exchange& vpartition_exchange);

  /**
   * Create a vertex_partition_exchange object from the stored python object vertex partition.
   */
  vertex_partition_exchange get_vertex_partition_exchange(size_t partition_id, const std::unordered_set<size_t>& vertex_ids, const std::vector<size_t>& field_ids);

  inline std::vector<sgraph_vertex_data>& get_partition(size_t partition_id) {
    DASSERT_LT(partition_id, m_num_partitions);
    DASSERT_TRUE(is_loaded(partition_id));
    return m_vertex_partitions[partition_id];
  }

  inline bool is_loaded(size_t partition_id) {
    DASSERT_LT(partition_id, m_num_partitions);
    return m_is_partition_loaded[partition_id];
  }

  inline void clear() {
    m_vertex_partitions.clear();
    m_is_partition_loaded.clear();
    m_vertex_keys.clear();
    m_num_partitions = 0;
  }

 private:
  std::vector<std::vector<sgraph_vertex_data>> m_vertex_partitions;
  std::vector<bool> m_is_partition_loaded;
  std::vector<std::string> m_vertex_keys;
  size_t m_num_partitions;
};


class graph_pylambda_evaluator : public graph_lambda_evaluator_interface {
 public:

  /**************************************************************************/
  /*                                                                        */
  /*                       Constructor and Destructor                       */
  /*                                                                        */
  /**************************************************************************/
  graph_pylambda_evaluator();

  ~graph_pylambda_evaluator();

  void init(const std::string& lambda,
            size_t num_partitions,
            const std::vector<std::string>& vertex_fields,
            const std::vector<std::string>& edge_fields,
            size_t src_column_id, size_t dst_column_id);

  void clear();


  /**************************************************************************/
  /*                                                                        */
  /*                        Communication Interface                         */
  /*                                                                        */
  /**************************************************************************/
  /**
   * Load a python object vertex partition given the flexible_type vertex partition. 
   */
  inline void load_vertex_partition(size_t partition_id, std::vector<sgraph_vertex_data>& vertices) {
    logstream(LOG_INFO) << "graph_lambda_worker load partition " << partition_id << std::endl;
    m_graph_sync.load_vertex_partition(partition_id, vertices);
  }

  inline bool is_loaded(size_t partition_id) {
    return m_graph_sync.is_loaded(partition_id);
  }

  /**
   * Update the pyobject vertex partition with the vertex_partition_exchange.
   */
  inline void update_vertex_partition(vertex_partition_exchange& vpartition_exchange) {
    logstream(LOG_INFO) << "graph_lambda_worker update partition "
      << vpartition_exchange.partition_id << std::endl;
    m_graph_sync.update_vertex_partition(vpartition_exchange);
  }

  /**
   * Create a vertex_partition_exchange object from the python vertex partition.
   */
  inline vertex_partition_exchange get_vertex_partition_exchange(size_t partition_id, const std::unordered_set<size_t>& vertex_ids, const std::vector<size_t>& field_ids) {
    logstream(LOG_INFO) << "graph_lambda_worker get partition " << partition_id << std::endl;
    return m_graph_sync.get_vertex_partition_exchange(partition_id, vertex_ids, field_ids);
  }

  /**************************************************************************/
  /*                                                                        */
  /*                           Compute Interface                            */
  /*                                                                        */
  /**************************************************************************/
  /**
   * Apply the lambda function on a vector of edge data, and return
   * the vector of mutated edge data.
   *
   * The return vector is aligned with the input vector, but each element only contains
   * the fields specified mutated_edge_field_ids.
   */
  std::vector<sgraph_edge_data> eval_triple_apply(const std::vector<sgraph_edge_data>& all_edge_data,
                                                  size_t src_partition, size_t dst_partition,
                                                  const std::vector<size_t>& mutated_edge_field_ids = {});

 private:
  void make_lambda(const std::string& pylambda_str);

  std::vector<std::string> m_vertex_keys;
  std::vector<std::string> m_edge_keys;
  size_t m_srcid_column;
  size_t m_dstid_column;
  boost::python::api::object* m_current_lambda = NULL;

  pysgraph_synchronize m_graph_sync;
};

} // end of lambda
} // end of graphlab

#endif
