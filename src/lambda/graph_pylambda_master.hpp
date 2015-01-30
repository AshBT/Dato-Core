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
#ifndef GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_MASTER_HPP
#define GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_MASTER_HPP

#include<lambda/graph_lambda_interface.hpp>
#include<lambda/worker_pool.hpp>

namespace graphlab {

namespace lambda {
  /**
   * \ingroup lambda
   *
   * Simple singleton object managing a worker_pool of graph lambda workers.
   */
  class graph_pylambda_master {
   public:

    static graph_pylambda_master& get_instance();

    inline size_t num_workers() { return m_worker_pool->num_workers(); }

    static void set_pylambda_worker_binary(const std::string& path) { pylambda_worker_binary = path; };

    inline std::shared_ptr<worker_pool<graph_lambda_evaluator_proxy>> get_worker_pool() {
      return m_worker_pool;
    }

   private:

    graph_pylambda_master(size_t nworkers = 8);

    graph_pylambda_master(graph_pylambda_master const&) = delete;

    graph_pylambda_master& operator=(graph_pylambda_master const&) = delete;

   private:
    std::shared_ptr<worker_pool<graph_lambda_evaluator_proxy>> m_worker_pool;

    static std::string pylambda_worker_binary;
  };
} // end lambda
} // end graphlab

#endif
