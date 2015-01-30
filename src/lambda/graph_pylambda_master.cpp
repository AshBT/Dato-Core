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
#include <lambda/graph_pylambda_master.hpp>
#include <parallel/lambda_omp.hpp>
#include <lambda/lambda_constants.hpp>

namespace graphlab {

namespace lambda {

// Path of the pylambda_worker binary relative to the unity_server binary.
// The relative path will be set when unity_server starts.
std::string graph_pylambda_master::pylambda_worker_binary = "pylambda_worker";

graph_pylambda_master& graph_pylambda_master::get_instance() {
  static graph_pylambda_master instance(std::min<size_t>(DEFAULT_NUM_GRAPH_LAMBDA_WORKERS,
                                                         std::max<size_t>(thread::cpu_count(), 1)));
  return instance;
}

graph_pylambda_master::graph_pylambda_master(size_t nworkers) {
  std::vector<std::string> worker_addresses;
  for (size_t i = 0; i < nworkers; ++i) {
    worker_addresses.push_back(std::string("ipc://") + get_temp_name());
  }
  m_worker_pool.reset(new worker_pool<graph_lambda_evaluator_proxy>(nworkers,
        pylambda_worker_binary,
        worker_addresses));
}

} // end of lambda
} // end of graphlab
