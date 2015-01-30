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
#include <lambda/pylambda_master.hpp>
#include <lambda/lambda_utils.hpp>
#include <parallel/lambda_omp.hpp>
#include <fileio/temp_files.hpp>
#include <algorithm>
#include <lambda/lambda_constants.hpp>

namespace graphlab {
namespace lambda {

  // Path of the pylambda_worker binary relative to the unity_server binary.
  // The relative path will be set when unity_server starts.
  std::string pylambda_master::pylambda_worker_binary = "pylambda_worker";

  pylambda_master& pylambda_master::get_instance() {
    static pylambda_master instance(std::min<size_t>(DEFAULT_NUM_PYLAMBDA_WORKERS,
                                                     std::max<size_t>(thread::cpu_count(), 1)));
    return instance;
  };

  pylambda_master::pylambda_master(size_t nworkers) {
    std::vector<std::string> worker_addresses;
    for (size_t i = 0; i < nworkers; ++i) {
      worker_addresses.push_back(std::string("ipc://") + get_temp_name());
    }
    m_worker_pool.reset(new worker_pool<lambda_evaluator_proxy>(nworkers,
                                                                pylambda_worker_binary,
                                                                worker_addresses));

  }

  size_t pylambda_master::make_lambda(const std::string& lambda_str) {
    size_t lambda_hash = (size_t)(-1);
    auto all_workers = m_worker_pool->get_all_workers();
    try {
      parallel_for (0, m_worker_pool->num_workers(), [&](size_t i) {
        auto worker_proxy = all_workers[i];
        auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
        size_t hash = worker_proxy->make_lambda(lambda_str);
        DASSERT_MSG(lambda_hash == (size_t)(-1) || lambda_hash == hash, "workers should return the same lambda index");
        lambda_hash = hash;
      });
      return lambda_hash;
    } catch (cppipc::ipcexception e) {
      release_lambda(lambda_hash);
      throw(reinterpret_comm_failure(e));
    } catch (...) {
      release_lambda(lambda_hash);
      throw;
    }
  }

  void pylambda_master::release_lambda(size_t lambda_hash) noexcept {
    auto all_workers = m_worker_pool->get_all_workers();
    try {
      parallel_for (0, m_worker_pool->num_workers(), [&](size_t i) {
        auto worker_proxy = all_workers[i];
        auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
        logstream(LOG_INFO) << "Proxy " << worker_proxy << " releasing lambda hash: " << lambda_hash << std::endl;
        worker_proxy->release_lambda(lambda_hash);
      });
    } catch (std::exception e){
      logstream(LOG_ERROR) << "Error on releasing lambda: " << e.what() << std::endl;
    } catch (std::string e) {
      logstream(LOG_ERROR) << "Error on releasing lambda: " << e << std::endl;
    } catch (const char* e) {
      logstream(LOG_ERROR) << "Error on releasing lambda: " << e << std::endl;
    } catch (...) {
      logstream(LOG_ERROR) << "Error on releasing lambda: Unknown error" << std::endl;
    }
  }


  std::vector<flexible_type> pylambda_master::bulk_eval(size_t lambda_hash, const std::vector<flexible_type>& args, bool skip_undefined, int seed) {

    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);

    std::vector<flexible_type>ret;
    // catch and reinterpret comm failure
    try {
      ret = worker_proxy->bulk_eval(lambda_hash, args, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
    return ret;
  }

  std::vector<flexible_type> pylambda_master::bulk_eval(size_t lambda_hash,
      const std::vector<std::string>& keys,
      const std::vector<std::vector<flexible_type>>& values,
      bool skip_undefined, int seed) {
    std::vector<flexible_type>ret;
    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
    // catch and reinterpret comm failure
    try {
      ret = worker_proxy->bulk_eval_dict(lambda_hash, keys, values, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
    return ret;
  }

} // end of lambda
} // end of graphlab
