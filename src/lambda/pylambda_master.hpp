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
#ifndef GRAPHLAB_LAMBDA_PYLAMBDA_MASTER_HPP
#define GRAPHLAB_LAMBDA_PYLAMBDA_MASTER_HPP

#include<lambda/lambda_interface.hpp>
#include<lambda/worker_pool.hpp>

namespace graphlab {

namespace lambda {

  /**
   * \ingroup lambda
   *
   * The pylambda_master provides functions to evaluate a lambda
   * function on different input types (single value, list, dictionary)
   * in parallel.
   *
   * Internally, it manages a worker pool of pylambda_workers.
   *
   * Each evaluation call is allocated to a worker, and block until the evaluation
   * returns or throws an exception.
   *
   * The evaluation functions can be called in parallel. When this happens,
   * the master evenly allocates the jobs to workers who has the shortest job queue.
   *
   * \code
   *
   * std::vector<flexible_type> args{0,1,2,3,4};
   *
   * // creates a master with 10 workers;
   * pylambda_master master(10);
   *
   * // Evaluate a single argument.
   * // plus_one_lambda is equivalent to lambda x: x + 1
   * auto lambda_hash = master.make_lambda(plus_one_lambda);
   *
   * flexible_type one = master.bulk_eval(lambda_hash, {0});
   * flexible_type two = master.bulk_eval(lambda_hash, {1});
   *
   *
   * // Evaluate in parallel, still using plus_one_lambda.
   * std::vector< flexible_type > x_plus_one;
   * parallel_for(0, args.size(), [&](size_t i) {
   *    x_plus_one[i] = master.bulk_eval(lambda_hash, {args[i]});
   * });
   * for (auto val : args) {
   *   ASSERT_EQ(master.bulk_eval(lambda_hash, {val}) == (val + 1));
   * }
   * master.release_lambda(plus_one_lambda);
   *
   * \endcode
   */
  class pylambda_master {
   public:

    static pylambda_master& get_instance();

    /**
     * Register the lambda_str for all workers, and returns the id for the lambda.
     * Throws the exception  
     */
    size_t make_lambda(const std::string& lambda_str);

    /**
     * Unregister the lambda_str.
     */
    void release_lambda(size_t lambda_hash) noexcept;

    /**
     * Evaluate lambda on batch of inputs.
     */
    std::vector<flexible_type> bulk_eval(size_t lambda_hash, const std::vector<flexible_type>& args, bool skip_undefined, int seed);

    /**
     * \overload
     * Lambda takes dictionary argument.
     */
    std::vector<flexible_type> bulk_eval(size_t lambda_hash,
        const std::vector<std::string>& keys,
        const std::vector<std::vector<flexible_type>>& args, bool skip_undefined, int seed);


    inline size_t num_workers() { return m_worker_pool->num_workers(); }

    static void set_pylambda_worker_binary(const std::string& path) { pylambda_worker_binary = path; };

   private:

    pylambda_master(size_t nworkers = 8);

    pylambda_master(pylambda_master const&) = delete;

    pylambda_master& operator=(pylambda_master const&) = delete;

   private:
    std::shared_ptr<worker_pool<lambda_evaluator_proxy>> m_worker_pool;

    static std::string pylambda_worker_binary;
  };

} // end lambda
} // end graphlab

#endif
