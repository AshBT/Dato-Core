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
#ifndef GRAPHLAB_LAMBDA_LUALAMBDA_MASTER_HPP
#define GRAPHLAB_LAMBDA_LUALAMBDA_MASTER_HPP

#include <mutex>
#include <thread>
#include <memory>
#include <queue>
#include <condition_variable>
#include <flexible_type/flexible_type.hpp>

namespace lua {
class State;
};

namespace graphlab {
namespace lambda {

  /**
   * \ingroup lambda
   *
   * The lualambda_master provides functions to evaluate a lambda
   * function on different input types (single value, list, dictionary)
   * in parallel.
   *
   * Internally, it manages a forked pool of lualambda_workers, and each
   * evaluation call goes to the a worker, and block until the evaluation
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
   * lualambda_master master(10);
   *
   * // Evaluate a single argument.
   * // plus_one_lambda is equivalent to lambda x: x + 1
   * auto lambda_hash = master.make_lambda(plus_one_lambda);
   *
   * flexible_type one = master.eval(lambda_hash, 0);
   * flexible_type two = master.eval(lambda_hash, 1);
   *
   *
   * // Evaluate in parallel, still using plus_one_lambda.
   * std::vector< flexible_type > x_plus_one;
   * parallel_for(0, args.size(), [&](size_t i) {
   *    x_plus_one[i] = master.eval(lambda_hash, args[i]);
   * });
   * for (auto val : args) {
   *   ASSERT_EQ(master.eval(lambda_hash, val) == (val + 1));
   * }
   * master.release_lambda(plus_one_lambda);
   *
   * // Evaluate a list of arguments.
   * master.make_lambda(sum_lambda);
   * flexible_type ten = master.eval(lambda_hash, args);
   * master.release_lambda(sum_lambda);
   * \endcode
   *
   */
  class lualambda_master {

   public:

    static lualambda_master& get_instance();

    size_t make_lambda(const std::string& lambda_str);
    void release_lambda(size_t lambda_hash);

    /**
     * Evaluate lambda on batch of inputs.
     */
    std::vector<flexible_type> bulk_eval(size_t lambda_hash, const std::vector<flexible_type>& args, bool skip_undefined, int seed);

    /**
     * Lambda takes dictionary argument.
     */
    std::vector<flexible_type> bulk_eval(size_t lambda_hash,
        const std::vector<std::string>& keys,
        const std::vector<std::vector<flexible_type>>& args, bool skip_undefined, int seed);

    inline size_t num_workers() { return clients.size(); }

   private:

    lualambda_master(size_t nworkers = 8);

    ~lualambda_master() { shutdown(); }

    lualambda_master(lualambda_master const&) = delete;

    lualambda_master& operator=(lualambda_master const&) = delete;

    /**
     * Launch(fork) n lualambda_worker and creates n clients
     * for each of the worker.
     */
    void start(size_t nworkers);

    /**
     * Terminates all workers.
     */
    void shutdown();

    /**
     * Get the next avaiable worker. Block if the all workers are busy.
     */
    size_t pop_worker();

    /**
     * Make the worker avaiable by pushing it back to the worker_queue.
     */
    void push_worker(size_t);


   private:
    std::vector<std::shared_ptr<lua::State>> clients;

    // A simple concurrent queue for scheduling.
    std::mutex mtx;
    std::condition_variable cv;
    std::queue<size_t> worker_queue;
  };
} // end lambda
} // end graphlab

#endif
