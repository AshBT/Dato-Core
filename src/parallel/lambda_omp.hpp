/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_PARALLEL_LAMBDA_OMP_HPP
#define GRAPHLAB_PARALLEL_LAMBDA_OMP_HPP
#include <iterator>
#include <utility>
#include <functional>
#include <type_traits>
#include <parallel/thread_pool.hpp>
namespace graphlab {

/**
 * Runs a provided function in parallel, passing the function the thread ID 
 * and the number of threads. The thread ID is always between 0 and 
 * #threads - 1. 
 *
 * \code
 *  in_parallel([&](size_t thrid, size_t num_threads) {
 *                   std::cout << "Thread " << thrid << " out of " 
 *                             << num_threads << "\n";
 *                 });
 * \endcode
 *
 * \param fn The function to run. The function must take two size_t arguments:
 *           the thread ID and the number of threads.
 */
inline void in_parallel(const std::function<void (size_t thread_id, 
                                                  size_t num_threads)>& fn) {
  size_t nworkers = thread_pool::get_instance().size();

  if (thread::get_tls_data().is_in_thread() || nworkers <= 1) {

    fn(0, 1);
    return;

  } else {

    parallel_task_queue threads(thread_pool::get_instance());

    for (size_t i = 0;i < nworkers; ++i) {
      threads.launch([&fn, i, nworkers]() { fn(i, nworkers); }, i);
    }
    threads.join();
  }
}

/**
 * Returns the thread pool dedicated for running parallel for jobs.
 */
thread_pool& get_parfor_thread_pool();

/**
 * Runs a parallel for ranging from the integers 'begin' to 'end'.
 *
 * When run single threaded, is equivalent to
 * \code
 * for(size_t i = begin; i < end; ++i) {
 *   fn(i);
 * }
 * \endcode
 *
 * Example:
 * \code
 *  // performs an element wise multiplication of 'a' and 'b'
 *  std::vector<double> vec_multiple(const std::vector<double>& a,
 *                                   const std::vector<double>& b) {
 *    std::vector<double> ret(a.size(), 0); // allocate the return object
 *
 *    parallel_for(0, a.size(), [&](size_t i) {
 *                   ret[i] = a[i] * b[i]; 
 *                 });
 *
 *    return ret;
 *  }
 * \endcode
 *
 * \param begin The beginning integer of the for loop
 * \param end The ending integer of the for loop
 * \param fn The function to run. The function must take a single size_t 
 *           argument which is a current index.
 */
template <typename FunctionType>
void parallel_for(size_t begin,
                  size_t end,
                  const FunctionType& fn) {

  size_t nworkers = thread_pool::get_instance().size();

  if (thread::get_tls_data().is_in_thread() || nworkers <= 1) {
    // we do not support recursive calls to in parallel yet.
    for(size_t i = begin; i < end; ++i) {
      fn(i);
    }
  } else {
    parallel_task_queue threads(thread_pool::get_instance());
    size_t nlen = end - begin; // total range
    double split_size = (double)nlen / nworkers; // size of range each worker gets
    for (size_t i = 0; i < nworkers; ++i) {
      size_t worker_begin = begin + split_size * i; // beginning of this worker's range
      size_t worker_end = begin + split_size * (i + 1); // end of this worker's range
      if (i == nworkers - 1) worker_end = end;
      threads.launch([&fn, worker_begin, worker_end]() {
        size_t worker_iter = worker_begin;
        while (worker_iter < worker_end) {
          fn(worker_iter);
          ++worker_iter;
        }
      }, i);
    }
    threads.join();
  }
}


/**
 * Runs a map reduce operation for ranging from the integers 'begin' to 'end'.
 *
 * When run single threaded, is equivalent to
 * \code
 * T acc;
 * for(size_t i = begin; i < end; ++i) {
 *   acc = n(i, acc);
 * }
 * return acc;
 * \endcode
 *
 * Example:
 * \code
 *  // performs an inner product of 'a' and 'b'
 *  double vec_proc(const std::vector<double>& a,
 *                  const std::vector<double>& b) {
 *
 *    double result = fold_reduce(0, a.size(), [&](size_t i, double& acc) {
 *                          acc += a[i] * b[i];
 *                          return acc;
 *                    }, 0.0);
 *    return result;
 *  }
 * \endcode
 *
 * \param begin The beginning integer of the for loop
 * \param end The ending integer of the for loop
 * \param fn The function to run. The function must take a single size_t 
 *           argument which is a current index.
 */
template <typename FunctionType, typename ReduceType>
ReduceType fold_reduce (size_t begin,
                  size_t end,
                  const FunctionType& fn,
                  ReduceType base = ReduceType()) {
  size_t nworkers = thread_pool::get_instance().size();

  if (thread::get_tls_data().is_in_thread() || nworkers <= 1) {
    // we do not support recursive calls to in parallel yet.
    ReduceType acc = base;
    for(size_t i = begin; i < end; ++i) {
      fn(i, acc);
    }
    return acc;
  } else {
    parallel_task_queue threads(thread_pool::get_instance());

    size_t nlen = end - begin; // total rangeS
    double split_size = (double)nlen / nworkers; // size of range each worker gets

    std::vector<ReduceType> acc(nworkers, base);
    for (size_t i = 0;i < nworkers; ++i) {
      size_t worker_begin = begin + split_size * i; // beginning of this worker's range
      size_t worker_end = begin + split_size * (i + 1); // end of this worker's range
      if (i == nworkers - 1) worker_end = end;
      threads.launch([&fn, &acc, worker_begin, worker_end, i]() {
                       size_t worker_iter = worker_begin;
                       while (worker_iter < worker_end) {
                         fn(worker_iter, acc[i]);
                         ++worker_iter;
                       }
                     }, i);
    }
    threads.join();
    ReduceType ret = base;
    for (size_t i = 0; i < acc.size(); ++i) {
      ret += acc[i];
    }
    return ret;
  }
}

/**
 * Runs a parallel for over a random access iterator range
 *
 * When run single threaded, is equivalent to
 * \code
 * RandomAccessIterator iter = begin;
 * while (iter != end) {
 *   fn(*iiter);
 *   ++end;
 * }
 * \endcode
 *
 * Example:
 * \code
 *  // squares each element of the vector in place
 *  void square(std::vector<double>& v) {
 *    parallel_for(v.begin(), v.end(), [&](double& d) {
 *                   d = d * d;
 *                 });
 *
 *  }
 * \endcode
 *
 * \param begin The beginning integer of the for loop
 * \param end The ending integer of the for loop
 * \param fn The function to run. The function must take a single size_t 
 *           argument which is a current index.
 */
template <typename RandomAccessIterator, typename FunctionType>
inline void parallel_for(RandomAccessIterator iter_begin,
                         RandomAccessIterator iter_end,
                         const FunctionType& fn,
                         std::random_access_iterator_tag = typename std::iterator_traits<RandomAccessIterator>::iterator_category()) {

  size_t nworkers = thread_pool::get_instance().size();

  if (thread::get_tls_data().is_in_thread() || nworkers <= 1) {
    RandomAccessIterator iter = iter_begin;
    while (iter != iter_end) {
      fn(*iter);
      ++iter;
    }
  } else {
    parallel_task_queue threads(thread_pool::get_instance());

    size_t nlen = std::distance(iter_begin, iter_end); // number of elements 

    double split_size = (double)nlen / nworkers; // number of elements per worker

    for (size_t i = 0;i < nworkers; ++i) {
      size_t worker_begin = split_size * i;    // index this worker starts at
      size_t worker_end = split_size * (i + 1); // index this worker ends at
      if (i == nworkers - 1) worker_end = nlen;
      threads.launch(
          [&fn, worker_begin, worker_end, &iter_begin]() {
            RandomAccessIterator my_begin = iter_begin + worker_begin;
            RandomAccessIterator my_end = iter_begin + worker_end;
            while (my_begin != my_end) {
              fn(*my_begin);
              ++my_begin;
            }
          } );
    }
    threads.join();
  }
}


};

#endif
