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
#include <cxxtest/TestSuite.h>
#include <parallel/lambda_omp.hpp>



using namespace graphlab;

class lambda_omp_test: public CxxTest::TestSuite {
public:
  void test_parallel_for(void) {
    std::vector<int> ctr(100000);
    // parallel for over integers
    parallel_for(0, ctr.size(), [&](size_t idx) {
                      ctr[idx]++;
                    });
    for (size_t i = 0; i < ctr.size(); ++i) {
      TS_ASSERT_EQUALS(ctr[i], 1);
    }

    // fold
    int sum = fold_reduce(0, ctr.size(), 
                          [&](size_t idx, int& sum) {
                            sum += ctr[idx];
                          }, 0);
    TS_ASSERT_EQUALS(sum, 100000);

    // parallel for over iterators 
    parallel_for(ctr.begin(), ctr.end(), [&](int& c) {
                      c++;
                    });
    for (size_t i = 0; i < ctr.size(); ++i) {
      TS_ASSERT_EQUALS(ctr[i], 2);
    }

    // just do stuff in parallel
    in_parallel([&](size_t thrid, size_t num_threads) {
                     ctr[thrid]++;
                   });

    size_t nthreads = thread_pool::get_instance().size();
    for (size_t i = 0; i < nthreads; ++i) {
      TS_ASSERT_EQUALS(ctr[i], 3);
    }
    for (size_t i = nthreads; i < ctr.size(); ++i) {
      TS_ASSERT_EQUALS(ctr[i], 2);
    }

  }

  long fib (long n) {
    if (n <= 2) {
      return 1;
    } else {
      return fib(n-1) + fib(n-2);
    }
  }

  void test_parallel_for_fib(void) {

    std::vector<long> ls {40,40,40,40,40,40};
    std::cout << "----------------------------------------------------------" << std::endl;
    parallel_for(0, ls.size(), [&](size_t idx) {
                    std::cout << ls[idx] << ": " << fib(ls[idx]) << "\n";
                 });
  }

  void test_exception_forward() {
    std::vector<int> ctr(100000);
    // parallel for over integers
    TS_ASSERT_THROWS_ANYTHING(parallel_for((size_t)0, (size_t)100, 
                                           [&](size_t idx) {
                                             throw("hello world");
                                           }));

    TS_ASSERT_THROWS_ANYTHING(fold_reduce((size_t)0, (size_t)100, 
                                          [&](size_t idx, double& sum) {
                                            throw("hello world");
                                          }, 0.0));

    TS_ASSERT_THROWS_ANYTHING(parallel_for(ctr.begin(), ctr.end(), 
                                           [&](int& c) {
                                             throw("hello world");
                                           }));

    TS_ASSERT_THROWS_ANYTHING(in_parallel([&](size_t thrid, size_t num_threads) {
                                             throw("hello world");
                                          }));
  }
};
