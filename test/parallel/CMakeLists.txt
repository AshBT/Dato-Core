project(parallel_test)


make_cxxtest(thread_tools.cxx REQUIRES parallel)
make_cxxtest(lambda_omp_test.cxx REQUIRES fiber)
