project(sgraph_test)

make_cxxtest(hilbert_par_for.cxx REQUIRES sgraph)
make_cxxtest(sgraph_test.cxx REQUIRES sgraph)
make_cxxtest(sgraph_compute_test.cxx REQUIRES sgraph)
make_executable(sgraph_bench SOURCES sgraph_bench.cpp REQUIRES sgraph)
