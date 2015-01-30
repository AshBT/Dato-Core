project(sframe_test)

make_executable(sframe_bench SOURCES sframe_bench.cpp REQUIRES sframe python)
make_cxxtest(sframe_test.cxx REQUIRES sframe python)
make_cxxtest(shuffle_test.cxx REQUIRES sframe)
make_cxxtest(sarray_file_format_v1_test.cxx REQUIRES sframe python)
make_cxxtest(sarray_file_format_v2_test.cxx REQUIRES sframe python)
make_cxxtest(sarray_test.cxx REQUIRES sframe python)
make_cxxtest(integer_pack_test.cxx REQUIRES sframe)
