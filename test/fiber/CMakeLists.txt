project(fiber_test)


make_executable(fiber_test SOURCES fiber_test.cpp REQUIRES fiber)
make_executable(fibo_fiber_test SOURCES fibo_fiber_test.cpp REQUIRES fiber)
