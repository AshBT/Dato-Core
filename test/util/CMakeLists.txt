project(fault_test)

make_executable(time_fast_power
  SOURCES time_fast_power.cpp
  REQUIRES util logger random)

make_executable(test_crash_handler
  SOURCES test_crash_handler.cpp
  REQUIRES util)

make_cxxtest(fast_power_test.cxx REQUIRES util logger random)

make_cxxtest(cityhash_gl.cxx REQUIRES util logger random)
