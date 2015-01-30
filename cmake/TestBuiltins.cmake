# - this module looks for 128 bit integer support.  It sets up the
# type defs in util/int128_types.hpp.  Simply add ${INT128_FLAGS} to the
# compiler flags.

include(CheckTypeSize)

MACRO(CONFIGURE_BUILTIN_FUNCTION_FLAGS COMPILER_FLAGS_VARIABLE)

  ############################################################
  # Check for a number of intrinsics

  set(_BUILTIN_FLAGS "")

  set(TMP_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
  set(CMAKE_REQUIRED_FLAGS ${CMAKE_CXX_FLAGS_RELEASE})

  # crc
  check_cxx_source_compiles("int main(int argc, char** argv) { return __builtin_ia32_crc32di(0,0);}" _HAS_CRC32)

  if(_HAS_CRC32)
    set(_BUILTIN_FLAGS "${_BUILTIN_FLAGS} -DHAS_BUILTIN_CRC32"
      CACHE STRING "builtin options" FORCE)
    message("Found builtin for calculating CRC.")
  endif()

  # ctz
  check_cxx_source_compiles("int main(int argc, char** argv) { __builtin_ctz(1); __builtin_ctzl(1); return __builtin_ctzll(1);}" _HAS_CTZ)

  if(_HAS_CTZ)
    set(_BUILTIN_FLAGS "${_BUILTIN_FLAGS} -DHAS_BUILTIN_CTZ"
      CACHE STRING "builtin options" FORCE)
    message("Found builtin for counting bitwise trailing zeros.")
  endif()

  # clz
  check_cxx_source_compiles("int main(int argc, char** argv) { __builtin_clz(1); __builtin_clzl(1); return __builtin_clzll(1);}" _HAS_CLZ)

  if(_HAS_CLZ)
    set(_BUILTIN_FLAGS "${_BUILTIN_FLAGS} -DHAS_BUILTIN_CLZ"
      CACHE STRING "builtin options" FORCE)
    message("Found builtin for counting bitwise leading zeros.")
  endif()

  # popcount
  check_cxx_source_compiles("int main(int argc, char** argv) { __builtin_popcount(1); __builtin_popcountl(1); return __builtin_popcountll(1);}" _HAS_POPCOUNT)

  if(_HAS_POPCOUNT)
    set(_BUILTIN_FLAGS "${_BUILTIN_FLAGS} -DHAS_BUILTIN_POPCOUNT"
      CACHE STRING "builtin options" FORCE)
    message("Found builtin for counting bits.")
  endif()

  set(${COMPILER_FLAGS_VARIABLE} ${_BUILTIN_FLAGS})

endmacro()