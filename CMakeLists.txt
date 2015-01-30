project(GraphLab)

# We require the most recent version of cmake and automatically
# install the correct version when using the cmake lists
cmake_minimum_required(VERSION 2.8.3)

# Libraries linked via full path no longer produce linker search paths.
cmake_policy(SET CMP0003 NEW)
# Preprocessor definition values are now escaped automatically.
cmake_policy(SET CMP0005 NEW)

# for cmake 3.0
if(${CMAKE_MAJOR_VERSION} GREATER 2)
cmake_policy(SET CMP0045 OLD)
cmake_policy(SET CMP0046 OLD)
endif()

# Determine where additional GraphLab specific cmake modules are
# defined
set(CMAKE_MODULE_PATH ${GraphLab_SOURCE_DIR}/cmake)

# TODO: what is this used by
set(BUILD_SHARED_LIBS ON)

include(CheckLibraryExists)
include(CheckFunctionExists)
include(ExternalProject)
include(CheckCXXSourceCompiles)
# # Display information about cmake variables
# include(CMakePrintSystemInformation)

# set include path
include_directories(
  ${GraphLab_SOURCE_DIR}/src
  ${GraphLab_SOURCE_DIR}/cxxtest
  ${GraphLab_SOURCE_DIR}/deps/local/include)

if (APPLE)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(APPLE_CLANG false)
    message(STATUS "Using GCC Compiler on Mac")
  else()
    set(APPLE_CLANG true)
    message(STATUS "Using Clang Compiler on Mac")
  endif()
endif()

# set link path
link_directories(${GraphLab_SOURCE_DIR}/deps/local/lib)
# things which should be compiled as part of the graphlab source tree
add_definitions(-DIN_GRAPHLAB_SOURCE_TREE)
add_definitions(-DUSE_DYNAMIC_LOCAL_GRAPH)
if (DISABLE_CUDA)
  add_definitions(-DDISABLE_CUDA)
endif()

# locate the libgomp static
if(NO_OPENMP)
  set(OPENMP_C_FLAGS "")
  set(OPENMP_LIBRARIES "")
  add_definitions(-D__NO_OPENMP__)
else()
  find_package(OpenMP)
  if (OPENMP_FOUND)
    # figure out where gcc puts its stuff. Here is where I expect to find libgomp.a
    execute_process(COMMAND
      bash -c "${CMAKE_C_COMPILER} -print-search-dirs | sed -n -e's/libraries: =//p'| sed -e's/:/;/g'"
      OUTPUT_VARIABLE GCC_SEARCH_PATHS)
    find_library(OPENMP_LIBRARIES libgomp.a ${GCC_SEARCH_PATHS})
    message(STATUS "OpenMP Library: ${OPENMP_LIBRARIES}")
    set(OPENMP_C_FLAGS "-fopenmp")
    if (OPENMP_LIBRARIES)
    else()
      message(WARNING "Cannot statically link against OpenMP. Will dynamic link instead.")
      set(OPENMP_LIBRARIES "gomp")
    endif()
  else()
    message(WARNING "OpenMP Libraries were not found")
    set(OPENMP_C_FLAGS "")
    set(OPENMP_LIBRARIES "")
    add_definitions(-D__NO_OPENMP__)
  endif()
endif()

FIND_PACKAGE(PythonLibs)
if (PYTHONLIBS_FOUND)
  message("PythonLibs Found.")
  message("Include dirs of Python: " ${PYTHON_INCLUDE_DIRS} )
  message("Libs of Python: " ${PYTHON_LIBRARIES} )
  include_directories( ${PYTHON_INCLUDE_DIRS})
endif()

if(NOCPP11)
  message(WARNING "C++11 Disabled")
  set(CPP11_FLAGS "" CACHE STRING "C++11 enabling flags")
else()
  message(STATUS "C++11 Enabled")
  if (APPLE_CLANG)
    set(CPP11_FLAGS "-std=c++11 -stdlib=libc++ -Wno-deprecated-register -Wno-enum-compare -Wno-conversion-null -ftemplate-depth=900" CACHE STRING "C++11 enabling flags")
  else()
    set(CPP11_FLAGS "-std=c++11 -Wno-enum-compare -Wno-conversion-null -ftemplate-depth=900" CACHE STRING "C++11 enabling flags")
  endif()
endif()

if(VID32)
  message(STATUS "Using 32bit vertex id types")
  add_definitions(-DUSE_VID32)
endif()


find_program(HAS_CCACHE ccache)
if(HAS_CCACHE)
SET_PROPERTY(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
SET_PROPERTY(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

find_program(HAS_GOLD gold)
if(HAS_GOLD)
        SET(COMPILER_FLAGS "${COMPILER_FLAGS} -Bgold")
endif()
# Shared compiler flags used by all builds (debug, profile, release)
# Allow COMPILER_FLAGS to be used as options from the ./configure without 
# forcing all the other options I want to add to be lost
set(C_REAL_COMPILER_FLAGS "-Wall -g ${OPENMP_C_FLAGS} ${COMPILER_FLAGS}" CACHE STRING "common compiler options")
set(CPP_REAL_COMPILER_FLAGS "-Wall -g ${CPP11_FLAGS} ${OPENMP_C_FLAGS} ${COMPILER_FLAGS}" CACHE STRING "common compiler options")

# Disable address space randomization for OSX lion and above
if (APPLE_CLANG)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Xlinker -no_pie")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${GraphLab_SOURCE_DIR}/deps/local/lib")

# Set subdirectories
subdirs(src test)
if (IS_DIRECTORY demoapps)
  subdirs(demoapps)
endif()
if(EXPERIMENTAL)
  if (IS_DIRECTORY ${GraphLab_SOURCE_DIR}/experimental)
    subdirs(experimental)
  endif()
endif()

# Set installation rules
install(DIRECTORY src/
  DESTINATION include
  FILES_MATCHING PATTERN "*.hpp"  PATTERN "*.h"
  PATTERN ".svn" EXCLUDE
  )
install(CODE "execute_process(COMMAND ./scripts/install_deps.sh
  ${CMAKE_INSTALL_PREFIX} WORKING_DIRECTORY
  ${CMAKE_CURRENT_SOURCE_DIR})")


# If you add an external project, you should also update the builtin list
# in the add_builtin function below
include(ExternalProjectEigen)
include(ExternalProjectLibbz2)
include(ExternalProjectBoost)
if (NOT APPLE_CLANG)
  include(ExternalProjectTcmalloc)
else()
  # require_tcmalloc does nothing
  macro(requires_tcmalloc NAME)
  endmacro(requires_tcmalloc)
endif()
include(ExternalProjectLibEvent)
include(ExternalProjectLibJson)
include(ExternalProjectZookeeper)
include(ExternalProjectHDFS)
include(ExternalProjectLibSodium)
include(ExternalProjectZeroMQ)
include(ExternalProjectOpenSSL)
include(ExternalProjectLibCurl)
include(ExternalProjectPython)
include(ExternalProjectLibXML2)
include(ExternalProjectLuaJIT)
include(ExternalProjectAvroCPP)
include(ExternalProjectLibPNG)
include(ExternalProjectLibJpeg)
include(ExternalProjectODBC)
if (EXISTS "${GraphLab_SOURCE_DIR}/cmake/RequiresMshadow.cmake")
  include(RequiresMshadow)
endif()
# set(HDFS-FOUND OFF CACHE BOOL "Hadoop was found")


# MPI =========================================================================
# MPI is currently used to establish communication between rpc nodes
if(NO_MPI)
  message(STATUS "MPI disabled as a configure option.")
else()
  #find_package(MPICH2)
  find_package(MPI) 
  if(MPI_FOUND)
    message(STATUS "MPI Found: " ${MPI_LIBRARY} ${MPI_EXTRA_LIBRARY})
    add_definitions(-DHAS_MPI)
    include_directories(${MPI_INCLUDE_PATH})
  else(MPI_FOUND)
    message(STATUS "MPI Not Found! Distributed Executables will not be compiled")
    set(MPI_LIBRARY "")
    set(MPI_EXTRA_LIBRARY "")
    # ExternalProject_Add(openmpi
    #   PREFIX ${GraphLab_SOURCE_DIR}/deps/openmpi
    #   URL http://www.open-mpi.org/software/ompi/v1.4/downloads/openmpi-1.4.5.tar.gz
    #   CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
    #   INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local)
  endif(MPI_FOUND)
endif()


if(DISTRIBUTION)
  message(STATUS "Compiling for public distribution. Turning on static flags")

  if (NOT APPLE)
    # First line of gcc -Xlinker -version looks like this
    # GNU ld (GNU Binutils) 2.24.51.20131107
    # Rest of the cuts cut out the "2" and "24"
    set(GET_LD_MAJOR_COMMAND bash -c "gcc -Xlinker -version | head -1 | rev | cut -f 1 -d' '| rev | cut -f 1 -d.")
    set(GET_LD_MINOR_COMMAND bash -c "gcc -Xlinker -version | head -1 | rev | cut -f 1 -d' '| rev | cut -f 2 -d.")

    execute_process(COMMAND ${GET_LD_MAJOR_COMMAND}
                    OUTPUT_VARIABLE LD_MAJOR)

    execute_process(COMMAND ${GET_LD_MINOR_COMMAND}
                    OUTPUT_VARIABLE LD_MINOR)

    # for whatever reason, execute_process leaves a trailing newline behind 
    # in the output variable.
    # This is a little regex found online to drop the training newline

    STRING(REGEX REPLACE "(\r?\n)+$" "" LD_MAJOR "${LD_MAJOR}")
    STRING(REGEX REPLACE "(\r?\n)+$" "" LD_MINOR "${LD_MINOR}")
    message(STATUS "Binutils LD version: ${LD_MAJOR}.${LD_MINOR} Detected")
    
    # ok if version is at least 2.24
    set(BINUTILS_OK_VERSION 0)
    if ((LD_MAJOR GREATER 2) OR (LD_MAJOR EQUAL 2)) 
      if ((LD_MINOR GREATER 24) OR (LD_MINOR EQUAL 24)) 
        set(BINUTILS_OK_VERSION 1)
      endif()
    endif()

    if (BINUTILS_OK_VERSION EQUAL 0)
      message(FATAL_ERROR "Binutils 2.24 required to build distribution release on linux.")
    else()
      message(STATUS "Binutils 2.24 requirement satisfied for distribution release on Linux")
    endif()
  endif()
endif()


## ============================================================================
# System Checks

## ============================================================================
# Test for cpu affinity support
set(CMAKE_REQUIRED_LIBRARIES "pthread")
check_function_exists(pthread_setaffinity_np HAS_SET_AFFINITY)
set(CMAKE_REQUIRED_LIBRARIES ${crlbackup})

include(CheckCXXCompilerFlag)
## ============================================================================
# check if MARCH is set
if (NOT MARCH)
  #set to native if supported
  check_cxx_compiler_flag(-march=native HAS_MARCH_NATIVE)
  if(HAS_MARCH_NATIVE)
    set(MARCH "native")
  else()
    set(MARCH "x86-64")
  endif()
endif()

# check if MTUNE is set
if (NOT MTUNE)
  #set to native if supported
  check_cxx_compiler_flag(-mtune=native HAS_MTUNE_NATIVE)
  if(HAS_MTUNE_NATIVE)
    set(MTUNE "native")
  else()
    set(MTUNE "x86-64")
  endif()
endif()

## ============================================================================
# Setup compiler

# If profiling mode is enabled then additional profiling flags are set for
# the compiler
if (COMPILE_PROFILING MATCHES 1)
  set(PROFILING_FLAGS "-DUSE_EVENT_LOG -DUSE_TRACEPOINT")
else()
  set(PROFILING_FLAGS "")
endif()

# check for SSE instruction set (needed for CRC32)
#check_cxx_compiler_flag(-msse4.2 HAS_SSE42)
#set(SSE42_FLAG "")
#if (HAS_SSE42)
# set(SSE42_FLAG "-msse4.2")
#endif()

# Set up the 128 bit integer support; defines INT128_FLAGS
include(FindInt128)
Find_Int128_Types()


#disable Wno-unused-local-typedefs if available
check_cxx_compiler_flag(-Wno-unused-local-typedefs HAS_WNO_LOCAL_TYPEDEFS)
if(HAS_WNO_LOCAL_TYPEDEFS)
  set(WNO_LOCAL_TYPEDEFS "-Wno-unused-local-typedefs")
else()
  set(WNO_LOCAL_TYPEDEFS "")
endif()

#disable gcc-only flags on clang (newer clang with XCode 5.1 errors on these)
check_cxx_compiler_flag(-fpeel-loops HAS_FPEEL_LOOPS)
if(HAS_FPEEL_LOOPS)
  set(FPEEL_LOOPS "-fpeel-loops")
else()
  set(FPEEL_LOOPS "")
endif()
check_cxx_compiler_flag(-funswitch-loops HAS_FUNSWITCH_LOOPS)
if(HAS_FUNSWITCH_LOOPS)
  set(FUNSWITCH_LOOPS "-funswitch-loops")
else()
  set(FUNSWITCH_LOOPS "")
endif()
check_cxx_compiler_flag(-ftracer HAS_FTRACER)
if(HAS_FTRACER)
  set(FTRACER "-ftracer")
else()
  set(FTRACER "")
endif()

# if mac, but not using clang, we should use the clang linker anyway since
# the gcc linker seems to cause problems. Especially with -march=native
#
# see: 
# http://stackoverflow.com/questions/9840207/how-to-use-avx-pclmulqdq-on-mac-os-x-lion
set(ALTERNATE_LINKER "")
if(APPLE)
  if (NOT APPLE_CLANG)
    set(ALTERNATE_LINKER "-Wa,-q")
  endif()
endif()


# Set the debug flags
set(CMAKE_C_FLAGS_DEBUG
  "-O0 -Wno-attributes -march=${MARCH} -Winit-self ${PROFILING_FLAGS} ${C_REAL_COMPILER_FLAGS} -fno-inline ${ALTERNATE_LINKER}"
  CACHE STRING "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_DEBUG
  "-O0 ${WNO_LOCAL_TYPEDEFS} -Wno-attributes -march=${MARCH} -Winit-self ${PROFILING_FLAGS} ${CPP_REAL_COMPILER_FLAGS} ${INT128_FLAGS} ${ALTERNATE_LINKER} -fno-inline"
  CACHE STRING "compiler options" FORCE)

set(CMAKE_C_FLAGS_RELEASE
  "-O3 -Wno-attributes -march=${MARCH} -mtune=${MTUNE} ${PROFILING_FLAGS} ${C_REAL_COMPILER_FLAGS} -DNDEBUG ${ALTERNATE_LINKER}"
  CACHE STRING "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE
  "-O3 ${FPEEL_LOOPS} ${FUNSWITCH_LOOPS} ${FTRACER} ${WNO_LOCAL_TYPEDEFS} -Wno-attributes -march=${MARCH} -mtune=${MTUNE} ${PROFILING_FLAGS} ${CPP_REAL_COMPILER_FLAGS} ${INT128_FLAGS} ${ALTERNATE_LINKER} -DNDEBUG"
  CACHE STRING "compiler options" FORCE)

set(CMAKE_REQUIRED_FLAGS ${CMAKE_CXX_FLAGS_RELEASE})
check_cxx_source_compiles("int main(int argc, char** argv) { return __builtin_ia32_crc32di(0,0);}" HAS_CRC32)
set(CMAKE_REQUIRED_FLAGS "")
if(HAS_CRC32)
  set(CMAKE_CXX_FLAGS_RELEASE
      "${CMAKE_CXX_FLAGS_RELEASE} -DHAS_BUILTIN_CRC32"
      CACHE STRING "compiler options" FORCE)
endif()


set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Build Type")
if (CMAKE_BUILD_TYPE MATCHES "Release")
  message(STATUS "Release build with C++ flags: " ${CMAKE_CXX_FLAGS_RELEASE})
  message(STATUS "Release build with C flags: " ${CMAKE_C_FLAGS_RELEASE})
elseif(CMAKE_BUILD_TYPE MATCHES "Debug")
  message(STATUS "Debug build with C++ flags: " ${CMAKE_CXX_FLAGS_DEBUG})
  message(STATUS "Debug build with C flags: " ${CMAKE_C_FLAGS_DEBUG})
else()
  message(ERROR "Unknown build type: " ${CMAKE_BUILD_TYPE} "!. Rerun ./configure")
endif()




check_cxx_source_compiles("#include <ios>
                           #include <system_error>
                           int main(int argc, char** argv) { 
                             throw std::ios_base::failure(\"hello\", std::error_code());
                             }" COMPILER_HAS_IOS_BASE_FAILURE_WITH_ERROR_CODE)

if(COMPILER_HAS_IOS_BASE_FAILURE_WITH_ERROR_CODE)
  message(STATUS "Compiler supports ios_base::failure(str, error_code)")
  add_definitions(-DCOMPILER_HAS_IOS_BASE_FAILURE_WITH_ERROR_CODE)
else()
  message(STATUS "Compiler does not support ios_base::failure(str, error_code)")
endif()



## ============================================================================
## ============================================================================
## ============================================================================
# Setup testing tools
# Make sure testing is enabled
enable_testing()
# Use Python interpreter
find_package(PythonInterp)
set(CXXTESTGEN ${CMAKE_SOURCE_DIR}/cxxtest/cxxtestgen)
# create a macro to define a test
macro(ADD_CXXTEST NAME)
  if(PYTHONINTERP_FOUND)
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp
      COMMAND
      ${PYTHON_EXECUTABLE} ${CXXTESTGEN}
      --runner=XUnitPrinter
      --xunit-file=TestResults-${NAME}.xml
      -o ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp ${ARGV}
      DEPENDS ${ARGV}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      )
  endif(PYTHONINTERP_FOUND)
  add_graphlab_executable(${NAME}test ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp)
  set_source_files_properties( ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp
    PROPERTIES COMPILE_FLAGS "-I${CMAKE_CURRENT_SOURCE_DIR}" )

  add_test(${NAME} ${NAME}test)
endmacro(ADD_CXXTEST)


# copy_file ===================================================================
# copy a single file into build environment
macro(copy_file NAME)
  message(STATUS "Copying File: " ${NAME})
  file(INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/${NAME}
    DESTINATION   ${CMAKE_CURRENT_BINARY_DIR} )
endmacro(copy_file NAME)

# copy_files ==================================================================
# copy all files matching a pattern into the build environment
macro(copy_files NAME)
  message(STATUS "Copying Files: " ${NAME})
  file(INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION  ${CMAKE_CURRENT_BINARY_DIR}
    FILES_MATCHING PATTERN ${NAME} )
endmacro(copy_files NAME)



## =============================================================
# Core graphlab requirements
macro(requires_core_deps NAME)
  requires_boost(${NAME})
  target_link_libraries(${NAME} pthread ${OPENMP_LIBRARIES})
endmacro(requires_core_deps)

# force building of boost. Always
add_custom_target(external_dependencies)
requires_core_deps(external_dependencies)


# This function should not be used directly. Use requires(...) instead.
#
# Usage: add_builtin(my_prog eigen built_in_not_found)
# All ExternalProject builds are referenced here by name. Essentially,
#
#   add_builtin(my_prog curl built_in_not_found)
#
# performs 
#
#   requires_curl(my_prog)
#
# The built_in_not_found variable is set to TRUE in the builtin is found,
#
function(add_builtin targetname builtinname builtin_found_var)
  set(${builtin_found_var} TRUE PARENT_SCOPE)
  if(builtinname STREQUAL "boost")
    requires_boost(${targetname})
  elseif(builtinname STREQUAL "eigen")
    requires_eigen(${targetname})
  elseif(builtinname STREQUAL "hdfs")
    requires_hdfs(${targetname})
  elseif(builtinname STREQUAL "curl")
    requires_curl(${targetname})
  elseif(builtinname STREQUAL "openssl")
    requires_openssl(${targetname})
  elseif(builtinname STREQUAL "event")
    requires_libevent(${targetname})
  elseif(builtinname STREQUAL "jpeg")
    requires_libjpeg(${targetname})
  elseif(builtinname STREQUAL "png")
    requires_libpng(${targetname})
  elseif(builtinname STREQUAL "json")
    requires_libjson(${targetname})
  elseif(builtinname STREQUAL "xml2")
    requires_libxml2(${targetname})
  elseif(builtinname STREQUAL "bz2")
    requires_libbz2(${targetname})
  elseif(builtinname STREQUAL "python")
    requires_python(${targetname})
  elseif(builtinname STREQUAL "tcmalloc")
    requires_tcmalloc(${targetname})
  elseif(builtinname STREQUAL "v8")
    requires_v8(${targetname})
  elseif(builtinname STREQUAL "zeromq")
    requires_zeromq(${targetname})
  elseif(builtinname STREQUAL "libsodium")
    requires_libsodium(${targetname})
  elseif(builtinname STREQUAL "zookeeper")
    requires_zookeeper(${targetname})
  elseif(builtinname STREQUAL "mpi")
    if(MPI_FOUND)
      target_link_libraries(${targetname} ${MPI_LIBRARY} ${MPI_EXTRA_LIBRARY})
    endif(MPI_FOUND)
  elseif(builtinname STREQUAL "luajit")
    requires_luajit(${targetname})
  elseif(builtinname STREQUAL "avrocpp")
    requires_avrocpp(${targetname})
  elseif(builtinname STREQUAL "odbc")
    requires_odbc(${targetname})
  else()
    set(${builtin_found_var} FALSE PARENT_SCOPE)
  endif()
endfunction()

include(CMakeParseArguments)

# This is an internal function and should not be used directly. 
#
# Usage: recursive_expand_requirements(output_var target_name)
#
# Example: recursive_expand_requirements(output_var fileio)
# output_var = "logger;dl;pthread;z;iconv"
# 
# This enumerates the list in the target property GRAPHLAB_MAKE_LIBRARY_REQUIRES
# of the target "target_name" storing it in the output_var. It then 
# recursively calls recursive_expand_requirements on each of the elements of the
# list and appends it to the output_var.
#
# Essentially, the target property GRAPHLAB_MAKE_LIBRARY_REQUIRES stores the
# list of all direct dependencies of a given target library. This function 
# recursively enumerates all the dependencies of the target library.
function(recursive_expand_requirements VarName TargetName Acc)
  get_target_property(requires_val ${TargetName} GRAPHLAB_MAKE_LIBRARY_REQUIRES)
  if(requires_val)
    foreach(item ${requires_val})
      list(FIND Acc "${item}" has_item)
      #message(STATUS "${result} contains ${item} = ${has_item}")
      if(has_item EQUAL -1)
        set(Acc ${Acc} ${item})
        recursive_expand_requirements(locallist ${item} "${Acc}")
        set(Acc ${locallist})
      endif()
    endforeach()
  endif()
  SET(${VarName} "${Acc}" PARENT_SCOPE)
endfunction()

# This is an internal function and should not be used directly. Use "requires"
#
# Usage: recursive_requires_library(target_name library BuiltinOnly)
#
# Example: recursive_requires_library(wscmd fileio)
# 
# This causes the specified target to depend on, and link against one library,
# as well as all recursive dependencies of the library. Builtin names are 
# automatically resolved
#
# If BuiltinOnly is set, only builtin libraries are added.
function(recursive_requires_library TargetName Library BuiltinOnly)
  add_builtin(${TargetName} ${Library} builtin_found)
  if(NOT builtin_found)
    # no builtin found. Do the full recursive expansion
    get_target_property(is_empty ${Library} GRAPHLAB_MAKE_LIBRARY_EMPTY)
    if(NOT BuiltinOnly)
      add_dependencies(${TargetName} ${Library})
    if(NOT is_empty)
      target_link_libraries(${TargetName} ${Library})
    endif()
    endif()
    message(STATUS "${TargetName} requires minimally ${Library}")

    recursive_expand_requirements(locallist "${Library}" "")
    if (locallist)
      #  message(STATUS "${TargetName} recursively requires ${locallist}")
    endif()
    foreach(item ${locallist})
      add_builtin(${TargetName} ${item} builtin_found)
      if(NOT builtin_found)
        get_target_property(is_empty ${item} GRAPHLAB_MAKE_LIBRARY_EMPTY)
        if(NOT BuiltinOnly)
          add_dependencies(${TargetName} ${item})
        if(NOT is_empty)
          target_link_libraries(${TargetName} ${item})
        endif()
        endif()
      endif()
    endforeach()
  endif()
endfunction()

# This is an external function
# Usage:
#   requires(target [... list of library requirements ...])
#
# Example:
#   add_graphlab_executable(wscmd ...)
#   requires(wscmd fileio logger)
#
# This causes the specified target to depend on, and link against all listed
# libraries, as well as all recursive dependencies.Builtin names are 
# automatically resolved.
function(requires NAME)
  foreach(item ${ARGN})
    recursive_requires_library(${NAME} "${item}" FALSE)
  endforeach()
  foreach(item ${ARGN})
    recursive_requires_library(${NAME} "${item}" FALSE)
  endforeach()
endfunction()


# This is an external function
# Usage:
#   requires_builtin_only(target [... list of library requirements ...])
#
# Example:
#   add_graphlab_executable(wscmd ...)
#   requires(wscmd fileio logger)
#
# This causes the specified target to depend on, and all listed
# libraries, as well as all recursive dependencies, adding only builtin 
# dependencies.
function(requires_builtin_only NAME)
  foreach(item ${ARGN})
    recursive_requires_library(${NAME} "${item}" TRUE)
  endforeach()
endfunction()


# This is an internal function and should not be used
# Usage:
# make_target_impl(target compile_flags sources requirements is_library)
#
# Example:
# make_target_impl(fileio "-fPIC" 
#                   "oss_webstor/asyncurl.cpp;oss_webstor/sysutils.cpp"
#                   "logger;dl;pthread;z"
#                   TRUE)
#
# This generates a target library/binary with the given name. The optional
# compile_flags are appended to the target compile flags. "-fPIC" is ALWAYS
# added for libraries. "sources" is a list listing all the library/binary
# source files.  "requirements" is a list listing all the libraries, and
# builtins this target depends on. IS_LIBRARY must be "TRUE" or "FALSE"
#
# Boost, pthread is always added as a default dependency. OpenMP is added
# when possible.
macro(make_target_impl NAME FLAGS SOURCES REQUIREMENTS IS_LIBRARY)
  # create the target
  if (${IS_LIBRARY})
    message(STATUS "Adding Library: ${NAME}")
    add_library(${NAME} STATIC ${SOURCES})
  else()
    message(STATUS "Adding Executable: ${NAME}")
    add_executable(${NAME} ${SOURCES})
  endif()

  # default dependencies
  requires_boost(${NAME})
  target_link_libraries(${NAME} pthread ${OPENMP_LIBRARIES})

  # add a custom property to the target listing its dependencies
  set_target_properties(${NAME} PROPERTIES GRAPHLAB_MAKE_LIBRARY_REQUIRES "${REQUIREMENTS}")
  if(NOT ${FLAGS} STREQUAL "")
    set_property(TARGET ${NAME} APPEND_STRING PROPERTY COMPILE_FLAGS " ${FLAGS}") 
  endif()
  if (${IS_LIBRARY})
    set_property(TARGET ${NAME} APPEND_STRING PROPERTY COMPILE_FLAGS " -fPIC") 
  endif()


  # libraries are ok with just headers really.
  if(${IS_LIBRARY})
    requires_builtin_only(${NAME} "${REQUIREMENTS}")
  else()
    requires(${NAME} "${REQUIREMENTS}")
  endif()

  # if it is a library, create the requires_${NAME} macro.
  # This is for backward compatibility reasons. 
  # This can be omitted in the future.
  if(${IS_LIBRARY})
    function(requires_${NAME} N)
      recursive_requires_library(${N} ${NAME} FALSE)
    endfunction() 
  endif()
endmacro()

# This is an external function
# Usage:
#    make_library(NAME target
#                 SOURCES a.cpp b.cpp
#                 REQUIRES libx liby
#                 MAC_REQUIRES libz libzz
#                 LINUX_REQUIRES libk libj)
# Example:  
# 
# make_library(NAME fileio
#              SOURCES
#                oss_webstor/asyncurl.cpp
#                oss_webstor/sysutils.cpp
#                oss_webstor/wsconn.cpp
#                s3_api.cpp
#                hdfs.cpp
#               REQUIRES
#                 logger dl pthread z curl xml2 openssl
#               MAC_REQUIRES
#                 iconv
#                 )
# This generates a library with the provided target name. 
#
# NAME and SOURCES must be specified.
# REQUIRES lists all dependent libraries. These can be:
#   - other libraries built by the the graphlab build system
#   - builtin libraries
#   - system libraries
# MAC_REQUIRES lists all dependent libraries which are included only on Mac.
# LINUX_REQUIRES lists all dependent libraries which are included only on Linux.
# 
# All other targets which depends on this library (using the "requires" function)
# will automatically include all recursive dependencies.
#
# Boost, pthread is always added as a default dependency. OpenMP is added
# when possible.

# this enumerates all the libraries exported with "internal visibility"
set_property(GLOBAL PROPERTY INTERNAL_VISIBILITY_LIBRARY_LIST "")
# this is a unfortunately, hand-curated list of libraries which should not be exported.
# What is known:
#    tcmalloc should not be here. unexporting it will cause issues
#    libboost_system should not be here. needed for the SDK to behave
#    - really maintaining this list is stupidly annoying. Basically:
#       - Make sure everything works
#       - Make sure what the SDK headers use are NOT here.
set_property(GLOBAL PROPERTY UNEXPORTED_LIBRARY_LIST "libavrocpp_s.a:libboost_chrono.a:libboost_context.a:libboost_coroutine.a:libboost_date_time.a:libboost_filesystem.a:libboost_iostreams.a:libboost_program_options.a:libboost_python.a:libboost_random.a:libboost_regex.a:libboost_thread.a:libbz2.a:libcrypto.a:libcurl.a:libevent.a:libevent_core.a:libevent_extra.a:libevent_pthreads.a:libjpeg.a:libjson.a:libluajit.a:libpng16.a:libpng.a:libprofiler.a:libsodium.a:libssl.a:libxml2.a:libzmq.a:libzookeeper_mt.a:libzookeeper_st.a")

# utility function to join a list
function(join VALUES GLUE OUTPUT)
  string (REPLACE ";" "${GLUE}" _TMP_STR "${VALUES}")
  set (${OUTPUT} "${_TMP_STR}" PARENT_SCOPE)
endfunction()

macro(make_library NAME)
  set(options EXTERNAL_VISIBILITY)
  set(one_value_args COMPILE_FLAGS)
  set(multi_value_args SOURCES REQUIRES MAC_REQUIRES LINUX_REQUIRES)
 CMAKE_PARSE_ARGUMENTS(make_library "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(NOT make_library_SOURCES) 
    MESSAGE(FATAL_ERROR "make_library call with no sources")
  endif()


  if (APPLE)
    if (make_library_MAC_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_MAC_REQUIRES})
    endif()
  else()
    if (make_library_LINUX_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_LINUX_REQUIRES})
    endif()
  endif()

  if (NOT make_library_EXTERNAL_VISIBILITY)
    MESSAGE(STATUS "${NAME} Internal visibility")
    make_target_impl("${NAME}" "${make_library_COMPILE_FLAGS} -fvisibility=hidden"
            "${make_library_SOURCES}" "${make_library_REQUIRES}" TRUE)
    set_property(GLOBAL APPEND PROPERTY INTERNAL_VISIBILITY_LIBRARY_LIST "lib${NAME}")
  else()
    MESSAGE(STATUS "${NAME} External visibility")
    make_target_impl("${NAME}" "${make_library_COMPILE_FLAGS}"
            "${make_library_SOURCES}" "${make_library_REQUIRES}" TRUE)
  endif()
  if(DISTRIBUTION)
    if(NOT APPLE_CLANG)
      set_property(TARGET ${NAME} APPEND_STRING PROPERTY LINK_FLAGS "-static-libstdc++")
    endif()
  endif()
endmacro()



# This is an external function
# Usage:
#    make_empty_library(NAME target
#                       REQUIRES libx liby
#                       MAC_REQUIRES libz libzz
#                       LINUX_REQUIRES libk libj)
# Example:  
# 
# make_empty_library(NAME graph
#               REQUIRES
#                 logger dl pthread z curl xml2 openssl
#               MAC_REQUIRES
#                 iconv
#                 )
# This generates an empty target with the provided target name, but all
# other targets which require this target will inherit all REQUIRED
# dependencies.
#
# NAME must be specified
# REQUIRES lists all dependent libraries. These can be:
#   - other libraries built by the the graphlab build system
#   - builtin libraries
#   - system libraries
# MAC_REQUIRES lists all dependent libraries which are included only on Mac.
# LINUX_REQUIRES lists all dependent libraries which are included only on Linux.
# 
# All other targets which depends on this library (using the "requires" function)
# will automatically include all recursive dependencies.
macro(make_empty_library NAME)
  set(one_value_args COMPILE_FLAGS)
  set(multi_value_args REQUIRES MAC_REQUIRES LINUX_REQUIRES)
  CMAKE_PARSE_ARGUMENTS(make_library "" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if (APPLE)
    if (make_library_MAC_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_MAC_REQUIRES})
    endif()
  else()
    if (make_library_LINUX_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_LINUX_REQUIRES})
    endif()
  endif()

  add_custom_target(${NAME})
  set_target_properties(${NAME} PROPERTIES GRAPHLAB_MAKE_LIBRARY_REQUIRES "${make_library_REQUIRES}")
  set_target_properties(${NAME} PROPERTIES GRAPHLAB_MAKE_LIBRARY_EMPTY TRUE)
endmacro()


# This is an external function
# Usage:
#    make_binary(NAME target
#                SOURCES a.cpp b.cpp
#                REQUIRES libx liby
#                MAC_REQUIRES libz libzz
#                LINUX_REQUIRES libk libj)
# Example:  
# 
# make_binary(NAME wscmd 
#              SOURCES
#                wscmd.cpp
#              REQUIRES
#                fileio
#             )
#
# This generates a binary with the provided target name. 
#
# NAME and SOURCES must be specified.
# REQUIRES lists all dependent libraries. These can be:
#   - other libraries built by the the graphlab build system
#   - builtin libraries
#   - system libraries
# MAC_REQUIRES lists all dependent libraries which are included only on Mac.
# LINUX_REQUIRES lists all dependent libraries which are included only on Linux.
# 
# All other targets which depends on this library (using the "requires" function)
# will automatically include all recursive dependencies.
#
# Boost, pthread is always added as a default dependency. OpenMP is added
# when possible.
function (make_executable NAME)
  set(one_value_args COMPILE_FLAGS)
  set(multi_value_args SOURCES REQUIRES MAC_REQUIRES LINUX_REQUIRES)
  CMAKE_PARSE_ARGUMENTS(make_library "" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(NOT make_library_SOURCES) 
    MESSAGE(FATAL_ERROR "make_library call with no sources")
  endif()

  if (APPLE)
    if (make_library_MAC_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_MAC_REQUIRES})
    endif()
  else()
    if (make_library_LINUX_REQUIRES)
      set(make_library_REQUIRES ${make_library_REQUIRES} ${make_library_LINUX_REQUIRES})
    endif()
  endif()

  make_target_impl("${NAME}" "${make_library_COMPILE_FLAGS}"
    "${make_library_SOURCES}" "${make_library_REQUIRES}" FALSE)
  if(DISTRIBUTION)
    if(NOT APPLE_CLANG)
      set_property(TARGET ${NAME} APPEND_STRING PROPERTY LINK_FLAGS "-static-libgcc -static-libstdc++")
    endif()
  endif()
  if(NOT APPLE)
    get_property(INTERNAL_VISIBILITY_LIBRARY_LIST GLOBAL PROPERTY INTERNAL_VISIBILITY_LIBRARY_LIST)
    join("${INTERNAL_VISIBILITY_LIBRARY_LIST}" ":" output)
    get_property(UNEXPORTED_LIBRARY_LIST GLOBAL PROPERTY UNEXPORTED_LIBRARY_LIST)
    set_property(TARGET ${NAME} APPEND_STRING PROPERTY LINK_FLAGS " -Wl,-exclude-lib,${UNEXPORTED_LIBRARY_LIST}:${output} ")
  endif()
endfunction()

function (make_cxxtest NAME)
  set (SOURCES ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp)
  if(PYTHONINTERP_FOUND)
    add_custom_command(
      OUTPUT ${SOURCES}
      COMMAND
      ${PYTHON_EXECUTABLE} ${CXXTESTGEN}
      --runner=XUnitPrinter
      --xunit-file=TestResults-${NAME}.xml
      -o ${SOURCES} ${NAME}
      DEPENDS ${NAME}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      )
  endif(PYTHONINTERP_FOUND)

  set(args ${ARGN})
  make_executable(${NAME}test SOURCES ${SOURCES} ${args})

  set_source_files_properties( ${SOURCES}
    PROPERTIES COMPILE_FLAGS "-I${CMAKE_CURRENT_SOURCE_DIR}" )

  add_test(${NAME} ${NAME}test)
endfunction()



# This is an external function
# Usage:
#    add_graphlab_executable(target sources)
#
# Example:
#    add_graphlab_executable(my_prog a.cpp b.cpp)
#    requires(my_prog fileio logger)
#
# Creates a binary executable using the source files listed.
# Provided for compatibility with older GraphLab OSS stuff.
macro(add_graphlab_executable NAME)
  add_executable(${NAME} ${ARGN})
  requires(${NAME} boost mpi json zookeeper event)
  requires_graphlab(${NAME})
  target_link_libraries(${NAME} pthread ${OPENMP_LIBRARIES})
endmacro(add_graphlab_executable)
