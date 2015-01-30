# Build Boost =================================================================


set(PATCHCMD "(patch -N -p1 -i ${CMAKE_SOURCE_DIR}/patches/boost_date_time_parsers2.patch || true)")
if (APPLE_CLANG)
  SET(ADD_BOOST_BOOTSTRAP --with-toolset=clang)
  SET(tmp "-std=c++11 -stdlib=libc++")
  SET(tmp2 "-stdlib=libc++")
  SET(ADD_BOOST_COMPILE_TOOLCHAIN toolset=clang cxxflags=${tmp} linkflags=${tmp2})
  UNSET(tmp)
  UNSET(tmp2)
elseif(APPLE)
  set(PATCHCMD "${PATCHCMD} && cp ${CMAKE_SOURCE_DIR}/patches/user-config.jam ./tools/build/src/")
  SET(ADD_BOOST_COMPILE_TOOLCHAIN toolset=darwin-4.9)
else()
  SET(ADD_BOOST_BOOTSTRAP "")
  SET(ADD_BOOST_COMPILE_TOOLCHAIN "")
endif()

ExternalProject_Add(boost
  PREFIX ${CMAKE_SOURCE_DIR}/deps/boost
  URL "http://glbin-engine.s3.amazonaws.com/deps/boost_1_56_0.tar.gz"
  URL_MD5 8c54705c424513fa2be0042696a3a162
  BUILD_IN_SOURCE 1
  INSTALL_DIR ${CMAKE_SOURCE_DIR}/deps/local 
  PATCH_COMMAND sh -c ${PATCHCMD}
  CONFIGURE_COMMAND
  ./bootstrap.sh
  ${ADD_BOOST_BOOTSTRAP}
  --with-libraries=filesystem
  --with-libraries=program_options
  --with-libraries=system
  --with-libraries=iostreams
  --with-libraries=date_time
  --with-libraries=random
  --with-libraries=context
  --with-libraries=thread
  --with-libraries=chrono
  --with-libraries=python
  --with-libraries=coroutine
  --with-libraries=regex
  --prefix=<INSTALL_DIR>
  BUILD_COMMAND rm -rf <INSTALL_DIR>/include/boost && 
  C_INCLUDE_PATH=${CMAKE_SOURCE_DIR}/deps/local/include
  CPLUS_INCLUDE_PATH=${CMAKE_SOURCE_DIR}/deps/local/include
  LIBRARY_PATH=${CMAKE_SOURCE_DIR}/deps/local/lib
  ./b2 ${ADD_BOOST_COMPILE_TOOLCHAIN} install link=static variant=release threading=multi runtime-link=static cxxflags=-fPIC
  INSTALL_COMMAND ./b2 tools/bcp && cp dist/bin/bcp ${CMAKE_SOURCE_DIR}/deps/local/bin
  )

# we need to clean the build first. boost did not clean the include properly.
# and there is some issue going from 1.53 to 1.54

set(BOOST_ROOT ${CMAKE_SOURCE_DIR}/deps/local )
set(BOOST_LIBS_DIR ${CMAKE_SOURCE_DIR}/deps/local/lib)
set(Boost_LIBRARIES
  ${BOOST_LIBS_DIR}/libboost_filesystem.a
  ${BOOST_LIBS_DIR}/libboost_program_options.a
  ${BOOST_LIBS_DIR}/libboost_system.a
  ${BOOST_LIBS_DIR}/libboost_iostreams.a
  ${BOOST_LIBS_DIR}/libboost_context.a
  ${BOOST_LIBS_DIR}/libboost_thread.a
  ${BOOST_LIBS_DIR}/libboost_chrono.a
  ${BOOST_LIBS_DIR}/libboost_date_time.a
  ${BOOST_LIBS_DIR}/libboost_coroutine.a
  ${BOOST_LIBS_DIR}/libboost_regex.a
  ${BOOST_LIBS_DIR}/libboost_python.a)
add_dependencies(boost libbz2)
message(STATUS "Boost libs: " ${Boost_LIBRARIES})
# add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)
# add_definitions(-DBOOST_ALL_DYN_LINK)
# set(Boost_SHARED_LIBRARIES "")
foreach(blib ${Boost_LIBRARIES})
  message(STATUS "Boost libs: " ${blib})
  string(REGEX REPLACE "\\.a$" ${CMAKE_SHARED_LIBRARY_SUFFIX} bout ${blib})
  message(STATUS "Boost dyn libs: " ${bout})
  set(Boost_SHARED_LIBRARIES ${Boost_SHARED_LIBRARIES} ${bout})
endforeach()
message(STATUS "Boost Shared libs: " ${Boost_SHARED_LIBRARIES})


macro(requires_boost NAME)
  add_dependencies(${NAME} boost)
  target_link_libraries(${NAME} ${Boost_LIBRARIES})
  if (APPLE)
    target_link_libraries(${NAME} z)
  else()
    target_link_libraries(${NAME} z rt)
  endif()
endmacro(requires_boost)
