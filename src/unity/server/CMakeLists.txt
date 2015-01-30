project(unity)

make_executable(unity_server
                SOURCES
                  unity_server_init.cpp
                  unity_server.cpp
                  exported_symbols.cpp
                REQUIRES
                  unity
                  unity_toolkits
                  product_key
                  cppipc
                  perf
                  tcmalloc
                  query_process
                  boost
                  metric
)
if (APPLE)
  SET_PROPERTY(TARGET unity_server APPEND_STRING PROPERTY LINK_FLAGS " -pagezero_size 10000 -image_base 100000000 ") 
endif()

add_dependencies(unity_server pylambda_worker rddtosf_pickle rddtosf_nonpickle sftordd_pickle ./src/sframe/graphlab-create-spark-integration.jar)
