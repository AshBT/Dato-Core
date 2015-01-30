project(parallel)

make_library(parallel 
  SOURCES
    pthread_tools.cpp
    thread_pool.cpp
  REQUIRES
    logger
  EXTERNAL_VISIBILITY
)
