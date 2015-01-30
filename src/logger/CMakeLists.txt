project(logger)

#/**************************************************************************/
#/*                                                                        */
#/*                             Logger Library                             */
#/*                                                                        */
#/**************************************************************************/

make_library(logger
  SOURCES
    logger.cpp
    backtrace.cpp
    log_rotate.cpp
  REQUIRES
    boost
    timer
    parallel
   EXTERNAL_VISIBILITY
  )
