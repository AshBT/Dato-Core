project(random)

#/**************************************************************************/
#/*                                                                        */
#/*                             Random Library                             */
#/*                                                                        */
#/**************************************************************************/

make_library(random
  SOURCES
    random.cpp
    alias.cpp
  REQUIRES
    boost
    logger
    parallel
  EXTERNAL_VISIBILITY
  )
