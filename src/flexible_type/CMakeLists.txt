project(flexible_type)

#/**************************************************************************/
#/*                                                                        */
#/*                             Logger Library                             */
#/*                                                                        */
#/**************************************************************************/

make_library(flexible_type
  SOURCES
    flexible_type.cpp
    flexible_type_record.cpp
    flexible_type_registry.cpp
    string_escape.cpp
    flexible_type_spirit_parser.cpp
  REQUIRES
    logger
    image_type
  EXTERNAL_VISIBILITY
  )
