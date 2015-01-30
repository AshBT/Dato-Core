project(image)


#/**************************************************************************/
#/*                                                                        */
#/*                             Logger Library                             */
#/*                                                                        */
#/**************************************************************************/

make_library(image_util
  SOURCES
    io.cpp
    image_util.cpp
    jpeg_io.cpp
    png_io.cpp
  REQUIRES
    png
    jpeg
    logger
    fileio
  )

make_library(image_type
  SOURCES
    image_type.cpp
  REQUIRES
    logger
  EXTERNAL_VISIBILITY
)

make_executable(io_example
  SOURCES
    io_example.cpp
  REQUIRES
    image_util
)

#set to native if supported
check_cxx_compiler_flag(-Wno-c++11-narrowing HAS_NARROWING)

if(HAS_NARROWING)
set_property(TARGET image_util APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-c++11-narrowing")
endif()
