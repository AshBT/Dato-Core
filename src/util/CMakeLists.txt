project(util)

make_library(util SOURCES crash_handler.cpp any.cpp cityhash_gl.cpp EXTERNAL_VISIBILITY)
