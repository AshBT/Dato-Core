# Note: when executed in the build dir, then CMAKE_CURRENT_SOURCE_DIR is the
# build dir.

file(GLOB_RECURSE webappfiles "${CMAKE_ARGV3}/*")
foreach(item ${webappfiles})
  get_filename_component(fname ${item} NAME)
  if ( "${fname}" MATCHES ".*\\.png$"  OR
       "${fname}" MATCHES ".*\\.jpg$"  OR
       "${fname}" MATCHES ".*\\.jpeg$"  OR
       "${fname}" MATCHES ".*\\.js$"  OR
       "${fname}" MATCHES ".*\\.jsx$"  OR
       "${fname}" MATCHES ".*\\.html$"  OR
       "${fname}" MATCHES ".*\\.ico$"  OR
       "${fname}" MATCHES ".*\\.css$"  OR
       "${fname}" MATCHES "LICENSE$"  OR
       "${fname}" MATCHES ".*\\.otf$"  OR
       "${fname}" MATCHES ".*\\.eot$"  OR
       "${fname}" MATCHES ".*\\.svg$"  OR
       "${fname}" MATCHES ".*\\.ttf$"  OR
       "${fname}" MATCHES ".*\\.woff$")
    file(REMOVE ${item})
  endif()
endforeach()


file (REMOVE_RECURSE ${CMAKE_ARGV3}/docs)
