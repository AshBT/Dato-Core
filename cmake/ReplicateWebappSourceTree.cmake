# Note: when executed in the build dir, then CMAKE_CURRENT_SOURCE_DIR is the
# build dir.
file( COPY . tests DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.html" PATTERN "*.css" PATTERN "*.ico" PATTERN "*.jpg" PATTERN "*.png" PATTERN "*.js" PATTERN "*.jsx" PATTERN "LICENSE" PATTERN "*.png" PATTERN "*.jpeg" PATTERN "*.jpg" PATTERN "*.otf" PATTERN "*.eot" PATTERN "*.svg" PATTERN "*.ttf" PATTERN "*.woff"
  PATTERN "CMakeFiles" EXCLUDE
  )
