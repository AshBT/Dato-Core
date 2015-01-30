project(lambda)

make_library(pylambda
  SOURCES
    lambda_constants.cpp
    python_api.cpp
    pylambda.cpp
    pylambda_master.cpp
    graph_pylambda.cpp
    graph_pylambda_master.cpp
    lualambda_master.cpp
  REQUIRES
    python flexible_type cppipc sframe luastate luajit
)

make_executable(pylambda_worker
  SOURCES
    pylambda_worker.cpp
  REQUIRES
    pylambda tcmalloc
)
