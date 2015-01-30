project(unity)

make_library(unity_graph_analytics
  SOURCES
    kcore_sgraph.cpp
    connected_component_sgraph.cpp
    graph_coloring_sgraph.cpp
    triangle_counting_sgraph.cpp
    sssp_sgraph.cpp
    pagerank_sgraph.cpp
  REQUIRES
    unity
  )
