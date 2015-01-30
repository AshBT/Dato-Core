project(unity)

subdirs(
  python
  lib
  toolkits
  server
  )


#/**************************************************************************/
#/*                                                                        */
#/*                            Unity Libraries                             */
#/*                                                                        */
#/**************************************************************************/

make_library(unity_common
  SOURCES
    lib/api/function_closure_info.cpp
    lib/variant_converter.cpp
    lib/variant.cpp
  REQUIRES
    flexible_type
    unity_sdk
  EXTERNAL_VISIBILITY)


make_library(unity_common_without_sdk_types
  SOURCES
    lib/api/function_closure_info.cpp
    lib/variant_converter.cpp
    lib/variant.cpp
  REQUIRES
    flexible_type
  EXTERNAL_VISIBILITY)
set_property(TARGET unity_common_without_sdk_types APPEND PROPERTY COMPILE_DEFINITIONS DISABLE_SDK_TYPES)

make_library(unity_global
  SOURCES
    lib/unity_global.cpp
    lib/unity_global_singleton.cpp
    lib/get_toolkit_function_from_closure.cpp
    lib/toolkit_class_registry.cpp
    lib/toolkit_function_registry.cpp
  REQUIRES
    flexible_type pylambda tcmalloc unity_sgraph image_util unity_common table_printer
  EXTERNAL_VISIBILITY
)


make_library(unity_model
  SOURCES
    lib/simple_model.cpp
    lib/variant_deep_serialize.cpp
  REQUIRES
    flexible_type unity_global unity_common
  EXTERNAL_VISIBILITY
)

make_library(query_process
  SOURCES
    query_process/query_processor.cpp
    query_process/lazy_sarray.cpp
    query_process/lazy_sframe.cpp
    query_process/lazy_groupby_aggregate.cpp
    query_process/sort.cpp
  REQUIRES
    flexible_type sframe pylambda cancel_serverside_ops fileio
)

make_library(unity_sframe
  SOURCES
    lib/unity_sarray_binary_operations.cpp
    lib/unity_sarray.cpp
    lib/unity_sframe.cpp
    lib/flex_dict_view.cpp
  REQUIRES
    flexible_type sframe pylambda cancel_serverside_ops query_process unity_global table_printer
  EXTERNAL_VISIBILITY 
)

make_library(unity_sgraph
  SOURCES
    lib/unity_sgraph.cpp
  REQUIRES
    flexible_type json lazy_eval sframe sgraph unity_global unity_sframe
  EXTERNAL_VISIBILITY 
)

make_library(unity_sketch
  SOURCES
    lib/unity_sketch.cpp
  REQUIRES
    flexible_type unity_sframe
)

make_library(unity_sdk
  SOURCES
    lib/gl_datatype_registration.cpp
    lib/toolkit_class_base.cpp
    lib/gl_sgraph.cpp
    lib/gl_gframe.cpp
    lib/gl_sarray.cpp
    lib/gl_sframe.cpp
  REQUIRES
    unity_sframe
  EXTERNAL_VISIBILITY 
)

make_library(unity_odbc_connection
  SOURCES
    lib/unity_odbc_connection.cpp
  REQUIRES
    flexible_type unity_sframe
)

make_empty_library(unity REQUIRES
  unity_common unity_global unity_model unity_sketch unity_sgraph unity_sframe unity_sdk unity_odbc_connection)


make_empty_library(unity_toolkits
  REQUIRES
    unity_graph_analytics
    )

