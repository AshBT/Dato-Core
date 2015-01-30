project(sframe)

 make_library(sframe
   SOURCES
     dataframe.cpp
     sarray_index_file.cpp
     sframe_constants.cpp
     sframe_config.cpp
     sframe.cpp
     sframe_reader.cpp
     sframe_index_file.cpp
     sframe_from_flex_type_record_inserter.cpp
     parallel_csv_parser.cpp
     sframe_io.cpp
     shuffle.cpp
     csv_line_tokenizer.cpp
     sarray_v1_block_manager.cpp
     sarray_v2_block_manager.cpp
     sarray_v2_type_encoding.cpp
     sarray_v2_block_writer.cpp
     sarray_sorted_buffer.cpp
     sarray_v2_encoded_block.cpp
     integer_pack.cpp
     groupby.cpp
     groupby_aggregate.cpp
     groupby_aggregate_impl.cpp
     group_aggregate_value.cpp
     sframe_iterators.cpp
     csv_writer.cpp
     join.cpp
     join_impl.cpp
     unfair_lock.cpp
     sframe_rows.cpp
     generic_avro_reader.cpp
     odbc_connector.cpp
     libodbc_shim.cpp
   REQUIRES
     random flexible_type fileio parallel fiber lz4 
     cancel_serverside_ops serialization json globals avrocpp odbc
 )

 make_executable(rddtosf_nonpickle
   SOURCES
     rddtosframe-nonpickle.cpp
   REQUIRES
     sframe
 )
  
 make_executable(rddtosf_pickle
                SOURCES
                  rddtosframe-pickle.cpp
                REQUIRES
                  logger
                  python
                  cppipc
                  sframe
                  pylambda
 )

 make_executable(sftordd_pickle
                SOURCES
                  sframetordd-pickle.cpp
                REQUIRES
                  logger
                  python
                  cppipc
                  sframe
                  pylambda
 )

 file(DOWNLOAD http://s3-us-west-2.amazonaws.com/glbin-engine/graphlab-create-spark-integration_2.10-1.0.jar ./src/sframe/graphlab-create-spark-integration.jar
      EXPECTED_MD5 5c4eebfd0426e4f5189c7e576639bc6a)

