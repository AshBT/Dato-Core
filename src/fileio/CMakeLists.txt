project(fileio)

make_library(fileio
  SOURCES
    oss_webstor/asyncurl.cpp
    oss_webstor/sysutils.cpp
    oss_webstor/wsconn.cpp
    s3_api.cpp
    run_aws.cpp
    hdfs.cpp
    libhdfs_shim.cpp
    union_fstream.cpp
    general_fstream_source.cpp
    general_fstream_sink.cpp
    general_fstream.cpp
    cache_stream_source.cpp
    cache_stream_sink.cpp
    fixed_size_cache_manager.cpp
    temp_files.cpp
    curl_downloader.cpp
    sanitize_url.cpp
    file_download_cache.cpp
    fs_utils.cpp
    file_handle_pool.cpp
    fileio_constants.cpp
  REQUIRES
    curl openssl xml2 logger dl pthread z cancel_serverside_ops globals
  MAC_REQUIRES
    iconv
  EXTERNAL_VISIBILITY
  )

add_dependencies(fileio hadoop)

make_executable(wscmd SOURCES oss_webstor/wscmd.cpp REQUIRES fileio)
make_executable(wsdbg SOURCES oss_webstor/wsdbg.cpp REQUIRES fileio)
make_executable(wsperf SOURCES oss_webstor/wsperf.cpp REQUIRES fileio)
