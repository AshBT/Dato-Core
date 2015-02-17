/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <string>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <globals/globals.hpp>
#include <globals/global_constants.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/s3_api.hpp>
#include <cppipc/cppipc.hpp>
#include <cppipc/common/authentication_token_method.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sketch.hpp>
#include <unity/lib/version.hpp>
#include <unity/lib/simple_model.hpp>
#include <parallel/pthread_tools.hpp>
#include <logger/logger.hpp>
#include <logger/log_rotate.hpp>

#include <lambda/pylambda_master.hpp>
#include <lambda/graph_pylambda_master.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

#include <unity/lib/gl_sarray.hpp>

#include <product_key/product_key.hpp>


#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif

#include <util/crash_handler.hpp>
#include <signal.h>

#include "unity_server_init.hpp"

void exported_symbols();

namespace po = boost::program_options;

#ifdef HAS_TCMALLOC
/**
 *  If TCMalloc is available, we try to release memory back to the
 *  system every 15 seconds or so. TCMalloc is otherwise somewhat...
 *  aggressive about keeping memory around.
 */
static bool stop_memory_release_thread = false;
graphlab::mutex memory_release_lock;
graphlab::conditional memory_release_cond;


void memory_release_loop() {
  memory_release_lock.lock();
  while (!stop_memory_release_thread) {
    memory_release_cond.timedwait(memory_release_lock, 15);
    MallocExtension::instance()->ReleaseFreeMemory();
  }
  memory_release_lock.unlock();
}
#endif

std::string SERVER_LOG_FILE;
REGISTER_GLOBAL(std::string, SERVER_LOG_FILE, false);

// Global variables 
graphlab::toolkit_function_registry* g_toolkit_functions;
graphlab::toolkit_class_registry* g_toolkit_classes;
cppipc::comm_server* server;

void print_help(std::string program_name, po::options_description& desc) {
  std::cerr << "Unity Server version: " << UNITY_VERSION << "\n"
            << desc << "\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020\n"
            << "Example: " << program_name << " tcp://*:10020\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server --auth_token=auth_token_value\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server ipc:///tmp/unity_status auth_token_value\n";
}


/**
 * Attempts to increase the file handle limit. 
 * Returns true on success, false on failure.
 */
bool upgrade_file_handle_limit(size_t limit) {
  struct rlimit rlim;
  rlim.rlim_cur = limit;
  rlim.rlim_max = limit;
  return setrlimit(RLIMIT_NOFILE, &rlim) == 0;
}

/**
 * Gets the current file handle limit.
 * Returns the current file handle limit on success, 
 * -1 on infinity, and 0 on failure.
 */
rlim_t get_file_handle_limit() {
  struct rlimit rlim;
  int ret = getrlimit(RLIMIT_NOFILE, &rlim);
  if (ret != 0) return 0;
  return rlim.rlim_cur;
}

void init_sdk() {
  /*
   * g_toolkit_classes->register_toolkit_class(graphlab::sdk_sarray::get_toolkit_class_registration());
   * g_toolkit_functions->register_toolkit_function(graphlab::sdk_sarray::get_toolkit_function_registration(), "gl_sarray_");
   */
}

/**
 * Tries to automatically set the LUA_PATH environment variable.
 *
 * Where <GRAPHLAB_PATH> is the directory containing
 *  the directory graphlab/ with the python sources, the environment
 *  variable LUA_PATH should be set to:
 *  <GRAPHLAB_PATH>/graphlab/lua/?/init.lua;<GRAPHLAB_PATH>/graphlab/lua/?.lua
 *
 *  i.e. if you have export PYTHONPATH=<GRAPHLAB_PATH>
 *  export LUA_PATH="$PYTHONPATH/graphlab/lua/?/init.lua;$PYTHONPATH/graphlab/lua/?.lua"
 *
 */
void set_lua_path() {
  const char* pythonpath = getenv("PYTHONPATH");
  std::string lua_root;
  std::vector<std::string> lua_path;
  std::set<boost::filesystem::path> possible_lua_paths;
  if (pythonpath) {
    std::vector<std::string> splitted_python_paths;
    boost::algorithm::split(splitted_python_paths, 
                            pythonpath, 
                            boost::algorithm::is_any_of(":"));
    for(auto& pypath: splitted_python_paths) {
      boost::filesystem::path normalized_path(pypath);
      normalized_path = boost::filesystem::absolute(normalized_path);
      possible_lua_paths.insert(normalized_path);
    }
  }
  possible_lua_paths.insert(graphlab::GLOBALS_MAIN_PROCESS_PATH);
  for (auto path: possible_lua_paths) {
    lua_path.push_back(std::string(path.native()) + "/graphlab/lua/?/init.lua");
    lua_path.push_back(std::string(path.native()) + "/graphlab/lua/?.lua");
  }
  std::string final_lua_path = boost::algorithm::join(lua_path, ";");
  setenv("LUA_PATH", final_lua_path.c_str(), true);
}

void configure_environment(std::string argv0) {
  graphlab::SFRAME_DEFAULT_NUM_SEGMENTS = graphlab::thread::cpu_count();
  graphlab::SFRAME_MAX_BLOCKS_IN_CACHE = 4 * graphlab::thread::cpu_count();
  graphlab::globals::initialize_globals_from_environment(argv0);
}
int main(int argc, char** argv) {

#ifndef NDEBUG
  global_logger().set_log_level(LOG_DEBUG);
#endif

  /// Installing crash handler to print stack trace in case of segfault.
  struct sigaction sigact;
  sigact.sa_sigaction = crit_err_hdlr;
  sigact.sa_flags = SA_RESTART | SA_SIGINFO;
  // the crit_err_hdlr writes to this file, by default stderr. 
  extern std::string BACKTRACE_FNAME; 
  BACKTRACE_FNAME = std::string("/tmp/unity_server_") 
                            + std::to_string(getpid())
                            + ".backtrace";
  if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
    fprintf(stderr, "error setting signal handler for %d (%s)\n",
        SIGSEGV, strsignal(SIGSEGV));
    exit(EXIT_FAILURE);
  }

  configure_environment(argv[0]);

  // file limit upgrade has to be the very first thing that happens. The 
  // reason is that on Mac, once a file descriptor has been used (even STDOUT),
  // the file handle limit increase will appear to work, but will in fact fail
  // silently.
  upgrade_file_handle_limit(4096);
  rlim_t file_handle_limit = get_file_handle_limit();
  if (file_handle_limit < 4096) {
    logstream(LOG_WARNING) 
        << "Unable to raise the file handle limit to 4096. "
        << "Current file handle limit = " << file_handle_limit << ". "
        << "You may be limited to frames with about " << file_handle_limit / 16 
        << " columns" << std::endl;
  }

  set_lua_path();

  std::string program_name = argv[0];
  std::string server_address;
  std::string control_address;
  std::string publish_address;
  std::string auth_token;
  std::string secret_key;
  std::string product_key;
  std::string log_file;
  bool daemon = false;
  bool check_product_key_only = false;
  size_t metric_server_port = 0;
  size_t log_rotation_interval = 0;
  size_t log_rotation_truncate = 0;
  boost::program_options::variables_map vm;

  // define all the command line options 
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Print this help message.")
      ("server_address", 
       po::value<std::string>(&server_address)->implicit_value(server_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens on")
      ("control_address", 
       po::value<std::string>(&control_address)->implicit_value(control_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens for control "
                         "messages on. OPTIONAL")
      ("publish_address", 
       po::value<std::string>(&publish_address)->implicit_value(publish_address),
       "This must be a valid ZeroMQ endpoint and "
                          "is the address on which the server "
                          "publishes status logs. OPTIONAL")
      ("metric_server_port", 
       po::value<size_t>(&metric_server_port)->default_value(metric_server_port),
       "This is the port number the Metrics Server listens on. It will accept "
       "connections to this port on all interfaces. If 0, will listen to a "
       "randomly assigned port. Defaults to 0. [[Deprecated]]")
      ("secret_key",
       po::value<std::string>(&secret_key),
       "Secret key used to secure the communication. Client must know the public "
       "key. Default is not to use secure communication.")
      ("auth_token", 
       po::value<std::string>(&auth_token)->implicit_value(auth_token),
       "This is an arbitrary string which is used to authenticate "
                     "the connection. OPTIONAL")
      ("daemon", po::value<bool>(&daemon)->default_value(false),
       "If set to true, will run the process in back-groundable daemon mode.")
      ("product_key", po::value<std::string>(&product_key)->default_value(""),
       "Required. The product registration key.")
      ("check_product_key_only", 
       "If set, this will validate the product_key argument, returning "
       "with an exit code of 0 on success, and an exit code of 1 on failure.")
      ("log_file",
       po::value<std::string>(&log_file),
       "The aggregated log output file. Logs will be printed to stderr as well as "
       "written to the log file ")
      ("log_rotation_interval", 
       po::value<size_t>(&log_rotation_interval)->implicit_value(60*60*24)->default_value(0),
       "The log rotation interval in seconds. If set, Log rotation will be performed. "
       "The default log rotation interval is 1 day (60*60*24 seconds). "
       "--log_file must be set for this to be meaningful. The log files will be named "
       "[log_file].0, [log_file].1, etc")
      ("log_rotation_truncate",
       po::value<size_t>(&log_rotation_truncate)->implicit_value(8)->default_value(0),
       "The maximum number of logs to keep around. If set log truncation will be performed. "
       "--log_file and --log_rotation_interval must be set for this to be meaningful.");

  po::positional_options_description positional;
  positional.add("server_address", 1);
  positional.add("control_address", 1);
  positional.add("publish_address", 1);
  positional.add("auth_token", 1);

  // try to parse the command line options
  try {
    po::command_line_parser parser(argc, argv);
    parser.options(desc);
    parser.positional(positional);
    po::parsed_options parsed = parser.run();
    po::store(parsed, vm);
    po::notify(vm);
  } catch(std::exception& error) {
    std::cout << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    print_help(program_name, desc);
    return 1;
  }

  if(vm.count("help")) {
    print_help(program_name, desc);
    return 0;
  }
  if(vm.count("check_product_key_only")) {
    check_product_key_only = true;
  }
 
  // check the product key if it is not an internal version
  bool is_internal = 
      boost::algorithm::ends_with(std::string(UNITY_VERSION), "internal");
  bool product_key_ok = ::product_key::check_product_key(product_key);
  if (check_product_key_only) {
    if (product_key_ok) exit(0);
    else exit(1);
  }
  if (!is_internal && !product_key_ok) {
    logstream(LOG_FATAL) << "Invalid Product Key" << std::endl;
    exit(0);
  }
  if (is_internal) {
    logstream(LOG_EMPH) << "Internal deployment version detected" << std::endl;
  }
  if (product_key_ok) {
    logstream(LOG_EMPH) << "Product Key check ok" << std::endl;
  }

  global_logger().set_log_level(LOG_INFO);

  if (!log_file.empty()) {
    if (log_rotation_interval) {
      graphlab::begin_log_rotation(log_file, log_rotation_interval, log_rotation_truncate);
    } else {
      global_logger().set_log_file(log_file);
    }
  }
  SERVER_LOG_FILE = log_file;

  graphlab::reap_unused_temp_files();

  logstream(LOG_EMPH) << "Unity server listening on: " <<  server_address<< std::endl;

  // Prevent multiple server listen on the same ipc device.
  namespace fs = boost::filesystem;
  if (boost::starts_with(server_address, "ipc://") && fs::exists(fs::path(server_address.substr(6)))) {
    logstream(LOG_FATAL) << "Cannot start unity server at " << server_address<< ". File already exists" << "\n";
    exit(-1);
  }

  // Use process_id to construct a default server address
  if (server_address == "default") {
    std::string path = "/tmp/graphlab_server-" + std::to_string(getpid());
    if (fs::exists(fs::path(path))) {
      // It could be a leftover of a previous crashed process, try to delete the file
      if (remove(path.c_str()) != 0) {
        logstream(LOG_FATAL) << "Cannot start unity server at " << server_address<< ". File already exists, and cannot be deleted." << "\n";
        exit(-1);
      }
    }
    server_address = "ipc://" + path;
  }

  // construct the server
  server = new cppipc::comm_server(std::vector<std::string>(), "", 
                                   server_address, control_address, publish_address, secret_key);

  // include the authentication method if the auth token is provided
  if (vm.count("auth_token")) {
    logstream(LOG_EMPH) << "Authentication Method: authentication_token Applied" << std::endl;
    server->add_auth_method(std::make_shared<cppipc::authentication_token_method>(auth_token));
  } else {
    logstream(LOG_EMPH) << "No Authentication Method." << std::endl;
  }

  g_toolkit_functions = init_toolkits();

  g_toolkit_classes = init_models();

  init_sdk();
  /**
   * Set the path to the pylambda_slave binary used for evaluate python lambdas parallel in separate processes.
   * Two possible places are relative path to the server binary in the source build,
   * and relative path to the server binary in the pip install build.
   */
  fs::path p = fs::path(program_name).parent_path() /= fs::path("../../lambda/pylambda_worker");
  fs::path p2 = fs::path(program_name).parent_path() /= fs::path("pylambda_worker");
  if (fs::exists(p)) {
    graphlab::lambda::pylambda_master::set_pylambda_worker_binary(p.string());
    graphlab::lambda::graph_pylambda_master::set_pylambda_worker_binary(p.string());
  } else if (fs::exists(p2)) {
    graphlab::lambda::pylambda_master::set_pylambda_worker_binary(p2.string());
    graphlab::lambda::graph_pylambda_master::set_pylambda_worker_binary(p2.string());
  } else {
    logstream(LOG_ERROR) << "Cannot find pylambda_worker binary. Lambda evaluation will fail." << std::endl;
  }


  server->register_type<graphlab::unity_sgraph_base>([](){ 
                                            return new graphlab::unity_sgraph();
                                          });

  server->register_type<graphlab::model_base>([](){ 
                                            return new graphlab::simple_model();
                                          });
  server->register_type<graphlab::unity_sframe_base>([](){ 
                                            return new graphlab::unity_sframe();
                                          });
  server->register_type<graphlab::unity_sarray_base>([](){ 
                                            return new graphlab::unity_sarray();
                                          });
  server->register_type<graphlab::unity_sketch_base>([](){ 
                                            return new graphlab::unity_sketch();
                                          });


  create_unity_global_singleton(g_toolkit_functions, g_toolkit_classes, server);

  server->register_type<graphlab::unity_global_base>(
      []()->std::shared_ptr<graphlab::unity_global_base> {
        auto unity_shared_ptr = graphlab::get_unity_global_singleton();
        return std::dynamic_pointer_cast<graphlab::unity_global_base>(unity_shared_ptr);
      });

  // graphlab::launch_metric_server(metric_server_port);

  server->start();

  // set the progress observer
  global_logger().add_observer(
      LOG_PROGRESS,
      [=](int lineloglevel, const char* buf, size_t len){
        server->report_status("PROGRESS", std::string(buf, len));
      });



#ifdef HAS_TCMALLOC
  graphlab::thread memory_release_thread;
  memory_release_thread.launch(memory_release_loop);
#endif
  // make a copy of the stdin file handle since annoyingly some imported
  // libraries via dlopen might close stdin. (I am looking at you...
  // scipy/optimize/minpack2.so as distributed by anaconda)
  auto stdin_clone_fd = dup(STDIN_FILENO);
  auto stdin_clone_file = fdopen(stdin_clone_fd, "r");
  if (daemon) {
    while(1) {
      sleep(1000000);
    }
  } else {
    // lldb when breaking and continuing may
    // interrupt the getchar syscall  making this return
    // a failure. (It will return -1 (EOF) and set EAGAIN).
    // However, of course EOF is a valid value for getchar() to return really.
    // So we have to double check with feof.
    while(1) {
      int c = fgetc(stdin_clone_file);
      // returned EOF, but not actually EOF. This is an interrupted syscall.
      // continue looping.
      if (c == -1 && !feof(stdin_clone_file)) continue;
      logstream(LOG_EMPH) << "Quiting with received character: "
                          << int(c)
                          << " feof = " << feof(stdin_clone_file) << std::endl;
      // quit in all other cases.
      break;
    }
  }

#ifdef HAS_TCMALLOC
  stop_memory_release_thread = true;
  memory_release_cond.signal();
  memory_release_thread.join();
#endif

  // detach the progress observer
  global_logger().add_observer(LOG_PROGRESS, NULL);
  //graphlab::stop_metric_server();
  delete server;
  delete g_toolkit_functions;

  graphlab::reap_unused_temp_files();
  graphlab::stop_log_rotation();
}
