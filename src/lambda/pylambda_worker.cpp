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
#include <boost/program_options.hpp>
#include <cppipc/server/comm_server.hpp>
#include <lambda/pylambda.hpp>
#include <lambda/python_api.hpp>
#include <lambda/graph_pylambda.hpp>

#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif

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

void print_help(std::string program_name, po::options_description& desc) {
  std::cerr << "Pylambda Server\n"
            << desc << "\n"
            << "Example: " << program_name << " ipc:///tmp/pylambda_worker\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020\n"
            << "Example: " << program_name << " tcp://*:10020\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server --auth_token=secretkey\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server ipc:///tmp/unity_status secretkey\n";
}

int main(int argc, char** argv) {
  size_t parent_pid = getppid();
  // Options
  std::string program_name = argv[0];
  std::string server_address;

  // Handle server commandline options
  boost::program_options::variables_map vm;
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Print this help message.")
      ("server_address", 
       po::value<std::string>(&server_address)->implicit_value(server_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens on");

  po::positional_options_description positional;
  positional.add("server_address", 1);

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

  try {
    graphlab::lambda::init_python(argc, argv);
  } catch (const std::string& error) {
    logstream(LOG_WARNING) << "Fail initializing python: " << error << std::endl;
    exit(-1);
  }

  // construct the server
  cppipc::comm_server server(std::vector<std::string>(), "", server_address);

  server.register_type<graphlab::lambda::lambda_evaluator_interface>([](){
                                            return new graphlab::lambda::pylambda_evaluator();
                                          });
  server.register_type<graphlab::lambda::graph_lambda_evaluator_interface>([](){
                                            return new graphlab::lambda::graph_pylambda_evaluator();
                                          });

  server.start();

#ifdef HAS_TCMALLOC
  graphlab::thread memory_release_thread;
  memory_release_thread.launch(memory_release_loop);
#endif

  while(1) {
    sleep(5);
    // has parent_pid and parent_pid 
    if (parent_pid != 0 && kill(parent_pid, 0) == -1) {
      break;
    }

  }

#ifdef HAS_TCMALLOC
  stop_memory_release_thread = true;
  memory_release_cond.signal();
  memory_release_thread.join();
#endif

}
