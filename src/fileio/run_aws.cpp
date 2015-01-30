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
#include <fileio/run_aws.hpp>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <mutex>
#include <fileio/temp_files.hpp>
#include <logger/logger.hpp>
#include <unistd.h>
#include <cppipc/cppipc.hpp>

namespace graphlab {
  namespace fileio {

static std::mutex env_lock;

void wait_on_child_and_print_progress (int fd, pid_t child_pid) {
  const size_t BUF_SIZE = 4096;
  char buf[BUF_SIZE];
  ssize_t bytes_read;
  while( (bytes_read = read(fd, buf, BUF_SIZE)) != 0 ) {
    logprogress_stream << std::string(buf, buf + bytes_read);
    if (cppipc::must_cancel()) {
      logprogress_stream << "Cancel by user" << std::endl;
      kill(child_pid, SIGKILL);
      break;
    }
  }
  logprogress_stream << std::endl;

  waitpid(child_pid, NULL, 0);
  close(fd);
}

std::string get_child_error_or_empty(const std::string& file) {
  std::ifstream fin(file);
  return std::string((std::istreambuf_iterator<char>(fin)),
                      std::istreambuf_iterator<char>());
}

/**
 * Same logic as popen(cmd, "r") but return child pid and file descriptor for the stdout..
 */
int _popen(const std::string& cmd, const std::vector<std::string>& arglist, pid_t* out_pid) {
  // build argv
  const char** c_arglist = new const char*[arglist.size() + 1];
  size_t i = 0;
  for (const auto& arg: arglist) {
    c_arglist[i++] = arg.c_str();
  }
  c_arglist[i] = NULL;

  // build pipe
  int fd[2];
  int& read_fd = fd[0];
  int& write_fd = fd[1];
  if (pipe(fd)) {
    log_and_throw("pipe error");
  };

  // fork
  pid_t pid = fork();
  if (pid < 0) {
    delete[] c_arglist;
    log_and_throw("fork error");
  }

  // exec 
  if (pid == 0) {
    // child proc: set stdout to pipe
    close(read_fd);
    dup2(write_fd, STDOUT_FILENO);
    close(write_fd);
    execvp(cmd.c_str(), (char**)c_arglist);
    _exit(0);
  } else {
    // parent proc
    close(write_fd);
    if (out_pid != NULL) {
      *out_pid = pid;
    }
    delete[] c_arglist;
    return read_fd;
  }
}

std::string run_aws_command(const std::vector<std::string>& arglist,
                            const std::string& aws_access_key_id,
                            const std::string& aws_secret_access_key) {
  {
    std::lock_guard<std::mutex> lock_guard(env_lock);
    setenv("AWS_ACCESS_KEY_ID", aws_access_key_id.c_str(), 1 /*overwrite*/);
    setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key.c_str(), 1 /*overwrite*/);
  }

  // Creates a temp file and redirect the child process's stderr
  std::string child_err_file = get_temp_name();

  std::string cmd = "/bin/sh";
  std::vector<std::string> argv;

  argv.push_back("sh");
  argv.push_back("-c");

  std::stringstream command_builder;

  // We put cd here because aws command prints url relative to the working
  // directory. Without cd, it will print out stuff like
  // download s3://foo to ../../../../../../../../../var/tmp/graphlab/0001/foo
  // with cd it will have less ".."'s. This is still not pretty.
  command_builder << "cd && aws ";
  for (const auto& x: arglist)
    command_builder << x << " ";

  command_builder << "2>" << child_err_file;
  argv.push_back(command_builder.str());

  logstream(LOG_INFO) << "Running aws command: " << command_builder.str() << std::endl;

  pid_t child_pid;
  int child_out_fd = _popen(cmd, argv, &child_pid);
  wait_on_child_and_print_progress(child_out_fd, child_pid);

  std::string ret = get_child_error_or_empty(child_err_file);
  delete_temp_file(child_err_file);
  return ret;
}


}
}
