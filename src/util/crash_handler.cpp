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
#include <unistd.h>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <fcntl.h>
#include <string>
#include <signal.h>

// The filename which we write backtrace to,
// default empty and we write to STDERR_FILENO
std::string BACKTRACE_FNAME = "";

/**
 * Dump back trace to file, refer to segfault.c in glibc.
 * https://sourceware.org/git/?p=glibc.git;a=blob;f=debug/segfault.c;
 */
void crit_err_hdlr(int sig_num, siginfo_t * info, void * ucontext) {
  void** array = (void**)alloca (256 * sizeof (void *));
  int size = backtrace(array, 256);

  int fd = STDERR_FILENO;
  if (!BACKTRACE_FNAME.empty()) {
    const char* fname = BACKTRACE_FNAME.c_str();
    fd = open (fname, O_TRUNC | O_WRONLY | O_CREAT, 0666);
    if (fd == -1)
      fd = STDERR_FILENO;
  }
  backtrace_symbols_fd(array, size, fd);
  close(fd);
  exit(EXIT_FAILURE);
}
