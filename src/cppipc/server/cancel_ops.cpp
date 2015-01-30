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
#include "cppipc/server/cancel_ops.hpp"

namespace cppipc {

/** A way of making srv_running_command global
  * The values of srv_running_command are meant to have a range of an unsigned
  * 64-bit integer.  Two values have special meaning:
  *   - A value of 0 means that there is no command currently running.
  *   - A value of uint64_t(-1) means that the currently running command should
  *   be cancelled.
  *
  * NOTE: This design relies on the current fact that the cppipc server will only
  * run one command at a time.  This must be revisited if more than 1 command could
  * possibly be running.
  */
std::atomic<unsigned long long>& get_srv_running_command() {
  static std::atomic<unsigned long long> srv_running_command;
  return srv_running_command;
}

std::atomic<bool>& get_cancel_bit_checked() {
  static std::atomic<bool> cancel_bit_checked;
  return cancel_bit_checked;
}

bool must_cancel() {
  unsigned long long max_64bit = (unsigned long long)uint64_t(-1);
  std::atomic<unsigned long long> &command = get_srv_running_command();
  get_cancel_bit_checked().store(true);

  // In theory, this is not an atomic operation. However, this command variable
  // is only written at times strictly before and after must_cancel can be
  // called.  Command gets reset to 0 once the command has exited.
  return !(command.load() != max_64bit);
}

} // cppipc
