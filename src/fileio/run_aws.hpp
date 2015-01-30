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
#ifndef GRAPHLAB_FILEIO_RUN_AWS_HPP
#define GRAPHLAB_FILEIO_RUN_AWS_HPP

#include <string>
#include <vector>

namespace graphlab {
  namespace fileio {
/**
 * Helper function to launch an external aws-cli command.
 * Assuming aws-cli is installed and is generally accessible.
 *
 * The current thread will fork and execute the aws-cli,
 * block until the child process completes.
 *
 * Intermediate output to stdout is piped to logprogress_stream.
 * Any message written by child process to stderr is captured
 * and return to the caller.
 *
 * In the success case, this function should return an empty string.
 */
std::string run_aws_command(const std::vector<std::string>& arglist,
                            const std::string& aws_access_key_id,
                            const std::string& aws_secret_access_key);

} // end of fileio
} // end of graphlab 


#endif
