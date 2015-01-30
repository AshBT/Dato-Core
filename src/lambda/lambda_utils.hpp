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
#ifndef GRAPHLAB_LAMBDA_LAMBDA_UTILS_HPP
#define GRAPHLAB_LAMBDA_LAMBDA_UTILS_HPP

#include<cppipc/common/message_types.hpp>

namespace graphlab {
namespace lambda {

/**
 * Helper function to convert an communication failure exception to a user
 * friendly message indicating a failure lambda execution.
 */
inline cppipc::ipcexception reinterpret_comm_failure(cppipc::ipcexception e) {
  const char* message = "Fail executing the lambda function. The lambda worker may have run out of memory or crashed because it captured objects that cannot be properly serialized.";
  if (e.get_reply_status() == cppipc::reply_status::COMM_FAILURE) {
    return cppipc::ipcexception(cppipc::reply_status::EXCEPTION,
                                e.get_zeromq_errorcode(),
                                message);
  } else {
    return e;
  }
}

}
}

#endif
