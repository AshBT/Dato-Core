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
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/unity_global.hpp>

namespace graphlab {

std::shared_ptr<unity_global> unity_global_ptr;

void create_unity_global_singleton(toolkit_function_registry* _toolkit_functions,
                                   toolkit_class_registry* _classes,
                                   cppipc::comm_server* server) {
    unity_global_ptr = std::make_shared<unity_global>(_toolkit_functions, _classes, server);
}

std::shared_ptr<unity_global> get_unity_global_singleton() {
  if (unity_global_ptr == nullptr) {
    ASSERT_MSG(false, "Unity Global has not been created");
  }
  return unity_global_ptr;
}

} // namespace graphlab
