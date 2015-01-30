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
#include <unity/lib/variant.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/unity_global.hpp>

namespace graphlab {
namespace variant_converter_impl {
std::function<variant_type(const std::vector<variant_type>&)> 
    get_toolkit_function_from_closure(const function_closure_info& closure) {
  auto native_execute_function = get_unity_global_singleton()
      ->get_toolkit_function_registry()
      ->get_native_function(closure);

  return native_execute_function;
}
} // variant_converter_impl
} // ngraphlab
