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
#ifndef GRAPHLAB_RPC2_DC_REGISTRY_HPP
#define GRAPHLAB_RPC2_DC_REGISTRY_HPP
#include <utility>
#include <cstddef>
#include <stdint.h>
namespace graphlab {
typedef uint32_t function_dispatch_id_type;
namespace dc_impl {


function_dispatch_id_type add_to_function_registry(const void* c, size_t len);

std::pair<const void*, size_t> get_from_function_registry_impl(function_dispatch_id_type id);

template <typename F>
inline F get_from_function_registry(function_dispatch_id_type id) {
  std::pair<const void*, size_t> val = get_from_function_registry_impl(id);
  return *reinterpret_cast<F*>(const_cast<void*>(val.first));
}

}
}
#endif
