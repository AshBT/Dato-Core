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
#include <vector>
#include <string>
#include <utility>
#include <cassert>
#include <unistd.h>
#include <iomanip>
#include <rpc/dc_registry.hpp>
#include <logger/logger.hpp>
namespace graphlab {
namespace dc_impl {

static std::vector<std::string>& get_dc_registry() {
  static std::vector<std::string> dc_registry;
  return dc_registry;
}


function_dispatch_id_type add_to_function_registry(const void* c, size_t len) {
  std::string f((const char*)(c), len);
  std::vector<std::string>& dc_registry = get_dc_registry();
  dc_registry.push_back(f);
  const size_t* s = reinterpret_cast<const size_t*>(c);
  if (len == 8) {
    logstream(LOG_EMPH) << " Registering Function: " << dc_registry.size() - 1 
                      << " at " << std::hex << (*s) << std::dec << std::endl;
  } else if (len == 16) {
    logstream(LOG_EMPH) << " Registering Function: " << dc_registry.size() - 1 
                        << " at " << std::hex << s[0] << s[1] << std::dec << std::endl;
  } else {
    logstream(LOG_EMPH) << " Registering Function: " << dc_registry.size() - 1 
                        << " at ... " << std::endl;
  }
  return dc_registry.size() - 1;
}

std::pair<const void*, size_t> get_from_function_registry_impl(function_dispatch_id_type id) {
  std::vector<std::string>& dc_registry = get_dc_registry();
  if (id >= dc_registry.size()) {
    logstream(LOG_FATAL)<< " Nonexistant function ID " << id 
                         << " (registry table size: " << dc_registry.size() << ")" << std::endl;
  }

  assert(id < dc_registry.size());
  std::pair<const void*, size_t> ret;
  ret.first = reinterpret_cast<const void*>(dc_registry[id].data());
  ret.second = dc_registry[id].length();
  return ret;
}

}
}
