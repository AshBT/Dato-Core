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
#include <util/cityhash_gl.hpp>
#include <flexible_type/flexible_type.hpp> 
#include <vector>

namespace graphlab {

uint128_t hash128(const flexible_type& v) {
  return v.hash128(); 
}

uint64_t hash64(const flexible_type& v) {
  return v.hash(); 
}

uint128_t hash128(const std::vector<flexible_type>& v) {
  uint128_t h = hash128(v.size());

  for(const flexible_type& x : v)
    h = hash128_combine(h, x.hash128());

  return h;
}

uint64_t hash64(const std::vector<flexible_type>& v) {
  return hash64(hash128(v));
}


}; 
