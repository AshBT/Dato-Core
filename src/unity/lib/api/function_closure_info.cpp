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
#include <unity/lib/api/function_closure_info.hpp>

namespace graphlab {


void function_closure_info::save(oarchive& oarc) const {
  oarc << native_fn_name;
  oarc << arguments.size();
  for (size_t i = 0;i < arguments.size(); ++i) {
    oarc << arguments[i].first << *(arguments[i].second);
  }
}
void function_closure_info::load(iarchive& iarc) {
  arguments.clear();
  size_t nargs = 0;
  iarc >> native_fn_name >> nargs;
  arguments.resize(nargs);
  for (size_t i = 0;i < arguments.size(); ++i) {
    iarc >> arguments[i].first;
    arguments[i].second.reset(new variant_type);
    iarc >> *(arguments[i].second);
  }
}
}
