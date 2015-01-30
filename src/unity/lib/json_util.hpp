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
#ifndef GRAPHLAB_UNITY_JSON_UTIL_HPP
#define GRAPHLAB_UNITY_JSON_UTIL_HPP
#include <unity/lib/json_include.hpp>
#include <unity/lib/api/client_base_types.hpp>
#include <cmath>
namespace graphlab {
  /**
   * Helper utility for converting from flexible_type to json.
   * TODO: Fill in details 
   */
  inline JSONNode flexible_type_to_json(const flexible_type& val, std::string name) {

    if (val.get_type() == flex_type_enum::INTEGER) {
      // long cast needed to avoid a ambiguity error which seems to only show up
      // on mac clang++
      if (std::isnan(val.get<flex_int>())) {
        // treat nan as missing value null
        JSONNode v(JSON_NULL);
        v.set_name(name);
        v.nullify();
        return v;
      } else {
        return JSONNode(name, (long)val.get<flex_int>());
      }
    } else if (val.get_type() == flex_type_enum::FLOAT) {
      if (std::isnan(val.get<double>())) {
        // treat nan as missing value null
        JSONNode v(JSON_NULL);
        v.set_name(name);
        v.nullify();
        return v;
      } else {
        return JSONNode(name, val.get<double>());
      }
    } else if (val.get_type() == flex_type_enum::STRING) {
      return JSONNode(name, val.get<flex_string>());
    } else if (val.get_type() == flex_type_enum::VECTOR) {
      const std::vector<double>& v = val.get<flex_vec>();
      JSONNode vec(JSON_ARRAY);
      for (size_t i = 0;i < v.size(); ++i) {
        JSONNode vecval(JSON_NUMBER);
        vecval= v[i];
        vec.push_back(vecval);
      }
      vec.set_name(name);
      return vec;
    } else if (val.get_type() == flex_type_enum::DICT){
      return JSONNode(name, val.get<flex_string>());
    } else if (val.get_type() == flex_type_enum::UNDEFINED){
      JSONNode v(JSON_NULL);
      v.set_name(name);
      v.nullify();
      return v;
    }

    JSONNode v(JSON_NULL);
    v.set_name(name);
    v.nullify();
    return v;
  }

}
#endif
