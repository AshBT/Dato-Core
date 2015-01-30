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
#include <unity/lib/unity_global.hpp>
#include <unity/lib/variant_converter.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sframe.hpp>

#ifndef DISABLE_SDK_TYPES
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sgraph.hpp>
#include <unity/lib/gl_gframe.hpp>
#include <globals/globals.hpp>
#endif

#include <unity/lib/api/function_closure_info.hpp>

namespace graphlab {

#ifndef DISABLE_SDK_TYPES
int64_t USE_GL_DATATYPE = 0;
REGISTER_GLOBAL(int64_t, USE_GL_DATATYPE, true);
#endif

std::shared_ptr<unity_sarray> variant_converter<std::shared_ptr<unity_sarray>, void>::get(const variant_type& val) {
  return std::static_pointer_cast<unity_sarray>(variant_get_ref<std::shared_ptr<unity_sarray_base>>(val));
}

variant_type variant_converter<std::shared_ptr<unity_sarray>, void>::set(std::shared_ptr<unity_sarray> val) {
  return variant_type(std::static_pointer_cast<unity_sarray_base>(val));
}


std::shared_ptr<unity_sframe> variant_converter<std::shared_ptr<unity_sframe>, void>::get(const variant_type& val) {
  return std::static_pointer_cast<unity_sframe>(variant_get_ref<std::shared_ptr<unity_sframe_base>>(val));
}

variant_type variant_converter<std::shared_ptr<unity_sframe>, void>::set(std::shared_ptr<unity_sframe> val) {
  return variant_type(std::static_pointer_cast<unity_sframe_base>(val));
}

std::shared_ptr<unity_sgraph> variant_converter<std::shared_ptr<unity_sgraph>, void>::get(const variant_type& val) {
  return std::static_pointer_cast<unity_sgraph>(variant_get_ref<std::shared_ptr<unity_sgraph_base>>(val));
}

variant_type variant_converter<std::shared_ptr<unity_sgraph>, void>::set(std::shared_ptr<unity_sgraph> val) {
  return variant_type(std::static_pointer_cast<unity_sgraph_base>(val));
}



#ifndef DISABLE_SDK_TYPES
gl_sarray variant_converter<gl_sarray, void>::get(const variant_type& val) {
  return variant_get_ref<std::shared_ptr<unity_sarray_base>>(val);
}
variant_type variant_converter<gl_sarray, void>::set(gl_sarray val) {
  if (USE_GL_DATATYPE) {
    return variant_type(std::dynamic_pointer_cast<model_base>(std::make_shared<gl_sarray>(val)));
  } else {
    return variant_type(std::shared_ptr<unity_sarray_base>(val));
  }
}

gl_sframe variant_converter<gl_sframe, void>::get(const variant_type& val) {
  return variant_get_ref<std::shared_ptr<unity_sframe_base>>(val);
}
variant_type variant_converter<gl_sframe, void>::set(gl_sframe val) {
  return variant_type(std::shared_ptr<unity_sframe_base>(val));
};


gl_sgraph variant_converter<gl_sgraph, void>::get(const variant_type& val) {
  return variant_get_ref<std::shared_ptr<unity_sgraph_base>>(val);
}
variant_type variant_converter<gl_sgraph, void>::set(gl_sgraph val) {
  return variant_type(std::shared_ptr<unity_sgraph_base>(val));
};


gl_gframe variant_converter<gl_gframe, void>::get(const variant_type& val) {
  throw std::string("Cannot read a gl_gframe from a variant. Try an gl_sframe");
}

variant_type variant_converter<gl_gframe, void>::set(gl_gframe val) {
  return variant_type(std::shared_ptr<unity_sframe_base>(val));
};

#endif

} // graphlab
