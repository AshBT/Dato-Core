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
#include <unity/lib/variant_converter.hpp>

/*
 * This files include a bunch of symbols that may not be linked into unity_server
 * but really has to be there for toolkit sdk imports to wor.
 */
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#else
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#endif
namespace graphlab {
void exported_symbols() {
  { volatile auto x = graphlab::variant_converter_impl::get_toolkit_function_from_closure; }
  { volatile auto x = variant_converter<std::shared_ptr<unity_sarray>, void>(); }
  { volatile auto x = variant_converter<std::shared_ptr<unity_sframe>, void>(); }
  { volatile auto x = variant_converter<std::shared_ptr<unity_sgraph>, void>(); }
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sarray>, void>::get; } 
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sarray>, void>::set; } 
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sframe>, void>::get; } 
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sframe>, void>::set; } 
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sgraph>, void>::get; } 
  { volatile auto x = &variant_converter<std::shared_ptr<unity_sgraph>, void>::set; } 
}



}

#ifdef __clang__
#pragma clang diagnostic pop
#else
#pragma GCC diagnostic pop
#endif
