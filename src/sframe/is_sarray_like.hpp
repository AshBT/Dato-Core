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
#ifndef GRAPHLAB_UNITY_SFRAME_IS_SARRAY_LIKE_HPP
#define GRAPHLAB_UNITY_SFRAME_IS_SARRAY_LIKE_HPP
#include <memory>
#include <sframe/is_siterable.hpp>
#include <sframe/is_swriter_base.hpp>
namespace graphlab {
namespace sframe_impl {

template <typename T>
struct has_get_reader_function {
  template <typename U, std::unique_ptr<typename U::reader_type> (U::*)(size_t) const> struct SFINAE {};
  template<typename U> static char Test(SFINAE<U, &U::get_reader>*);
  template<typename U> static int Test(...);
  static constexpr bool value = sizeof(Test<T>(0)) == sizeof(char);
};


/**
 * is_sarray_like<T>::value is true if T inherits from swriter_base and 
 * has a get_reader() function implemented which returns an
 * std::unique_ptr<T::reader_type>
 */
template <typename T, 
          typename DecayedT = typename std::decay<T>::type,
          typename Iterator = typename DecayedT::iterator> 
struct is_sarray_like {
  static constexpr bool value = is_swriter_base<T>::value && 
      has_get_reader_function<DecayedT>::value;
};



template <typename T>
struct is_sarray_like<T,void,void> {
  static constexpr bool value = false;
};


} // sframe_impl
} // graphlab
#endif
