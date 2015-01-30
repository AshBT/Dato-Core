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
#ifndef GRAPHLAB_UNITY_IS_SITERABLE_HPP
#define GRAPHLAB_UNITY_IS_SITERABLE_HPP

#include <iterator>
#include <type_traits>
#include <sframe/siterable.hpp>
namespace graphlab {
namespace sframe_impl {


/**
 * is_siterable<T>::value is true if T inherits from siterable
 */
template <typename T, 
          typename DecayedT = typename std::decay<T>::type,
          typename Iterator = typename DecayedT::iterator> 
struct is_siterable {
  static constexpr bool value = 
      std::is_base_of<graphlab::siterable<Iterator>, DecayedT>::value;
};


template <typename T>
struct is_siterable<T,void,void> {
  static constexpr bool value = false;
};



} // sframe_impl
} // graphlab

#endif
