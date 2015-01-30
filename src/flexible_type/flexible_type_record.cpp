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
/* 
 * Copyright (c) 2013 GraphLab Inc. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      https://graphlab.com
 *
 */

#include <typeinfo>
#include <iostream>
#include <utility>
#include <flexible_type/flexible_type_record.hpp>

namespace graphlab {

flexible_type_record::flexible_type_record() {
  set_empty();
}
flexible_type_record::~flexible_type_record() {
  clear();
}

flexible_type_record::flexible_type_record(const flexible_type_record& other) {
  set_empty();
  (*this) = other;
}

flexible_type_record& flexible_type_record::operator=(const flexible_type_record& other) {
  if (this == &other) return *this;
  std::pair<size_t, const flexible_type*> otherptr = other.get_ptr();
  resize_and_clear(otherptr.first);
  std::pair<size_t, flexible_type*> myptr = get_ptr();
  for (size_t i = 0;i < otherptr.first; ++i) {
    myptr.second[i] = otherptr.second[i];
    myptr.second[i].tag() = otherptr.second[i].tag();
  }
  return *this;
}
field_id_type flexible_type_record::add_field(field_id_type field_id, flexible_type data) {
  // confirm that this field_id doesn't already exist in the vector
  std::pair<size_t, flexible_type*> ptr = get_ptr();
  for (size_t i = 0;i < ptr.first; ++i) {
    if (ptr.second[i].tag() == field_id) {
      // update existing value
      ptr.second[i] = data;
      // refresh the tag value
      ptr.second[i].tag() = field_id;
      return (field_id_type)(-1);
    }
  }
  // no it does not exist. create a new element
  flexible_type* newel = add_one_element();
  (*newel) = data;
  newel->tag() = field_id;
  return field_id;
}

void flexible_type_record::remove_field(field_id_type field_id) {
  // first find the element
  bool found = false;
  size_t foundat = 0;
  std::pair<size_t, flexible_type*> ptr = get_ptr();
  for (size_t i = 0;i < ptr.first; ++i) {
    if (ptr.second[i].tag() == field_id) {
      foundat = i;
      found = true;
      break;
    }
  }
  if (!found) return;

  if (ptr.first == 1) {
    // only 1 element, clear the array than
    clear();
  } else if (ptr.first == 2) {
    // we need to downgrade to 1 element
    // read out the other element
    flexible_type otherval = std::move(ptr.second[!foundat]);
    // preserve the tag
    otherval.tag() = ptr.second[!foundat].tag();
    resize_and_clear(1);
    //move into the new array
    ptr = get_ptr();
    // remember to reserve the tag.
    ptr.second[0] = std::move(otherval);
    ptr.second[0].tag() = otherval.tag();
  } else {
    // we have a full array.  
    // move the last element to it so we can just delete the last element
    if (ptr.first - 1 != foundat) {
      // if we are not the last element in the array
      // remember to preserve the tag
      ptr.second[foundat] = std::move(ptr.second[ptr.first - 1]);
      ptr.second[foundat].tag() = ptr.second[ptr.first - 1].tag();
    }
    // delete the last element
    ptr.second[ptr.first - 1].~flexible_type();
    // decrease the size of the array
    values.ptr.first--;
    values.ptr.second = (flexible_type*)realloc(values.ptr.second, 
                                sizeof(flexible_type) * values.ptr.first);
    
  }
}


void flexible_type_record::save(graphlab::oarchive& oarc) const {
  std::pair<size_t, const flexible_type*> ptr = get_ptr();
  oarc << ptr.first;
  for (size_t i = 0;i < ptr.first; ++i) {
    oarc << ptr.second[i] << ptr.second[i].tag();
  }
}

void flexible_type_record::load(graphlab::iarchive& iarc) {
  size_t numel;
  iarc >> numel;
  resize_and_clear(numel);
  std::pair<size_t, flexible_type*> ptr = get_ptr();
  ASSERT_EQ(ptr.first, numel);
  for (size_t i = 0;i < ptr.first; ++i) {
    iarc >> ptr.second[i] >> ptr.second[i].tag();
  }
}

void flexible_type_record::resize_and_clear(size_t numel) {
  clear();
  if (numel == 1) {
    add_one_element();
  } else {
    // allocate the array
    flexible_type* newarr = (flexible_type*)malloc(sizeof(flexible_type) * numel);
    std::uninitialized_fill_n(newarr, numel, flexible_type());
    values.ptr.first = numel;
    values.ptr.second = newarr;
  }
}
flexible_type* flexible_type_record::add_one_element() {
  // inserts an element
  if (is_empty()) {
    // upgrade to single from empty
    new (&values.single) flexible_type();
    // set the bits to show I am now single
    set_single();
    return &values.single;
  } else if (is_single()) {
    // upgrade to array from single
    // allocate the new array
    flexible_type* newarr = (flexible_type*)malloc(sizeof(flexible_type) * 2);
    std::uninitialized_fill_n(newarr, 2, flexible_type());
    //move the current contents to newarr[0]
    newarr[0] = std::move(values.single);
    newarr[0].tag() = values.single.tag();
    // delete the current value
    values.single.~flexible_type();
    // fill the union
    values.ptr.first = 2;
    values.ptr.second = newarr;
    return values.ptr.second + 1;
  } else if (is_array()) {
    // resize the array
    ++values.ptr.first;
    values.ptr.second = (flexible_type*)realloc(values.ptr.second, 
                                sizeof(flexible_type) * values.ptr.first);
    // initialize the newly created element using placement new
    new (values.ptr.second + (values.ptr.first - 1)) flexible_type();
    return values.ptr.second + (values.ptr.first - 1);
  }
  return NULL;
}
} // namespace
