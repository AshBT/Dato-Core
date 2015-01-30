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

#include <flexible_type/flexible_type_registry.hpp>
#include <typeinfo>
#include <iostream>
#include <utility>

namespace graphlab {

  field_id_type flexible_type_registry::get_field_id(std::string name) const {
    auto found = _registry_name.find(name);
    if (found == _registry_name.end()) {
      logstream(LOG_INFO) << "No field found for name: " << name << std::endl;
      return (field_id_type)(-1);
    } else {
      return (field_id_type) found->second;
    }

    return (field_id_type)(-1);
  }



  std::string flexible_type_registry::get_field_name(field_id_type id) const {
    for(auto& nameidpair : _registry_name) {
      if (nameidpair.second == id) return nameidpair.first;
    }
    return "";
  }


  field_id_type flexible_type_registry::register_field(std::string name, 
                                                const flexible_type& value) {
    return register_field(name, value.get_type());
  }

  field_id_type flexible_type_registry::register_field(std::string name, 
                                                flex_type_enum type) {
    // variable name already exists
    if (_registry_name.count(name)) {
      logstream(LOG_INFO) << "Failed to registering a field " << name 
                          << " of type " << (int)type
                          << " because field already exists" << std::endl;
      return (field_id_type)(-1);
    }
    field_id_type field_id = this->_registered_field_counter;
    this->_registered_field_counter++;
    logstream(LOG_INFO) << "Registering a field " << name << " of type " 
                        << (int)type << std::endl;
    _registry_name[name] = field_id;
    _registry_index_to_name[field_id] = name;
    _registry_index[field_id] = type;
    return field_id;
  }

  void flexible_type_registry::unregister_field(std::string name) {
    auto found_name = _registry_name.find(name);
    if (found_name == _registry_name.end()) {
      // if name not found, nothing else to do here
      return;
    }
    
    field_id_type field_id = found_name->second;
    _registry_name.erase(found_name);

    // delete from _registry_index_to_name if found
    auto found_index_to_name_iter = _registry_index_to_name.find(field_id);
    if (found_index_to_name_iter != _registry_index_to_name.end()) {
      _registry_index_to_name.erase(found_index_to_name_iter );
    }

    // delete from _registry_index if found
    auto found_index_iter = _registry_index.find(field_id);
    if (found_index_iter != _registry_index.end()) {
      _registry_index.erase(found_index_iter);
    }
  }


  std::vector<std::string> flexible_type_registry::get_all_field_names() const {
    std::vector<std::string> ret;
    for(auto iter: _registry_name) ret.push_back(iter.first);
    return ret;
  }

  std::pair<bool, flex_type_enum> 
  flexible_type_registry::get_field_type(std::string name) const {
    auto iter = _registry_name.find(name);
    if (iter == _registry_name.end()) {
      return {false, flex_type_enum::UNDEFINED};
    } else {
      return {true, _registry_index.at(iter->second)};
    } 
  }


  std::pair<bool, flex_type_enum> 
  flexible_type_registry::get_field_type(field_id_type id) const {
    auto iter = _registry_index.find(id);
    if (iter == _registry_index.end()) {
      return {false, flex_type_enum::UNDEFINED};
    } else {
      return {true, iter->second};
    } 
  }

  void flexible_type_registry::register_id_fields(flex_type_enum id_type) {
    if (!has_field("__id")) {
      register_field("__id", id_type);
      register_field("__src_id", id_type);
      register_field("__dst_id", id_type);
    } else {
      if (get_field_type("__id").second != id_type) {
        log_and_throw(std::string("ID field type mismatch"));
      }
    }
  }
} // namespace
