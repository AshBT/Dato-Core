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
#ifndef GRAPHLAB_UNITY_SERVER_UNITY_SERVER_INIT_HPP
#define GRAPHLAB_UNITY_SERVER_UNITY_SERVER_INIT_HPP

#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <unity/lib/api/model_interface.hpp>

graphlab::toolkit_function_registry* init_toolkits();

graphlab::toolkit_class_registry* init_models();

/// A helper function to automatically add an entry to the model
///// lookup table with the proper information.
template <typename Model> 
static void register_model_helper(graphlab::toolkit_class_registry* g_toolkit_classes) {
  Model m;
  std::string name = (dynamic_cast<graphlab::model_base*>(&m))->name();
  g_toolkit_classes->register_toolkit_class(name, [](){return new Model;});
}


#endif
