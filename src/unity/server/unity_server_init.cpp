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
#include <unity/server/unity_server_init.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <image/image_util.hpp>
#include <unity/toolkits/graph_analytics/include.hpp>
#include <unity/lib/simple_model.hpp>
#include <unity/lib/unity_odbc_connection.hpp>
#include <unity/lib/unity_odbc_connection.hpp>


graphlab::toolkit_function_registry* init_toolkits() {
  graphlab::toolkit_function_registry* g_toolkit_functions = new graphlab::toolkit_function_registry();

  g_toolkit_functions->register_toolkit_function(graphlab::pagerank::get_toolkit_function_registration());
  g_toolkit_functions->register_toolkit_function(graphlab::kcore::get_toolkit_function_registration());
  g_toolkit_functions->register_toolkit_function(graphlab::connected_component::get_toolkit_function_registration());
  g_toolkit_functions->register_toolkit_function(graphlab::graph_coloring::get_toolkit_function_registration());
  g_toolkit_functions->register_toolkit_function(graphlab::triangle_counting::get_toolkit_function_registration());
  g_toolkit_functions->register_toolkit_function(graphlab::sssp::get_toolkit_function_registration());

  g_toolkit_functions->register_toolkit_function(graphlab::image_util::get_toolkit_function_registration());

  return g_toolkit_functions;
}


graphlab::toolkit_class_registry* init_models() {
  graphlab::toolkit_class_registry* g_toolkit_classes = new graphlab::toolkit_class_registry();

  register_model_helper<graphlab::simple_model>(g_toolkit_classes);

  g_toolkit_classes->register_toolkit_class(graphlab::odbc_connection::get_toolkit_class_registration(), "_odbc_connection");

  return g_toolkit_classes;
}

