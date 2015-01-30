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
#ifndef GRAPHLAB_UNITY_GRAPH_INTERFACE_HPP
#define GRAPHLAB_UNITY_GRAPH_INTERFACE_HPP
#include <memory>
#include <vector>
#include <string>
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/options_map.hpp>
#include <unity/lib/api/unity_sframe_interface.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {


#if DOXYGEN_DOCUMENTATION
// Doxygen fake documentation

/**
 * The \ref graphlab::unity_graph and \ref graphlab::unity_sgraph_base classes 
 * implement a graph object on the server side which is exposed to the 
 * client via the cppipc system. The unity_graph is a lazily evaluated, immutable 
 * graph datastructure where most operations do not take time, and instead,
 * the graph is only fully constructed when accessed. See 
 * \ref graphlab::unity_graph for detailed documentation on the functions.
 */
class unity_sgraph_base {
    options_map_t summary();
    std::vector<std::string> get_fields();
    std::shared_ptr<unity_sframe_base> get_vertices(const std::vector<flexible_type>&,
                             const options_map_t&);
    std::shared_ptr<unity_sframe_base> get_edges(const std::vector<flexible_type>&
                          const std::vector<flexible_type>&
                          const options_map_t&);
    bool save_graph(std::string)
    bool load_graph(std::string)
    std::shared_ptr<unity_sgraph_base> clone()
    std::shared_ptr<unity_sgraph_base> add_vertices(dataframe_t&, const std::string&)
    std::shared_ptr<unity_sgraph_base> add_vertices(unity_sframe&, const std::string&)
    std::shared_ptr<unity_sgraph_base> add_vertices_from_file(const std::string&, const std::string&, char, bool)
    std::shared_ptr<unity_sgraph_base> add_edges_from_file(const std::string&, const std::string&, const std::string&, char, bool)
    std::shared_ptr<unity_sgraph_base> add_edges(dataframe_t&, const std::string&, const std::string&)
    std::shared_ptr<unity_sgraph_base> select_fields(const std::vector<std::string>&)
    std::shared_ptr<unity_sgraph_base> copy_field(std::string, std::string)
    std::shared_ptr<unity_sgraph_base> delete_field(std::string)
}

#endif

GENERATE_INTERFACE_AND_PROXY(unity_sgraph_base, unity_graph_proxy,
    (options_map_t, summary, )
    (std::vector<std::string>, get_vertex_fields, (size_t))
    (std::vector<std::string>, get_edge_fields, (size_t)(size_t))
    (std::vector<flex_type_enum>, get_vertex_field_types, (size_t))
    (std::vector<flex_type_enum>, get_edge_field_types, (size_t)(size_t))

    (std::shared_ptr<unity_sframe_base>, get_vertices,
        (const std::vector<flexible_type>&)(const options_map_t&)(size_t))
    (std::shared_ptr<unity_sframe_base>, get_edges,
      (const std::vector<flexible_type>&)
      (const std::vector<flexible_type>&)
      (const options_map_t&)(size_t)(size_t))
    // (bool, save_graph_as_json, (std::string))
    (bool, save_graph, (std::string)(std::string))
    (bool, load_graph, (std::string))
    (std::shared_ptr<unity_sgraph_base>, clone, )
    (std::shared_ptr<unity_sgraph_base>, add_vertices, (std::shared_ptr<unity_sframe_base>)(const std::string&)(size_t))
    (std::shared_ptr<unity_sgraph_base>, add_edges, (std::shared_ptr<unity_sframe_base>)(const std::string&)(const std::string&)(size_t)(size_t))

    (std::shared_ptr<unity_sgraph_base>, select_vertex_fields, (const std::vector<std::string>&)(size_t))
    (std::shared_ptr<unity_sgraph_base>, copy_vertex_field, (std::string)(std::string)(size_t))
    (std::shared_ptr<unity_sgraph_base>, add_vertex_field, (std::shared_ptr<unity_sarray_base>)(std::string))
    (std::shared_ptr<unity_sgraph_base>, delete_vertex_field, (std::string)(size_t))
    (std::shared_ptr<unity_sgraph_base>, rename_vertex_fields, (const std::vector<std::string>&)(const std::vector<std::string>&))
    (std::shared_ptr<unity_sgraph_base>, swap_vertex_fields, (const std::string&)(const std::string&))

    (std::shared_ptr<unity_sgraph_base>, select_edge_fields, (const std::vector<std::string>&)(size_t)(size_t))
    (std::shared_ptr<unity_sgraph_base>, add_edge_field, (std::shared_ptr<unity_sarray_base>)(std::string))
    (std::shared_ptr<unity_sgraph_base>, copy_edge_field, (std::string)(std::string)(size_t)(size_t))
    (std::shared_ptr<unity_sgraph_base>, delete_edge_field, (std::string)(size_t)(size_t))
    (std::shared_ptr<unity_sgraph_base>, rename_edge_fields, (const std::vector<std::string>&)(const std::vector<std::string>&))
    (std::shared_ptr<unity_sgraph_base>, swap_edge_fields, (const std::string&)(const std::string&))

    (std::shared_ptr<unity_sgraph_base>, lambda_triple_apply, (const std::string&)(const std::vector<std::string>&))
    (std::shared_ptr<unity_sgraph_base>, lambda_triple_apply_native, (const function_closure_info&)(const std::vector<std::string>&))
)

} // namespace graphlab
#endif // GRAPHLAB_UNITY_GRAPH_INTERFACE_HPP
