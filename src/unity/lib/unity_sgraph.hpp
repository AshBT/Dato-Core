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
#ifndef GRAPHLAB_UNITY_SGRAPH_HPP
#define GRAPHLAB_UNITY_SGRAPH_HPP
#include <unity/lib/api/unity_graph_interface.hpp>
#include <unity/lib/api/unity_sframe_interface.hpp>
#include <unity/lib/sgraph_triple_apply_typedefs.hpp>
#include <sgraph/sgraph_constants.hpp>
#include <sframe/dataframe.hpp>
#include <memory>

namespace graphlab {

// forward declarations
template <typename T>
class lazy_eval_operation_dag;
template <typename T>
class lazy_eval_future;

class sframe;
class sgraph;

typedef lazy_eval_operation_dag<sgraph> unity_graph_dag_type;
typedef lazy_eval_future<sgraph> sgraph_future;

/** 
 * \ingroup unity
 * The \ref graphlab::unity_sgraph and \ref graphlab::unity_sgraph_base classes
 * implement a graph object on the server side which is exposed to the
 * client via the cppipc system.
 *
 * The unity_sgraph is a lazily evaluated, immutable
 * graph datastructure where most operations do not take time, and instead,
 * the graph is only fully constructed when accessed. Furthermore we can
 * further exploit immutability for efficiency, by allowing graphs to shared
 * data/structure/etc through the use of shared_ptr, etc.
 */
class unity_sgraph: public unity_sgraph_base {
  public:
    /// Global lazy evaluation DAG object
    static unity_graph_dag_type* dag_singleton;

    /// Gets the lazy evaluation DAG object
    static unity_graph_dag_type* get_dag();

    static const char* GRAPH_MAGIC_HEADER;

    /// Default constructor
    explicit unity_sgraph(size_t npartitions = SGRAPH_DEFAULT_NUM_PARTITIONS);

    /**
     * Constructs a unity_sgraph by taking over an existing sgraph
     * object.
     */
    unity_sgraph(std::shared_ptr<sgraph>);

    /**
     * Makes a copy of a graph. (This is a shallow copy. The resultant graphs
     * share pointers. But since graphs are immutable, this is safe and can be
     * treated like a deep copy.)
     */
    explicit unity_sgraph(const unity_sgraph&) = default;

    /**
     * Copy assignment.
     */
    unity_sgraph& operator=(const unity_sgraph&) = default;

    /// Destructor
    virtual ~unity_sgraph();

    /**
     * Returns a new copy of this graph object
     */
    std::shared_ptr<unity_sgraph_base> clone();

    /**************************************************************************/
    /*                                                                        */
    /*                     UNITY GRAPH BASE INTERFACE API                     */
    /*                                                                        */
    /**************************************************************************/

    /**
     * Returns a sframe of vertices satisfying the certain constraints.
     *
     * \ref vid_vec A list of vertices. If provided, will only return vertices
     *              in the list. Otherwise all vertices will be considered.
     * \ref field_constraint A mapping of string-->value. Only vertices which
     *              contain a field with the particular value will be returned.
     *              value can be UNDEFINED, in which case vertices simply must
     *              contain the particular field.
     * \ref group The vertex group id.
     */
    std::shared_ptr<unity_sframe_base> get_vertices(
        const std::vector<flexible_type>& vid_vec = std::vector<flexible_type>(),
        const options_map_t& field_constraint = options_map_t(),
        size_t group = 0);

    /**
     * Returns a sframe of edges satisfying certain constraints.
     * source_vids and target_vids arrays must match up in length, and denote
     * source->target edges. For instance: i-->j will return the edge i--> if it 
     * exists. Wildcards are supported by setting the flexible_type to UNDEFINED.
     * For instance, i-->UNDEFINED will match every edge with source i. And
     * UNDEFINED-->j will match every edge with target j.
     *
     * The returned edge will have source vertices from one group and
     * target vertices from another group (can be the same).
     *
     * The edge must further match the values specified by the field_constraint.
     *
     * \ref source_vids A list of source vertices or wildcards (UNDEFINED).
     *                  Must match the length of target_vids. See above for 
     *                  semantics.
     * \ref target_vids A list of target vertices or wildcards (UNDEFINED).
     *                  Must match the length of source_vids. See above for 
     *                  semantics.
     * \ref field_constraint A mapping of string-->value. Only edges which
     *              contain a field with the particular value will be returned.
     *              value can be UNDEFINED, in which case edges simply must
     *              contain the particular field.
     *
     * \ref groupa The vertex group id for source vertices.
     *
     * \ref groupb The vertex group id for target vertices.
     */
    std::shared_ptr<unity_sframe_base> get_edges(const std::vector<flexible_type>& source_vids=std::vector<flexible_type>(),
                                 const std::vector<flexible_type>& target_vids=std::vector<flexible_type>(),
                                 const options_map_t& field_constraint=options_map_t(),
                                 size_t groupa = 0, size_t groupb = 0);

    /**
     * Returns a summary of the basic graph information such as the number of
     * vertices / number of edges.
     */
    options_map_t summary();

    /**
     * Adds each row of the sframe as a new vertex; A new graph
     * corresponding to the current graph with new vertices added will be
     * returned.  Columns of the dataframes are treated as fields (new fields
     * will be created as appropriate). The column with name 'id_field_name'
     * will be treated as the vertex ID field. The column with name
     * 'id_field_name' must therefore exist in the 'vertices' dataframe. If the
     * vertex with the given ID already exists, all field values will
     * overwrite. This function can therefore be also used to perform
     * modification of graph data. An exception is thrown on failure. 
     */
    std::shared_ptr<unity_sgraph_base> add_vertices(
        std::shared_ptr<unity_sframe_base> vertices,
        const std::string& id_field_name, size_t group = 0);


    /**
     * Adds each row of the sframe as a new edge; A new graph
     * corresponding to the current graph with new edges added will be
     * returned.  Columns of the dataframes are treated as fields (new fields
     * will be created as appropriate). The columns with name 'source_field_name'
     * and 'target_field_name' will be used to denote the source
     * and target vertex IDs for the edge. These columns must therefore exist
     * in the 'edges' dataframe. If the vertex does not exist, it will be
     * created. Unlike add_vertices, edge "merging" is not performed, but
     * instead, multiple edges can be created between any pair of vertices.
     * Throws an exception on failure.
     */
    std::shared_ptr<unity_sgraph_base> add_edges(
        std::shared_ptr<unity_sframe_base> edges,
        const std::string& source_field_name,
        const std::string& target_field_name,
        size_t groupa = 0, size_t groupb = 0);

    /**
     * Returns a list of of the vertex fields in the graph
     */
    std::vector<std::string> get_vertex_fields(size_t group = 0);

    /**
     * Returns a list of of the vertex fields in the graph
     */
    std::vector<flex_type_enum> get_vertex_field_types(size_t group = 0);

    /**
     * Returns a list of of the edge fields in the graph
     */
    std::vector<std::string> get_edge_fields(size_t groupa = 0, size_t groupb = 0);

    /**
     * Returns a list of of the edge field types in the graph
     */
    std::vector<flex_type_enum> get_edge_field_types(size_t groupa = 0, size_t groupb = 0);

    /**
     * Returns a new graph corresponding to the curent graph with only
     * the fields listed in "fields".
     */
    std::shared_ptr<unity_sgraph_base> select_vertex_fields(
        const std::vector<std::string>& fields, size_t group=0);

    /**
     * Returns a new graph corresponding to the current graph with the field
     * "field" renamed to "newfield". 
     */
    std::shared_ptr<unity_sgraph_base> copy_vertex_field(
        std::string field, std::string newfield, size_t group=0);

    /**
     * Returns a new graph corresponding to the curent graph with the field
     * "field" deleted.
     */
    std::shared_ptr<unity_sgraph_base> delete_vertex_field(
        std::string field, size_t group=0);

    /**
     * Add a new vertex field with column_data and return as a new graph.
     */
    std::shared_ptr<unity_sgraph_base> add_vertex_field(
        std::shared_ptr<unity_sarray_base> column_data, std::string field);

    /**
     * Rename the edge fields whoes names are in oldnames to the corresponding new names.
     * Return the new graph.
     */
    std::shared_ptr<unity_sgraph_base> rename_vertex_fields(
        const std::vector<std::string>& oldnames,
        const std::vector<std::string>& newnames);

    /**
     * Switch the column order of field1 and field2 in the vertex data.
     * Return the new graph.
     */
    std::shared_ptr<unity_sgraph_base> swap_vertex_fields(
        const std::string& field1, const std::string& field2);


    /**
     * Returns a new graph corresponding to the curent graph with only
     * the fields listed in "fields".
     */
    std::shared_ptr<unity_sgraph_base> select_edge_fields(
        const std::vector<std::string>& fields,
        size_t groupa=0, size_t groupb=0);

    /**
     * Returns a new graph corresponding to the current graph with the field
     * "field" renamed to "newfield". 
     */
    std::shared_ptr<unity_sgraph_base> copy_edge_field(
        std::string field, std::string newfield,
        size_t groupa=0, size_t groupb=0);

    /**
     * Returns a new graph corresponding to the curent graph with the field
     * "field" deleted.
     */
    std::shared_ptr<unity_sgraph_base> delete_edge_field(
        std::string field, size_t groupa=0, size_t groupb=0);

    /**
     * Add a new edge field with column_data and return as a new graph.
     */
    std::shared_ptr<unity_sgraph_base> add_edge_field(
        std::shared_ptr<unity_sarray_base> column_data, std::string field);

    /**
     * Rename the edge fields whoes names are in oldnames to the corresponding new names.
     * Return the new graph.
     */
    std::shared_ptr<unity_sgraph_base> rename_edge_fields(
        const std::vector<std::string>& oldnames,
        const std::vector<std::string>& newnames);

    /**
     * Switch the column order of field1 and field2 in the vertex data.
     * Return the new graph.
     */
    std::shared_ptr<unity_sgraph_base> swap_edge_fields(const std::string& field1, const std::string& field2);


    std::shared_ptr<unity_sgraph_base> lambda_triple_apply(const std::string& lambda_str,
                                          const std::vector<std::string>& mutated_fields);

    std::shared_ptr<unity_sgraph_base> lambda_triple_apply_native(
        const lambda_triple_apply_fn& lambda,
        const std::vector<std::string>& mutated_fields);

    std::shared_ptr<unity_sgraph_base> lambda_triple_apply_native(
        const function_closure_info& toolkit_fn_name,
        const std::vector<std::string>& mutated_fields);

    /**************************************************************************/
    /*                                                                        */
    /*                           Internal Functions                           */
    /*                                                                        */
    /**************************************************************************/
    /**
     * Internal
     *
     * Returns a reference to the underlying sgraph.
     *
     * Note: This operation willforce the lazy operations to be performed.
     */
    sgraph& get_graph() const;

    /**
     * Internal
     *
     * Deep serialization.
     */
    void save(oarchive& oarc) const;

    /**
     * Internal
     *
     * Deep deserialization.
     */
    void load(iarchive& iarc);

    /**
     * Saves the graph with the specified name to a given file in a
     * non-portable binary format. File can be on disk, or on HDFS.
     *
     * Supported formats are 'bin', 'json', 'csv'.
     *
     * Returns true on success, false on failure.
     */
    bool save_graph(std::string target_dir, std::string format);

    /**
     * Loads the graph from the given file in a 
     * non-portable binary format. File can be on disk, or on HDFS.
     * Returns true on success, false on failure.
     */
    bool load_graph(std::string target_dir);


  private:
    mutable std::shared_ptr<sgraph_future> m_graph;

  private:
    void fast_validate_add_vertices(const sframe& vertices,
                                    std::string id_field,
                                    size_t group) const;

    void fast_validate_add_edges(const sframe& vertices,
                                 std::string src_field,
                                 std::string dst_field,
                                 size_t groupa, size_t groupb) const;
    /**
     * Returns an lazy edge sframe containing all the edges from groupa to groupb.
     */
    std::shared_ptr<unity_sframe_base> get_edges_lazy(size_t groupa = 0, size_t groupb = 0);
};
} // namespace graphlab
#endif
