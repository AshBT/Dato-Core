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
#include <unity/toolkits/graph_analytics/connected_component.hpp>
#include <unity/lib/toolkit_util.hpp>
#include <unity/lib/simple_model.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <metric/simple_metrics_service.hpp>
#include <sgraph/sgraph_compute.hpp>
#include <sframe/algorithm.hpp>
#include <sframe/groupby_aggregate.hpp>
#include <sframe/groupby_aggregate_operators.hpp>
#include <table_printer/table_printer.hpp>
#include <atomic>

namespace graphlab {
namespace connected_component {

const std::string COMPONENT_ID_COLUMN = "component_id";

/**
 * Initialize a unique component id from 0 to N-1
 */
void init_component_id(sgraph& g) {
  auto& vgroup = g.vertex_group();
  std::vector<size_t> beginids{0};
  size_t acc = 0;
  for (auto& sf: vgroup) {
    acc += sf.size();
    beginids.push_back(acc);
  }
  parallel_for (0, vgroup.size(), [&](size_t partitionid) {
    size_t begin = beginids[partitionid];
    size_t end = beginids[partitionid+1];
    std::shared_ptr<sarray<flexible_type>> id_column =
        std::make_shared<sarray<flexible_type>>();
    id_column->open_for_write(1);
    id_column->set_type(flex_type_enum::INTEGER);
    auto out = id_column->get_output_iterator(0);
    while(begin != end) {
      *out = begin;
      ++begin;
      ++out;
    }
    id_column->close();
    vgroup[partitionid] = vgroup[partitionid].add_column(id_column, COMPONENT_ID_COLUMN);
  });
}

/**
 * Compute connected component on the graph, add a new column to the vertex
 * with name "component_id".
 *
 * Algorithm is simple:
 * Init every vertex with the component_id = vertexid
 * while(change) {
 *  Each vertex repeatedly gather neighbors id and choose the min id.
 * }
 *
 * Returns an sframe with component id and component size information.
 */
sframe compute_connected_component(sgraph& g) {
  typedef sgraph_compute::sgraph_engine<flexible_type>::graph_data_type graph_data_type;
  typedef sgraph::edge_direction edge_direction;

  init_component_id(g);
  std::atomic<long> num_changed;
  sgraph_compute::sgraph_engine<flexible_type> ga;

  const size_t cid_idx = g.get_vertex_field_id(COMPONENT_ID_COLUMN);

  table_printer table({{"Number of vertices updated", 0}});
  table.print_header();

  sgraph_compute::triple_apply_fn_type apply_fn =
      [&](sgraph_compute::edge_scope& scope) {
        size_t src_cid = scope.source()[cid_idx];
        size_t dst_cid = scope.target()[cid_idx];
        if (src_cid != dst_cid) {
          size_t min_cid = std::min<size_t>(src_cid, dst_cid);
          scope.source()[cid_idx] = min_cid;
          scope.target()[cid_idx] = min_cid;
          ++num_changed;
        }
      };

  while(true) {
    if(cppipc::must_cancel()) {
      log_and_throw(std::string("Toolkit cancelled by user."));
    }
    num_changed = 0;
    sgraph_compute::triple_apply(g, apply_fn, {COMPONENT_ID_COLUMN});
    table.print_row(num_changed);
    if (num_changed == 0) {
      break;
    }
  }

  table.print_footer();
  sframe component_info;
  if (g.get_vertices().size() > 0) {
   component_info = groupby_aggregate(std::move(g.get_vertices()),
                                      {COMPONENT_ID_COLUMN},
                                      {"Count"},
                                      {{{}, std::make_shared<groupby_operators::count>()}});
  } else {
    component_info.open_for_write({COMPONENT_ID_COLUMN, "Count"},
                                  {flex_type_enum::INTEGER, flex_type_enum::INTEGER}, "", 1);
    component_info.close();
  }
  return component_info;
}


/**************************************************************************/
/*                                                                        */
/*                             Main Function                              */
/*                                                                        */
/**************************************************************************/
toolkit_function_response_type get_default_options(toolkit_function_invocation& invoke) {
  toolkit_function_response_type response;
  response.success = true;
  return response;
}

toolkit_function_response_type exec(toolkit_function_invocation& invoke) {
  // no setup needed

  timer mytimer;
  std::shared_ptr<unity_sgraph> source_graph =
      safe_varmap_get<std::shared_ptr<unity_sgraph>>(invoke.params, "graph");

  ASSERT_TRUE(source_graph != NULL);
  sgraph& source_sgraph = source_graph->get_graph();
  // Do not support vertex groups yet.
  ASSERT_EQ(source_sgraph.get_num_groups(), 1);

  // Setup the graph we are going to work on. Copying sgraph is cheap.
  sgraph g(source_sgraph);
  g.select_vertex_fields({sgraph::VID_COLUMN_NAME});
  g.select_edge_fields({sgraph::SRC_COLUMN_NAME, sgraph::DST_COLUMN_NAME});

  sframe components = compute_connected_component(g);
  std::shared_ptr<unity_sframe> components_wrapper(new unity_sframe());
  components_wrapper->construct_from_sframe(components);

  std::shared_ptr<unity_sgraph> result_graph(new unity_sgraph(std::make_shared<sgraph>(g)));


  variant_map_type params;
  params["graph"] = to_variant(result_graph);
  params["component_id"] = to_variant(result_graph->get_vertices());
  params["training_time"] = mytimer.current_time();
  params["component_size"] = to_variant(components_wrapper);

  toolkit_function_response_type response;
  response.params["model"] = to_variant(std::make_shared<simple_model>(params));
  response.success = true;
  return response;
}

static const variant_map_type MODEL_FIELDS{
  {"graph", "A new SGraph with the color id as a vertex property"},
  {"component_id", "An SFrame with each vertex's component id"},
  {"component_size", "An SFrame with the size of each component"},
  {"training_time", "Total training time of the model"}
};

toolkit_function_response_type get_model_fields(toolkit_function_invocation& invoke) {
  toolkit_function_response_type response;
  response.success = true;
  response.params = MODEL_FIELDS;
  return response;
}
/**************************************************************************/
/*                                                                        */
/*                          Toolkit Registration                          */
/*                                                                        */
/**************************************************************************/


std::vector<toolkit_function_specification> get_toolkit_function_registration() {
  toolkit_function_specification main_spec;
  main_spec.name = "connected_components";
  main_spec.toolkit_execute_function = exec;

  toolkit_function_specification option_spec;
  option_spec.name = "connected_components_default_options";
  option_spec.toolkit_execute_function = get_default_options;

  toolkit_function_specification model_spec;
  model_spec.name = "connected_components_model_fields";
  model_spec.toolkit_execute_function = get_model_fields;
  return {main_spec, option_spec, model_spec};
}
} // end of namespace connected components






} // end of namespace graphlab
