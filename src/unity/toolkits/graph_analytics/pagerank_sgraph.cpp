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
#include <unity/toolkits/graph_analytics/pagerank.hpp>
#include <unity/lib/toolkit_util.hpp>
#include <unity/lib/simple_model.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <sgraph/sgraph_compute.hpp>
#include <sframe/algorithm.hpp>
#include <table_printer/table_printer.hpp>

namespace graphlab {
  namespace pagerank {

  const std::string PAGERANK_COLUMN = "pagerank";
  const std::string PREV_PAGERANK_COLUMN = "prev_pagerank";
  const std::string DELTA_COLUMN = "delta";
  const std::string OUT_DEGREE_COLUMN = "out_degree";

  double reset_probability = 0.15;
  double threshold = 1e-2;
  int max_iterations = 20;

  /**************************************************************************/
  /*                                                                        */
  /*                   Setup and Teardown functions                         */
  /*                                                                        */
  /**************************************************************************/
  void setup(toolkit_function_invocation& invoke) {
    threshold = safe_varmap_get<flexible_type>(invoke.params, "threshold");
    reset_probability = safe_varmap_get<flexible_type>(invoke.params, "reset_probability");
    max_iterations = safe_varmap_get<flexible_type>(invoke.params, "max_iterations");
    if (threshold < 0) {
      throw("Parameter 'threshold' must be positive.");
    } else if (reset_probability < 0 || reset_probability > 1) {
      throw("Parameter 'reset_probability' should be between 0 and 1.");
    } else if (max_iterations <= 0) {
      throw("Max iterations should be positive.");
    }
  }

void triple_apply_pagerank(sgraph& g, size_t& num_iter, double& total_pagerank, double& total_delta) {
  typedef sgraph_compute::sgraph_engine<flexible_type>::graph_data_type graph_data_type;
  typedef sgraph::edge_direction edge_direction;

  // initialize every vertex with core id kmin
  g.init_vertex_field(PAGERANK_COLUMN, reset_probability);
  g.init_vertex_field(PREV_PAGERANK_COLUMN, 1.0);
  g.init_vertex_field(DELTA_COLUMN, 0.0);

  // Initialize degree count
  sgraph_compute::sgraph_engine<flexible_type> ga;
  auto degrees = ga.gather(
          g,
          [=](const graph_data_type& center,
              const graph_data_type& edge,
              const graph_data_type& other,
              edge_direction edgedir,
              flexible_type& combiner) {
              combiner += 1;
          },
          flexible_type(0),
          edge_direction::OUT_EDGE);
  g.add_vertex_field(degrees, OUT_DEGREE_COLUMN);

  num_iter = 0;
  total_delta = 0.0;
  total_pagerank = 0.0;
  timer mytimer;

  // Triple apply
  double w = (1 - reset_probability);
  const size_t degree_idx = g.get_vertex_field_id(OUT_DEGREE_COLUMN);
  const size_t pr_idx = g.get_vertex_field_id(PAGERANK_COLUMN);
  const size_t old_pr_idx = g.get_vertex_field_id(PREV_PAGERANK_COLUMN);

  sgraph_compute::triple_apply_fn_type apply_fn =
    [&](sgraph_compute::edge_scope& scope) {
      auto& source = scope.source();
      auto& target = scope.target();
      scope.lock_vertices();
      target[pr_idx] += w * source[old_pr_idx] / source[degree_idx];
      scope.unlock_vertices();
    };

  table_printer table({{"Iteration", 0}, 
                                {"L1 change in pagerank", 0}});
  table.print_header();

  for (size_t iter = 0; iter < max_iterations; ++iter) {
    if(cppipc::must_cancel()) {
      log_and_throw(std::string("Toolkit cancelled by user."));
    }

    mytimer.start();
    ++num_iter;

    g.init_vertex_field(PAGERANK_COLUMN, reset_probability);

    sgraph_compute::triple_apply(g, apply_fn, {PAGERANK_COLUMN});

    // compute the change in pagerank
    auto delta = sgraph_compute::vertex_apply(
        g,
        flex_type_enum::FLOAT,
        [&](const std::vector<flexible_type>& vdata) {
          return std::abs((double)(vdata[pr_idx]) - (double)(vdata[old_pr_idx]));
        });

    // make the current pagerank the old pagerank
    g.copy_vertex_field(PAGERANK_COLUMN, PREV_PAGERANK_COLUMN);
    g.replace_vertex_field(delta, DELTA_COLUMN);

    total_delta = 
        sgraph_compute::vertex_reduce<double>(g, 
                               DELTA_COLUMN,
                               [](const flexible_type& v, double& acc) {
                                 acc += (flex_float)v;
                               },
                               [](const double& v, double& acc) {
                                 acc += v;
                               });


    table.print_row(iter+1, total_delta);

    // check convergence
    if (total_delta < threshold) {
      break;
    }
  } // end of pagerank iterations

  table.print_footer();

  // cleanup
  g.remove_vertex_field(PREV_PAGERANK_COLUMN);
  g.remove_vertex_field(OUT_DEGREE_COLUMN);
  total_pagerank =
      sgraph_compute::vertex_reduce<double>(g, 
                                     PAGERANK_COLUMN,
                                     [](const flexible_type& v, double& acc) {
                                       acc += (flex_float)v;
                                     },
                                     [](const double& v, double& acc) {
                                       acc += v;
                                     });
}



  /**************************************************************************/
  /*                                                                        */
  /*                             Main Function                              */
  /*                                                                        */
  /**************************************************************************/
  toolkit_function_response_type exec(toolkit_function_invocation& invoke) {

    timer mytimer;
    setup(invoke);

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
    double total_pagerank;
    double delta;
    size_t num_iter;

    triple_apply_pagerank(g, num_iter, total_pagerank, delta);

    std::shared_ptr<unity_sgraph> result_graph(new unity_sgraph(std::make_shared<sgraph>(g)));

    variant_map_type params;
    params["graph"] = to_variant(result_graph);
    params["pagerank"] = to_variant(result_graph->get_vertices());
    params["delta"] = delta;
    params["training_time"] = mytimer.current_time();
    params["num_iterations"] = num_iter;
    params["reset_probability"] = reset_probability;
    params["threshold"] = threshold;
    params["max_iterations"] = max_iterations;

    toolkit_function_response_type response;
    response.params["model"]= to_variant(std::make_shared<simple_model>(params));
    response.success = true;

    return response;
  }

  static const variant_map_type DEFAULT_OPTIONS {
    {"threshold", 1E-2}, {"reset_probability", 0.15}, {"max_iterations", 20}
  };

  toolkit_function_response_type get_default_options(toolkit_function_invocation& invoke) {
    toolkit_function_response_type response;
    response.success = true;
    response.params = DEFAULT_OPTIONS;
    return response;
  }

  static const variant_map_type MODEL_FIELDS{
    {"graph", "A new SGraph with the pagerank as a vertex property"},
    {"pagerank", "An SFrame with each vertex's pagerank"},
    {"delta", "Change in pagerank for the last iteration in L1 norm"},
    {"training_time", "Total training time of the model"},
    {"num_iterations", "Number of iterations"},
    {"reset_probability", "The probablity of randomly jumps to any node in the graph"},
    {"threshold", "The convergence threshold in L1 norm"},
    {"max_iterations", "The maximun number of iterations to run"},
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
    main_spec.name = "pagerank";
    main_spec.toolkit_execute_function = exec;
    main_spec.default_options = DEFAULT_OPTIONS;

    toolkit_function_specification option_spec;
    option_spec.name = "pagerank_default_options";
    option_spec.toolkit_execute_function = get_default_options;

    toolkit_function_specification model_spec;
    model_spec.name = "pagerank_model_fields";
    model_spec.toolkit_execute_function = get_model_fields;
    return {main_spec, option_spec, model_spec};
  }
} // end of namespace connected components
} // end of namespace graphlab
