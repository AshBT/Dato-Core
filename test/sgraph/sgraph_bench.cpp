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
#include<sgraph/sgraph.hpp>
#include<sgraph/sgraph_compute.hpp>
#include<sframe/csv_line_tokenizer.hpp>
#include<sframe/algorithm.hpp>
#include<logger/logger.hpp>
#include<timer/timer.hpp>
#include<iostream>

using namespace graphlab;

void print_usage() {
  std::cerr << "usage: ./sgraph_bench [graph_file]\n";
}

void compute_pagerank(sgraph& g, size_t num_iter) {
  sgraph_compute::sgraph_engine<flexible_type> ga;
  std::vector<sframe>& vdata = g.vertex_group();
  // merge the outgoing degree to graph
  for (size_t i = 0; i < g.get_num_partitions(); ++i) {
    std::shared_ptr<sarray<flexible_type>> sa = std::make_shared<sarray<flexible_type>>();
    sa->open_for_write();
    sa->set_type(flex_type_enum::FLOAT);
    std::vector<flexible_type> data(vdata[i].size(), 1.0);
    graphlab::copy(data.begin(), data.end(), *sa);
    sa->close();
    vdata[i] = vdata[i].add_column(sa, "pagerank");
  }

  typedef sgraph_compute::sgraph_engine<flexible_type>::graph_data_type graph_data_type;
  typedef sgraph::edge_direction edge_direction;
  // count the outgoing degree
  std::vector<std::shared_ptr<sarray<flexible_type>>> ret = ga.gather(g,
                                                      [](const graph_data_type& center, 
                                                         const graph_data_type& edge, 
                                                         const graph_data_type& other, 
                                                         edge_direction edgedir,
                                                         flexible_type& combiner) {
                                                      combiner = combiner + 1;
                                                      },
                                                      flexible_type(0),
                                                      edge_direction::OUT_EDGE);
  // merge the outgoing degree to graph
  for (size_t i = 0; i < g.get_num_partitions(); ++i) {
    ASSERT_LT(i, vdata.size());
    ASSERT_LT(i, ret.size());
    vdata[i] = vdata[i].add_column(ret[i], "__out_degree__");
  }

  size_t degree_idx = vdata[0].column_index("__out_degree__");
  size_t data_idx = vdata[0].column_index("pagerank");

  // now we compute the pagerank
  for (size_t iter = 0; iter < num_iter; ++iter) {
    ret = ga.gather(g,
        [=](const graph_data_type& center,
            const graph_data_type& edge,
            const graph_data_type& other,
            edge_direction edgedir,
            flexible_type& combiner) {
           combiner = combiner + 0.85 * (other[data_idx] / other[degree_idx]);
        },
        flexible_type(0.15),
        edge_direction::IN_EDGE);
    for (size_t i = 0; i < g.get_num_partitions(); ++i) {
      vdata[i] = vdata[i].replace_column(ret[i], "pagerank");
    }
    // g.get_vertices().debug_print();
  }
}

int main(int argc, char** argv) {
  global_logger().set_log_level(LOG_INFO);
  timer mytimer;
  if (argc < 2) {
    print_usage();
    exit(0);
  }

  char* graph_file = argv[1];
  std::cerr << "Loading sframe from " << graph_file << std::endl; 
  csv_line_tokenizer snap_parser;
  snap_parser.delimiter = '\t';
  sframe sf;
  mytimer.start();
  sf.init_from_csvs(std::string(graph_file), snap_parser,
      false, // no header
      false, // do not continue on failure
      false, // do not store errors
      {{"X1", flex_type_enum::INTEGER},
       {"X2", flex_type_enum::INTEGER}});
  std::cerr << "Finishing reading csv in " << mytimer.current_time() << " secs" << std::endl;

  mytimer.start();
  size_t npartition = 8;
  sgraph g(npartition);
  g.add_edges(sf, "X1", "X2");
  std::cerr << "Finishing graph construction in " << mytimer.current_time() << " secs" << std::endl;

  mytimer.start();
  compute_pagerank(g, 1);
  std::cerr << "Run 1 iter of pagerank in " << mytimer.current_time() << " secs" << std::endl;

  return 0;
}
