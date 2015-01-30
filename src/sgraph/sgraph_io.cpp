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
#include <logger/logger.hpp>
#include <sgraph/sgraph_io.hpp>
#include <sgraph/sgraph.hpp>
#include <sframe/sframe_io.hpp>
#include <unity/lib/json_include.hpp>

namespace graphlab {

  void save_sgraph_to_json(const sgraph& g, std::string targetfile) {
    general_ofstream fout(targetfile);
    if (!fout.good()) {
      log_and_throw_io_failure("Fail to write.");
    }

    // for one group only
    JSONNode vertices(JSON_ARRAY);
    vertices.set_name("vertices");
    const auto& vgroup = g.vertex_group();
    const auto& vertex_fields = g.get_vertex_fields();
    for (auto& sf: vgroup) {
      std::vector<std::vector<flexible_type>> buffer;
      sf.get_reader()->read_rows(0, sf.size(), buffer);
      for (auto& row : buffer) {
        JSONNode value;
        sframe_row_to_json(vertex_fields, row, value);
        vertices.push_back(value);
      }
    }

    JSONNode edges(JSON_ARRAY);
    edges.set_name("edges");
    const auto& edge_fields = g.get_edge_fields();
    sframe sf = g.get_edges();
    std::vector<std::vector<flexible_type>> buffer;
    sf.get_reader()->read_rows(0, sf.size(), buffer);
    for (auto& row : buffer) {
      JSONNode value;
      sframe_row_to_json(edge_fields, row, value);
      edges.push_back(value);
    }

    JSONNode everything;
    everything.set_name("graph");
    everything.push_back(vertices);
    everything.push_back(edges);
    fout << everything.write_formatted();
    if (!fout.good()) {
      log_and_throw_io_failure("Fail to write.");
    }
    fout.close();
  }

  void save_sgraph_to_csv(const sgraph& g, std::string targetdir) {
    switch (fileio::get_file_status(targetdir)) {
     case fileio::file_status::MISSING:
       if (!fileio::create_directory(targetdir)) {
          log_and_throw_io_failure("Unable to create directory.");
       }
       break;
     case fileio::file_status::DIRECTORY:
       break;
     case fileio::file_status::REGULAR_FILE:
       log_and_throw_io_failure("Cannot save to regular file. Must be a directory.");
       break;
    }
    // Write vertices
    sframe vertices = g.get_vertices();
    std::string vertex_file_name = targetdir + "/vertices.csv";
    general_ofstream v_fout(vertex_file_name);
    if (!v_fout.good()) {
      log_and_throw_io_failure("Fail to write.");
    }
    std::vector<std::string> vertex_fields = vertices.column_names();
    for (size_t i = 0; i < vertex_fields.size(); ++i) {
      v_fout << vertex_fields[i];
      if (i == vertices.num_columns()-1) {
        v_fout << "\n";
      } else {
        v_fout << ",";
      }
    }
    auto vertex_reader = vertices.get_reader();
    std::vector<std::vector<flexible_type>> buffer;
    size_t cnt = 0;
    size_t buflen = 512*1024;
    size_t bytes_written = 0;
    char* buf = new char[buflen]; // 512k buffer
    while (cnt < vertices.size()) {
      vertex_reader->read_rows(cnt, cnt +  DEFAULT_SARRAY_READER_BUFFER_SIZE, buffer);
      for (const auto& row : buffer) {
        bytes_written = sframe_row_to_csv(row, buf, buflen);
        if (bytes_written == buflen) {
          delete[] buf;
          v_fout.close();
          log_and_throw_io_failure("Row size exceeds max buffer.");
        }
        v_fout.write(buf, bytes_written);
      }
      cnt += buffer.size();
    }
    buffer.clear();
    cnt = 0;
    if (!v_fout.good()) {
      delete[] buf;
      log_and_throw_io_failure("Fail to write.");
    }
    v_fout.close();

    // Write edges
    sframe edges = g.get_edges();

    std::string edge_file_name = targetdir + "/edges.csv";
    general_ofstream e_fout(edge_file_name);
    if (!e_fout.good()) {
      log_and_throw_io_failure("Fail to write.");
    }

    std::vector<std::string> edge_fields = edges.column_names();
    for (size_t i = 0; i < edge_fields.size(); ++i) {
      e_fout << edge_fields[i];
      if (i == edges.num_columns()-1) {
        e_fout << "\n";
      } else {
        e_fout << ",";
      }
    }
    auto edge_reader = edges.get_reader();
    while (cnt < edges.size()) {
      edge_reader->read_rows(cnt, cnt +  DEFAULT_SARRAY_READER_BUFFER_SIZE, buffer);
      for (const auto& row : buffer) {
        bytes_written = sframe_row_to_csv(row, buf, buflen);
        if (bytes_written == buflen) {
          delete[] buf;
          e_fout.close();
          log_and_throw_io_failure("Row size exceeds max buffer.");
        }
        e_fout.write(buf, bytes_written);
      }
      cnt += buffer.size();
    }
    if (!e_fout.good()) {
      delete[] buf;
      log_and_throw_io_failure("Fail to write.");
    }
    delete[] buf;
    e_fout.close();
  }

}
