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
#ifndef GRAPHLAB_SGRAPH_IO_HPP
#define GRAPHLAB_SGRAPH_IO_HPP

#include<string>

class sgraph;

namespace graphlab {

  /**
   * Write the content of the graph into a json file.
   */
  void save_sgraph_to_json(const sgraph& g,
                           std::string targetfile);

  /**
   * Write the content of the graph into a collection csv files under target directory.
   * The vertex data are saved to vertex-groupid-partitionid.csv and edge data
   * are saved to edge-groupid-partitionid.csv.
   */
  void save_sgraph_to_csv(const sgraph& g,
                          std::string targetdir);
}

#endif
