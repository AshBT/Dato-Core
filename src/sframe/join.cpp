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
#include <sframe/join.hpp>

namespace graphlab {

sframe join(sframe& sf_left, 
            sframe& sf_right,
            std::string join_type,
            const std::map<std::string,std::string> join_columns,
            size_t max_buffer_size) {
  // ***SANITY CHECKS 

  // check that each sframe is valid
  if(!sf_left.num_rows() || !sf_left.num_columns()) {
    log_and_throw("Current SFrame has nothing to join!");
  }

  if(!sf_right.num_rows() || !sf_right.num_columns()) {
    log_and_throw("Given SFrame has nothing to join!");
  }

  std::vector<size_t> left_join_positions;
  std::vector<size_t> right_join_positions;
  for(const auto &col_pair : join_columns) {
    // Check that all columns exist (in both sframes)
    // These will throw if not found
    left_join_positions.push_back(sf_left.column_index(col_pair.first));
    right_join_positions.push_back(sf_right.column_index(col_pair.second));
    
    // Each column must have matching types to compare effectively
    if(sf_left.column_type(left_join_positions.back()) !=
        sf_right.column_type(right_join_positions.back())) {
      log_and_throw("Columns " + col_pair.first + " and " + col_pair.second + 
          " does not have the same type in both SFrames.");
    }
  }
  
  // Figure out what join type we have to do
  boost::algorithm::to_lower(join_type);

  join_type_t in_join_type;
  if(join_type == "outer") {
    in_join_type = FULL_JOIN;
  } else if(join_type == "left") {
    in_join_type = LEFT_JOIN;
  } else if(join_type == "right") {
    in_join_type = RIGHT_JOIN;
  } else if(join_type == "inner") {
    in_join_type = INNER_JOIN;
  } else {
    log_and_throw("Invalid join type given!");
  }

  // execute join (perhaps multiplex algorithm based on something?)
  join_impl::hash_join_executor join_executor(sf_left,
                                              sf_right,
                                              left_join_positions,
                                              right_join_positions,
                                              in_join_type,
                                              max_buffer_size);

  return join_executor.grace_hash_join();
}

} // end of graphlab
