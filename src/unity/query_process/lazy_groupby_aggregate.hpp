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
#ifndef GRAPHLAB_LAZY_SFRAME_GROUPBY_AGGREGATE_HPP
#define GRAPHLAB_LAZY_SFRAME_GROUPBY_AGGREGATE_HPP

#include <string>
#include <memory>
#include <vector>
#include <unity/query_process/lazy_sframe.hpp>
#include <sframe/group_aggregate_value.hpp>

namespace graphlab {

/**
 * Like groupby_aggregate, but reads from a lazy_sframe
 */
sframe lazy_groupby_aggregate(const lazy_sframe& source,
      const std::vector<std::string>& keys,
      const std::vector<std::string>& group_output_columns,
      const std::vector<std::pair<std::vector<std::string>,
                                  std::shared_ptr<group_aggregate_value>>>& groups,
      size_t max_buffer_size = 1024*1024);

}

#endif
