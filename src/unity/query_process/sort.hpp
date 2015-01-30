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
#ifndef GRAPHLAB_SFRAME_SORT_HPP
#define GRAPHLAB_SFRAME_SORT_HPP

#include <vector>
#include <unity/query_process/lazy_sframe.hpp>

namespace graphlab {

/**
 * Sort given SFrame.

 * The algorithm is like the following:
 *   - First do a quantile sketch over all sort columns and use the quantile sketch to
 *     figure out the partition keys that we will use to split the sframe rows into
 *     small chunks so that each chunk is realtively sorted. Each chunk is small enough
 *     so that we could sort in memory 
 *   - Scatter partition the sframe according to above partition keys. The resulting
 *     value is persisted. Each partition is stored as one segment in a sarray.
 *   - The sorting resulting is then lazily materialized through le_sort operator

 * There are a few optimizations along the way:
 *   - if all sorting keys are the same, then no need to sort
 *   - if the sframe is small enough to fit into memory, then we simply do a in
 *     memory sort
 *   - if some partitions of the sframe have the same sorting key, then that partition
 *     will not be sorted

 * \param sframe_ptr The sframe to be sorted
 * \param sort_column_names The columns to be sorted
 * \param sort_orders The ascending/descending order for each column to be sorted

 * \return The sorted lazy_sframe
**/
std::shared_ptr<sframe> sort(
    std::shared_ptr<lazy_sframe> sframe_ptr,
    const std::vector<std::string>& sort_column_names,
    const std::vector<bool>& sort_orders);

/**
* Estimate the total size of the sframe according to number of columns
* This may not be correct though.
* TODO: copied from join, we may need to do a better job of estimating by
* reading first n rows and do a estimation
**/
inline size_t sframe_num_cells(std::shared_ptr<lazy_sframe> sf) {
  return (sf->size() * sf->num_columns());
}
} // end of graphlab

#endif //GRAPHLAB_SFRAME_SORT_HPP
