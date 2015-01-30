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
#include <cmath>
#include <unity/query_process/sort_impl.hpp>
#include <unity/query_process/sort.hpp>
#include <unity/query_process/le_sort.hpp>
#include <sframe/sframe_config.hpp>

namespace graphlab {

  std::shared_ptr<sframe> sort(
    std::shared_ptr<lazy_sframe> sframe_ptr,
    const std::vector<std::string>& sort_column_names,
    const std::vector<bool>& sort_orders) {

    log_func_entry();

    // get sort column indexes from column names and also check column types
    std::vector<size_t> sort_column_indexes(sort_column_names.size());
    std::vector<flex_type_enum> supported_types =
        {flex_type_enum::STRING, flex_type_enum::INTEGER, flex_type_enum::FLOAT,flex_type_enum::DATETIME};
    std::set<flex_type_enum> supported_type_set(supported_types.begin(), supported_types.end());

    for(size_t i = 0; i < sort_column_names.size(); i++) {
      sort_column_indexes[i] = sframe_ptr->column_index(sort_column_names[i]);
      auto col_type = sframe_ptr->column_type(sort_column_indexes[i]);

      if (supported_type_set.count(col_type) == 0) {
        log_and_throw("Only column with type 'int', 'float', 'string', and 'datetime' can be sorted. Column '" +
            sort_column_names[i] + "'' is type: " + flex_type_enum_to_name(col_type));
      }
    }

    // Estimate the size of the sframe so that we could decide number of
    // chunks.  To account for strings, we estimate each cell is 64 bytes.
    // I'd love to estimate better.
    size_t estimated_sframe_size = sframe_num_cells(sframe_ptr) * 64.0;
    size_t num_partitions = std::ceil((1.0 * estimated_sframe_size) / sframe_config::SFRAME_SORT_BUFFER_SIZE);

    // Make partitions small enough for each thread to (theoretically) sort at once
    num_partitions = num_partitions * thread::cpu_count();

    // If we have more partitions than this, we could run into open file
    // descriptor limits
    num_partitions = std::min<size_t>(num_partitions, SFRAME_SORT_MAX_SEGMENTS);
    DASSERT_TRUE(num_partitions > 0);

    // Shortcut -- if only one partition, do a in memory sort and we are done
    if (num_partitions <= thread::cpu_count()) {
      logstream(LOG_INFO) << "Sorting SFrame in memory" << std::endl;
      return sframe_sort_impl::sort_sframe_in_memory(sframe_ptr, sort_column_indexes, sort_orders);
    }

    // This is a collection of partition keys sorted in the required order.
    // Each key is a flex_list value that contains the spliting value for
    // each sort column. Together they defines the "cut line" for all rows in
    // the SFrame.
    std::vector<flexible_type> partition_keys;


    // Do a quantile sketch on the sort columns to figure out the "splitting" points
    // for the SFrame
    timer ti;
    bool all_sorted = sframe_sort_impl::get_partition_keys(
      sframe_ptr->select_columns(sort_column_names),
      sort_orders, num_partitions, // in parameters
      partition_keys);  // out parameters
    logstream(LOG_INFO) << "Pivot estimation step: " << ti.current_time() << std::endl;

    // In rare case all values in the SFrame are the same, so no need to sort
    if (all_sorted) return sframe_ptr->get_sframe_ptr();

    // scatter partition the sframe into multiple chunks, chunks are relatively
    // sorted, but each chunk is not sorted. The sorting of each chunk is delayed
    // until it is consumed. Each chunk is stored as one segment for a sarray.
    // The chunk stores a serailized version of key and value
    std::vector<size_t> partition_sizes;

    // In the case where all sort keys in a given partition are the same, then
    // there is no need to sort the partition. This information is derived from
    // scattering
    std::vector<bool> partition_sorted(num_partitions, true);
    ti.start();
    auto partition_array = sframe_sort_impl::scatter_partition(
      sframe_ptr, sort_column_indexes, sort_orders, partition_keys, partition_sizes, partition_sorted);
    logstream(LOG_INFO) << "Scatter step: " << ti.current_time() << std::endl;

    // return a lazy sframe_ptr that would emit the sorted data lazily
    auto lazy_sort = std::make_shared<le_sort>(
      partition_array, partition_sorted, partition_sizes, sort_column_indexes,
      sort_orders, sframe_ptr->column_names(), sframe_ptr->column_types());

    return lazy_sort->eager_sort();
  }
} // namespace graphlab
