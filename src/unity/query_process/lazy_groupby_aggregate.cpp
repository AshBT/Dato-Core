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
#include <string>
#include <vector>
#include <logger/logger.hpp>
#include <timer/timer.hpp>
#include <unity/query_process/lazy_sframe.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>
#include <sframe/group_aggregate_value.hpp>
#include <sframe/groupby_aggregate_impl.hpp>
#include <sframe/sframe_config.hpp>
#include <sframe/groupby_aggregate.hpp>
namespace graphlab {

sframe lazy_groupby_aggregate(const lazy_sframe& source,
      const std::vector<std::string>& keys,
      const std::vector<std::string>& output_column_names,
      const std::vector<std::pair<std::vector<std::string>,
                                  std::shared_ptr<group_aggregate_value>>>& groups,
      size_t max_buffer_size) {
  // first, sanity checks
  // check that group keys exist
  if (output_column_names.size() != groups.size()) {
    log_and_throw("There must be as many output columns as there are groups");
  }
  {
    // check that output column names are all unique, and do not intersect with
    // keys. Since empty values will be automatically assigned, we will skip
    // those.
    std::set<std::string> all_output_columns(keys.begin(), keys.end());
    size_t named_column_count = 0;
    for (auto s: output_column_names) {
      if (!s.empty()) {
        all_output_columns.insert(s);
        ++named_column_count;
      }
    }
    if (all_output_columns.size() != keys.size() + named_column_count) {
      log_and_throw("Output columns names are not unique");
    }
  }

  for (const auto& key: keys) {
    // check that the column name is valid
    if (!source.contains_column(key)) {
      log_and_throw("SFrame does not contain column " + key);
    }
  }

  // check that each group is valid
  for (const auto& group: groups) {
    // check that the column name is valid
    if (group.first.size() > 0) {
      for(unsigned int index = 0; index < group.first.size(); index++) {
        auto& col_name = group.first[index];
        if (!source.contains_column(col_name)) {
          log_and_throw("SFrame does not contain column " + col_name);
        }
        if(graphlab::registered_arg_functions.count(group.second->name()) != 0 && index > 0)
          continue;
        // check that the types are valid
        size_t column_number = source.column_index(col_name);
        if (!group.second->support_type(source.column_type(column_number))) {
          log_and_throw("Requested operation: " + group.second->name() +
                        " not supported on the type of column " + col_name);
        }
      }
    }
  }

  // key should not have repeated columns
  std::set<std::string> key_columns;
  std::set<std::string> group_columns;
  for (const auto& key: keys) key_columns.insert(key);
  for (const auto& group: groups) {
    for(auto& col_name : group.first) {
      group_columns.insert(col_name);
    }
  }
  if (key_columns.size() != keys.size()) {
      log_and_throw("Group by key cannot have repeated column names");
  }

  // ok. select out just the columns I care about
  // begin with the key columns
  std::vector<std::string> all_columns(key_columns.begin(), key_columns.end());
  // then all the group columns (as long as they are not also key columns)
  for (const auto& group_column: group_columns) {
    if (group_column != "" && key_columns.count(group_column) == 0) {
      all_columns.push_back(group_column);
    }
  }
  std::shared_ptr<lazy_sframe> frame_with_relevant_cols = source.select_columns(all_columns);

  // prepare the output frame
  sframe output;
  std::vector<std::string> column_names;
  std::vector<flex_type_enum> column_types;
  // output frame has the key column name and types
  for (const auto& key: key_columns) {
    column_names.push_back(key);
    column_types.push_back(source.column_type(source.column_index(key)));
  }

  // then for each group, make a unique name and determine the output group type
  for (size_t i = 0;i < groups.size(); ++i) {
    const auto& group = groups[i];
    std::string candidate_name = output_column_names[i];
    if (candidate_name.empty()) {

      std::string root_candidate_name;
      if(graphlab::registered_arg_functions.count(group.second->name()) == 0) {

        for (auto& col_name: group.first) {
          if (root_candidate_name.empty()) {
            root_candidate_name += " of " + col_name;
          } else {
            root_candidate_name += "_" + col_name;
          }
        }

        root_candidate_name = group.second->name() + root_candidate_name;
      } else { 
        if(group.first.size() != 2)
          log_and_throw("arg functions takes exactly two arguments");
        root_candidate_name += group.first[1] + " for " + group.second->name() + " of " + group.first[0];
      }
      candidate_name = root_candidate_name;
      size_t ctr = 1;
      // keep trying to come up with a unique column name
      while (std::find(column_names.begin(),
                       column_names.end(),
                       candidate_name) != column_names.end()) {
        candidate_name = root_candidate_name + "." + std::to_string(ctr);
        ++ctr;
      }
    }
    column_names.push_back(candidate_name);

    std::vector<flex_type_enum> input_types;
    for(auto col_name : group.first) {
      input_types.push_back(source.column_type(source.column_index(col_name)));
    }
    auto output_type = group.second->set_input_types(input_types);
    column_types.push_back(output_type);
  }

  size_t nsegments = thread::cpu_count() * std::max<size_t>(1, log2(thread::cpu_count()));

  output.open_for_write(column_names,
                        column_types,
                        "",
                        nsegments);


  groupby_aggregate_impl::group_aggregate_container
      container(max_buffer_size, nsegments);

  // ok the input sframe (frame_with_relevant_cols) contains all the values
  // we care about. However, the challenge here is to figure out how the keys
  // and values line up. By construction, all the key columns come first.
  // which is good. But group columns can be pretty much anywhere.
  size_t num_keys = keys.size();
  for (const auto& group: groups) {
    std::vector<size_t> column_numbers;
    for(auto& col_name : group.first) {
      column_numbers.push_back(frame_with_relevant_cols->column_index(col_name));
    }

    container.define_group(column_numbers, group.second);
  }
  // done. now we can begin parallel processing

  // shuffle the rows based on the value of the key column.
  auto input_reader = frame_with_relevant_cols->get_iterator(thread::cpu_count(), false);
  graphlab::timer ti;
  logstream(LOG_INFO) << "Filling group container: " << std::endl;
  parallel_for (0, thread::cpu_count(),
                [&](size_t i) {
                  std::vector<std::vector<flexible_type> > rows;
                  while(1) {
                    rows = input_reader->get_next(i, graphlab::sframe_config::SFRAME_READ_BATCH_SIZE);
                    if (rows.size() == 0) break;
                    for (auto& row: rows) {
                      container.add(row, num_keys);
                    }
                  }
                });

  logstream(LOG_INFO) << "Group container filled in " << ti.current_time() << std::endl;
  logstream(LOG_INFO) << "Writing output: " << std::endl;
  ti.start();
  container.group_and_write(output);
  logstream(LOG_INFO) << "Output written in: " << ti.current_time() << std::endl;
  output.close();
  return output;
}

} // end of graphlab
