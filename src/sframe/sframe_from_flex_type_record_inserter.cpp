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
#include <memory>
#include <random/random.hpp>
#include <sframe/sframe_from_flex_type_record_inserter.hpp>

namespace graphlab {

sframe_from_flex_type_record_inserter::
    sframe_from_flex_type_record_inserter(const flexible_type_registry& registry, 
                                          size_t num_segments):
    registry(registry), segment_sizes(num_segments, 0), num_segments(num_segments) {
}

/**
 * Inserts a row into a dataframe, creating new columns if necessary.
 */
void sframe_from_flex_type_record_inserter::
    insert(const flexible_type_record& record, size_t segmentid) {
  if (segmentid == (size_t)(-1)) {
    segmentid = random::fast_uniform<size_t>(0, num_segments - 1);
  }
  if (segmentid >= num_segments) {
    log_and_throw(std::string("Invalid segment id"));
  }
  // number of column entries inserted
  size_t num_inserted = 0;
  for (const auto& field : record.fields()) {
    if (field_to_column_index.count(field.tag())) {
      // insert into the column if the column was found
      size_t column_id = field_to_column_index.at(field.tag()); 
      auto& target = writers[column_id];
      *(target.output_iterators[segmentid]) = field;
      ++(target.output_iterators[segmentid]);
      ++(target.segment_sizes[segmentid]);
      ++num_inserted;
    } else {
      // we need to add a new column
      // check if it is in the registry
      if (registry.get_field_type(field.tag()).first) {
        // yes it is in the registry
        field_id_type field_id = field.tag();
        std::string fieldname = registry.get_field_name(field_id);
        // create a new writer
        write_target new_column;
        new_column.writer = std::make_shared<sarray<flexible_type> >();
        new_column.writer->open_for_write(num_segments);
        // set the column name and the column type
        new_column.name = fieldname;
        new_column.segment_sizes.resize(num_segments, 0);
        new_column.writer->set_metadata("name", fieldname);
        new_column.writer->set_type(registry.get_field_type(field_id).second);

        // extract all the output iterators
        for (size_t i = 0;i < new_column.writer->num_segments(); ++i) {
          new_column.output_iterators.push_back(
              new_column.writer->get_output_iterator(i));
        }

        // pad the column with UNDEFINED for all the rows this column missed
        // for each segment
        for (size_t i = 0;i < new_column.writer->num_segments(); ++i) {
          flexible_type undefined_value(flex_type_enum::UNDEFINED);
          // for each row
          for (size_t j = 0; j < segment_sizes[i]; ++j) {
            (*new_column.output_iterators[i]) = undefined_value;
            ++(new_column.output_iterators[i]);
          }
          new_column.segment_sizes[i] = segment_sizes[i];
        }

        // store the new value
        *(new_column.output_iterators[segmentid]) = field;
        ++(new_column.output_iterators[segmentid]);
        ++(new_column.segment_sizes[segmentid]);

        // store the writer, and update the field_to_column_index
        writers.push_back(new_column);
        field_to_column_index[field_id] = writers.size() - 1;
        ++num_inserted;
      }
    }
  }
  // increment the global segment sizes
  ++(segment_sizes[segmentid]);

  // check if a value was inserted to every column
  if (num_inserted < writers.size()) {
    // we did not get all the fields.
    // pad the rest with missing values
    for (size_t i = 0;i < writers.size(); ++i) {
      if (writers[i].segment_sizes[segmentid] != segment_sizes[segmentid]) {
        *(writers[i].output_iterators[segmentid]) = 
            flexible_type(flex_type_enum::UNDEFINED);
        ++(writers[i].output_iterators[segmentid]);
        ++(writers[i].segment_sizes[segmentid]);
      }
    }
  }
}

sframe sframe_from_flex_type_record_inserter::close_and_get_result() {
  // close all the writers. Accumulate all the column names and 
  // open SArrays 
  std::vector<std::shared_ptr<sarray<flexible_type> > > arrays;
  std::vector<std::string> column_names;

  for (size_t i = 0;i < writers.size(); ++i) {
    writers[i].writer->close();
    writers[i].output_iterators.clear();

    column_names.push_back(writers[i].name);
    arrays.push_back(writers[i].writer);
  }
  return sframe(arrays, column_names);
}
} // namespace graphlab
