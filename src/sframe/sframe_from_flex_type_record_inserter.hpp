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
#ifndef GRAPHLAB_UNITY_SFRAME_SFRAME_FROM_FLEX_TYPE_RECORD_INSERTER_HPP
#define GRAPHLAB_UNITY_SFRAME_SFRAME_FROM_FLEX_TYPE_RECORD_INSERTER_HPP
#include <vector>
#include <map>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sframe_constants.hpp>
#include <flexible_type/flexible_type_registry.hpp>
#include <flexible_type/flexible_type_record.hpp>
namespace graphlab {




/**
 * \ingroup unity
 * The sframe is a column-wise representation. This class provides row-wise 
 * insertion into an sframe based on consecutive insertions of a 
 * flexible_type_record. The inserter automatically creates new columns as
 * required; it will otherwise try to create the minimal set of columns 
 * necessary.
 */
class sframe_from_flex_type_record_inserter{
 private:
  const flexible_type_registry& registry;
  /**
   * The write target for a single column
   */
  struct write_target {
    std::string name;
    /// the writer we are writing to
    std::shared_ptr<sarray<flexible_type> > writer;
    /// for each segment, the current output iterator
    std::vector<sarray<flexible_type>::iterator> output_iterators;
    /// for each segment, the number of elements written to it
    std::vector<size_t> segment_sizes;
  };

  /// for each column the writer associated
  std::vector<write_target> writers;

  /// for each segment, the total number of rows written to it
  std::vector<size_t> segment_sizes;

  /// maps a field ID to an entry in writers
  std::map<field_id_type, size_t> field_to_column_index;

  /// The number of segments
  size_t num_segments;
 public:

  /**
   * Constructor. Creates an inserter object which inserts rows into
   * a new sframe, using the provided registry to figure out field-id
   * and field name mappings.
   */
  sframe_from_flex_type_record_inserter(const flexible_type_registry& registry,
                            size_t num_segments = SFRAME_DEFAULT_NUM_SEGMENTS);

  /**
   * Inserts a row into the SFrame, creating new columns as necessary.
   * Not concurrent. Writes into the segmentid provided. If segmentid
   * is (size_t)(-1), picks a random segment. Otherwise, throws an error
   * if it is an invalid segment.
   */
  void insert(const flexible_type_record& record, 
              size_t segmentid = (size_t)(-1));

  /**
   * Stops all insertions and returns the resulting SFrame.
   */
  sframe close_and_get_result();
};



}; // namespace graphlab

#endif // GRAPHLAB_UNITY_SFRAME_SFRAME_FROM_FLEX_TYPE_RECORD_INSERTER_HPP
