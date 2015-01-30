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
#ifndef GRAPHLAB_UNITY_SKETCH_INTERFACE_HPP
#define GRAPHLAB_UNITY_SKETCH_INTERFACE_HPP
#include <memory>
#include <vector>
#include <map>
#include <string>

#include <flexible_type/flexible_type.hpp>
#include <unity/lib/api/unity_sarray_interface.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {
class unity_sketch_base;

typedef std::pair<flexible_type, size_t> item_count;
typedef std::map<flexible_type, std::shared_ptr<unity_sketch_base>> sub_sketch_map;

GENERATE_INTERFACE_AND_PROXY(unity_sketch_base, unity_sketch_proxy,
    (void, construct_from_sarray, (std::shared_ptr<unity_sarray_base>)(bool)(const std::vector<flexible_type>&))
    (double, get_quantile, (double))
    (double, frequency_count, (flexible_type))
    (std::vector<item_count>, frequent_items, )
    (double, num_unique, )
    (double, mean, )
    (double, max, )
    (double, min, )
    (double, var, )
    (size_t, size, )
    (double, sum, )
    (size_t, num_undefined, )
    (bool, sketch_ready, )
    (size_t, num_elements_processed, )
    (std::shared_ptr<unity_sketch_base>, element_summary, )
    (std::shared_ptr<unity_sketch_base>, element_length_summary, )
    (std::shared_ptr<unity_sketch_base>, dict_key_summary, )
    (std::shared_ptr<unity_sketch_base>, dict_value_summary, )
    (sub_sketch_map, element_sub_sketch, (const std::vector<flexible_type>&))
    (void, cancel, )
)

} // namespace graphlab
#endif // GRAPHLAB_UNITY_GRAPH_INTERFACE_HPP
