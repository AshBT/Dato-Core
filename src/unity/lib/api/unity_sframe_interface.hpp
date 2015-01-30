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
#ifndef GRAPHLAB_UNITY_SFRAME_INTERFACE_HPP
#define GRAPHLAB_UNITY_SFRAME_INTERFACE_HPP
#include <memory>
#include <vector>
#include <string>
#include <flexible_type/flexible_type.hpp>
#include <sframe/dataframe.hpp>
#include <unity/lib/options_map.hpp>
#include <cppipc/magic_macros.hpp>
#include <unity/lib/api/unity_sarray_interface.hpp>

namespace graphlab {

class unity_sframe_base;
typedef std::map<std::string, flex_type_enum> str_flex_type_map;
typedef std::map<std::string, flexible_type> csv_parsing_config_map;
typedef std::map<std::string, std::string> string_map;
typedef std::map<std::string, std::shared_ptr<unity_sarray_base>> csv_parsing_errors;

GENERATE_INTERFACE_AND_PROXY(unity_sframe_base, unity_sframe_proxy,
      (std::shared_ptr<unity_sframe_base>, clone, )
      (void, construct_from_dataframe, (const dataframe_t&))
      (void, construct_from_sframe_index, (std::string))
      (csv_parsing_errors, construct_from_csvs, (std::string)(csv_parsing_config_map)(str_flex_type_map))
      (void, clear, )
      (size_t, size, )
      (std::shared_ptr<unity_sarray_base>, transform, (const std::string&)(flex_type_enum)(bool)(int))
      (std::shared_ptr<unity_sarray_base>, transform_native, (const function_closure_info&)(flex_type_enum)(bool)(int))
      (std::shared_ptr<unity_sframe_base>, flat_map, (const std::string&)(std::vector<std::string>)
                                     (std::vector<flex_type_enum>)(bool)(int))
      (void, save_frame, (std::string) )
      (size_t, num_columns, )
      (std::vector<flex_type_enum>, dtype, )
      (std::vector<std::string>, column_names, )
      (std::shared_ptr<unity_sframe_base>, head, (size_t))
      (std::shared_ptr<unity_sframe_base>, tail, (size_t))
      (dataframe_t, _head, (size_t))
      (dataframe_t, _tail, (size_t))
      (std::shared_ptr<unity_sframe_base>, logical_filter, (std::shared_ptr<unity_sarray_base>))
      (std::shared_ptr<unity_sframe_base>, select_columns, (const std::vector<std::string>&))
      (std::shared_ptr<unity_sarray_base>, select_column, (const std::string&))
      (void, add_column, (std::shared_ptr<unity_sarray_base >)(const std::string&))
      (void, add_columns, (std::list<std::shared_ptr<unity_sarray_base >>)(std::vector<std::string>))
      (void, set_column_name, (size_t)(std::string))
      (void, remove_column, (size_t))
      (void, swap_columns, (size_t)(size_t))
      (void, begin_iterator, )
      (std::vector<std::vector<flexible_type>>, iterator_get_next, (size_t))
      (void, save_as_csv, (const std::string&)(csv_parsing_config_map))
      (std::shared_ptr<unity_sframe_base>, sample, (float)(int))
      (std::list<std::shared_ptr<unity_sframe_base>>, random_split, (float)(int))
      (std::shared_ptr<unity_sframe_base>, group, (std::string))
      (std::shared_ptr<unity_sframe_base>, groupby_aggregate, (const std::vector<std::string>&)
                                              (const std::vector<std::vector<std::string>>&)
                                              (const std::vector<std::string>&)
                                              (const std::vector<std::string>&))
      (std::shared_ptr<unity_sframe_base>, append, (std::shared_ptr<unity_sframe_base>))
      (void, materialize, )
      (bool, is_materialized, )
      (bool, has_size, )
      (std::shared_ptr<unity_sframe_base>, join, (std::shared_ptr<unity_sframe_base>)(const std::string)(string_map))
      (std::shared_ptr<unity_sframe_base>, sort, (const std::vector<std::string>&)(const std::vector<int>&))
      (std::shared_ptr<unity_sarray_base>, pack_columns, (const std::vector<std::string>&)(const std::vector<std::string>&)(flex_type_enum)(const flexible_type&))
      (std::shared_ptr<unity_sframe_base>, stack,  (const std::string&)(const std::vector<std::string>&)(const std::vector<flex_type_enum>&)(bool))
      (std::shared_ptr<unity_sframe_base>, copy_range, (size_t)(size_t)(size_t))
      (std::list<std::shared_ptr<unity_sframe_base>>, drop_missing_values, (const std::vector<std::string>&)(bool)(bool))
      (dataframe_t, to_dataframe, )
      (void, delete_on_close, )
    )
} // namespace graphlab
#endif // GRAPHLAB_UNITY_SFRAME_INTERFACE_HPP
