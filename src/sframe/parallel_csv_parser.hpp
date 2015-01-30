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
#ifndef GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
#define GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
#include <string>
#include <vector>
#include <map>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/sframe_constants.hpp>
namespace graphlab {

std::istream& eol_safe_getline(std::istream& is, std::string& t);

std::map<std::string, std::shared_ptr<sarray<flexible_type>>> parse_csvs_to_sframe(
    const std::string& url,
    csv_line_tokenizer& tokenizer,
    bool use_header,
    bool continue_on_failure,
    bool store_errors,
    std::map<std::string, flex_type_enum> column_type_hints,
    size_t row_limit,
    sframe& frame,
    std::string frame_sidx_file = "");

}

#endif // GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
