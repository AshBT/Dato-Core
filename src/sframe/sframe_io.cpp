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
#include<sframe/sframe_io.hpp>
#include<unity/lib/json_util.hpp>
#include<cstdio>

namespace graphlab{

/**
 * Write a csv string of a vector of flexible_types (as a row in the sframe) to buffer.
 * Return the number of bytes written.
 */
size_t sframe_row_to_csv(const std::vector<flexible_type>& row, char* buf, size_t buflen) {
  flexible_type val;
  char* cur = buf;
  int space_remaining = buflen;
  int last_bytes_written = 0;
  for(size_t i = 0; (i < row.size()) && (space_remaining); ++i) {
    val = row[i];
    switch(val.get_type()) {
      case flex_type_enum::STRING:
        last_bytes_written = snprintf(cur, space_remaining, "\"%s\"", val.get<flex_string>().c_str());
        break;
      case flex_type_enum::FLOAT:
        last_bytes_written = snprintf(cur, space_remaining, "%f", val.get<flex_float>());
        break;
      case flex_type_enum::INTEGER:
        last_bytes_written = snprintf(cur, space_remaining, "%ld", val.get<flex_int>());
        break;
      case flex_type_enum::UNDEFINED:
        last_bytes_written = 0;
        break;
      default:
        last_bytes_written = snprintf(cur, space_remaining, "%s", ((std::string)val).c_str());
        break;
    }
    if (last_bytes_written < 0 || last_bytes_written >= space_remaining) {
      return buflen;
    } else {
      cur += last_bytes_written;
      space_remaining -= last_bytes_written;
      if (space_remaining) {
        if (i == row.size()-1) {
          *cur++ = '\n';
          --space_remaining;
        } else {
          *cur++ = ',';
          --space_remaining;
        }
      } else {
        break;
      }
    }
  }
  return (cur - buf);
}

/**
 * Write column_names and column_values (as a row in the sframe) to JSONNode.
 */
void sframe_row_to_json(const std::vector<std::string>& column_names,
                        const std::vector<flexible_type>& column_values,
                        JSONNode& node) {
  DASSERT_EQ(column_names.size(), column_values.size());
  for (size_t i = 0; i < column_names.size(); ++i) {
    node.push_back(flexible_type_to_json(column_values[i], column_names[i]));
  }
}

}
