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
#include <sframe/csv_writer.hpp>
#include <flexible_type/string_escape.hpp>
#include <logger/logger.hpp>
namespace graphlab {

void csv_writer::write_verbatim(std::ostream& out,
                                const std::vector<std::string>& row) {
  for (size_t i = 0;i < row.size(); ++i) {
    out << row[i];
    // put a delimiter after every element except for the last element.
    if (i + 1 < row.size()) out << delimiter;
  }
  out << "\n";
}

void csv_writer::csv_print(std::ostream& out, const flexible_type& val) {
  switch(val.get_type()) {
    case flex_type_enum::INTEGER:
    case flex_type_enum::FLOAT:
    case flex_type_enum::VECTOR:
      out << std::string(val);
      break;
    case flex_type_enum::STRING:
      escape_string(val.get<flex_string>(), escape_char, 
                    quote_char, use_quote_char, double_quote, 
                    m_escape_buffer, m_escape_buffer_len);
      out.write(m_escape_buffer.c_str(), m_escape_buffer_len);
      break;
    case flex_type_enum::LIST:
      out << "[";
      for(size_t i = 0;i < val.get<flex_list>().size(); ++i) {
        csv_print(out, val.get<flex_list>()[i]);
        if (i + 1 < val.get<flex_list>().size()) out << ",";
      }
      out << "]";
      break;
    case flex_type_enum::DICT:
      out << "{";
      for(size_t i = 0;i < val.get<flex_dict>().size(); ++i) {
        csv_print(out, val.get<flex_dict>()[i].first);
        out << ":";
        csv_print(out, val.get<flex_dict>()[i].second);
        if (i + 1 < val.get<flex_dict>().size()) out << ",";
      }
      out << "}";
      break;
    case flex_type_enum::UNDEFINED:
    default:
      break;
  }
}

void csv_writer::write(std::ostream& out, 
                       const std::vector<flexible_type>& row) {
  for (size_t i = 0;i < row.size(); ++i) {
    csv_print(out, row[i]);
    // put a delimiter after every element except for the last element.
    if (i + 1 < row.size()) out << delimiter;
  }
  out << "\n";
}



} // namespace graphlab
