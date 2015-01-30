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
#ifndef GRAPHLAB_SFRAME_CSV_WRITER_HPP
#define GRAPHLAB_SFRAME_CSV_WRITER_HPP
#include <string>
#include <vector>
#include <iostream>
#include <flexible_type/flexible_type.hpp>
namespace graphlab {

class csv_writer {
 public:
  /**
   * The delimiter character to use to separate fields (Default ',')
   */
  std::string delimiter = ",";

  /**
   * The character to use to identify the beginning of a C escape sequence 
   * (Defualt '\'). i.e. "\n" will be converted to the '\n' character, "\\"
   * will be converted to "\", etc. Note that only the single character 
   * escapes are converted. unicode (\Unnnn), octal (\nnn), hexadecimal (\xnn)
   * are not interpreted.
   */
  char escape_char = '\\';

  /**
   * If set to true, pairs of quote characters in a quoted string 
   * are interpreted as a single quote (Default false).
   * For instance, if set to true, the 2nd field of the 2nd line is read as
   * \b "hello "world""
   * \verbatim
   * user, message
   * 123, "hello ""world"""
   * \endverbatim
   */
  bool double_quote = false; 

  /**
   * The quote character to use (Default '\"')
   */
  char quote_char = '\"';

  /**
   * Whether we should use the quote char to quote strings.
   */
  bool use_quote_char = true;

  /**
   * Whether the header is written
   */
  bool header = true;

  /**
   * Writes an array of strings as a row, verbatim without escaping /
   * modifications.  (only inserting delimiter characters).
   * Not safe to use in parallel.
   */
  void write_verbatim(std::ostream& out, const std::vector<std::string>& row);

  /**
   * Writes an array of values as a row, making the appropriate formatting 
   * changes. Not safe to use in parallel.
   */
  void write(std::ostream& out, const std::vector<flexible_type>& row);
  
  /**
   * Converts one value to a string
   */
  void csv_print(std::ostream& out, const flexible_type& val);
 
 private:


  std::string m_escape_buffer; // the output of escape_string 
  size_t m_escape_buffer_len = 0; // the length of the output in m_escape_buffer.
};
  

} // namespace graphlab
#endif
