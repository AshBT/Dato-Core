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
#include <flexible_type/string_escape.hpp>
namespace graphlab {
void escape_string(const std::string& val, 
                   char escape_char, 
                   char quote_char, 
                   bool use_quote_char,
                   bool double_quote,
                   std::string& output, size_t& output_len) {
  // A maximum of 2 + 2 * input array size is needed.
  // (every character is escaped, and quotes on both end
  if (output.size() < 2 + 2 * val.size()) {
    output.resize(2 + 2 * val.size());
  }
  // add the left quote
  char* cur_out = &(output[0]);
  if (use_quote_char) (*cur_out++) = quote_char;
  // loop through the input string
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    switch(c) {
     case '\'':
       (*cur_out++) = escape_char;
       (*cur_out++) = '\'';
       break;
     case '\"':
       if (double_quote) {
         (*cur_out++) = '\"';
         (*cur_out++) = '\"';
       } else {
         (*cur_out++) = escape_char;
         (*cur_out++) = '\"';
       }
       break;
     case '\\':
       // do not "double escape" if we have \u or \x. i.e. \u does not emit \\u
       if (i < val.size() - 1 && (val[i+1] == 'u' || val[i+1] == 'x')) {
         (*cur_out++) = c;
       } else { 
         (*cur_out++) = escape_char;
         (*cur_out++) = '\\';
       }
       break;
     case '\t':
       (*cur_out++) = escape_char;
       (*cur_out++) = 't';
       break;
     case '\b':
       (*cur_out++) = escape_char;
       (*cur_out++) = 'b';
       break;
     case '\r':
       (*cur_out++) = escape_char;
       (*cur_out++) = 'r';
       break;
     case '\n':
       (*cur_out++) = escape_char;
       (*cur_out++) = 'n';
       break;
     case 0:
       (*cur_out++) = escape_char;
       (*cur_out++) = 0;
       break;
     default:
       (*cur_out++) = c;
    }
  }
  if (use_quote_char) (*cur_out++) = quote_char;
  size_t len = cur_out - &(output[0]);
  output_len = len;
}


void unescape_string(std::string& cal, char escape_char) {
  // to avoid allocating a new string, we are do this entirely in-place
  // This works because for all the escapes we have here, the output string
  // is shorter than the input.
  size_t in = 0;
  size_t out = 0;
  while(in != cal.length()) {
    if (cal[in]  == escape_char && in + 1 < cal.size()) {
      char echar = cal[in + 1];
      switch (echar) {
       case '\'':
         cal[out++] = '\'';
         ++in;
         break;
       case '\"':
         cal[out++] = '\"';
         ++in;
         break;
       case '\\':
         cal[out++] = '\\';
         ++in;
         break;
       case '/':
         cal[out++] = '/';
         ++in;
         break;
       case 't':
         cal[out++] = '\t';
         ++in;
         break;
       case 'b':
         cal[out++] = '\b';
         ++in;
         break;
       case 'r':
         cal[out++] = '\r';
         ++in;
         break;
       case 'n':
         cal[out++] = '\n';
         ++in;
         break;
       default:
         cal[out++] = cal[in]; // do nothing
      }
    } else {
      cal[out++] = cal[in]; 
    }
    ++in;
  }
  cal.resize(out);
}

}
