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
/**
 * \file
 * CSV Parser as adapted from Pandas
 */
#include <vector>
#include <string>
#include <cstdlib>
#include <algorithm>
#include <boost/config/warning_disable.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <flexible_type/string_escape.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>

namespace graphlab {

csv_line_tokenizer::csv_line_tokenizer() {
  field_buffer.resize(1024);
}


bool csv_line_tokenizer::tokenize_line(const char* str, size_t len, 
                                       std::function<bool (std::string&, size_t)> fn) {
  return tokenize_line_impl(str, len, 
                            [&](const char* buf, size_t len)->bool {
                              std::string tmp(buf, len);
                              return fn(tmp, len);
                            },
                            [&](const char** buf, const char* bufend)->bool{
                              // now. this is actually quite annoying.
                              // This means I hit a '[' or a '{'.
                              const char* prevbuf = (*buf);
                              parser->general_flexible_type_parse(buf, bufend - (*buf));
                              if ((*buf) != prevbuf) {
                                // take the parsed section and turn it into a string
                                std::string v = std::string(prevbuf, ((*buf) - prevbuf));
                                fn(v, v.length());
                                return true;
                              }
                              return false;
                            },
                            [](){}
                            );
}

bool csv_line_tokenizer::tokenize_line(const char* str, size_t len,
                                       std::vector<std::string>& output) {
  output.clear();
  return tokenize_line_impl(str, len,
                     [&](const char* buf, size_t len)->bool {
                       while(len > 0 && buf[len - 1] == ' ') len--;
                       output.emplace_back(buf, len);
                       return true;
                     },
                    [&](const char** buf, const char* bufend)->bool{
                      // now. this is actually quite annoying.
                      // This means I hit a '[' or a '{'.
                      const char* prevbuf = (*buf);
                      parser->general_flexible_type_parse(buf, bufend - (*buf));
                      if ((*buf) != prevbuf) {
                        // take the parsed section and turn it into a string
                        std::string v = std::string(prevbuf, ((*buf) - prevbuf));
                        output.push_back(std::move(v));
                        return true;
                      }
                      return false;
                    },
                     [](){}
                     );
}

size_t csv_line_tokenizer::tokenize_line(const char* str, size_t len,
                                         std::vector<flexible_type>& output,
                                         bool permit_undefined) {

  size_t ctr = 0;
  bool success = 
  tokenize_line_impl(str, len,
                     [&](const char* buf, size_t len)->bool {
                       if (ctr >= output.size()) {
                         // special handling for space ' ' delimiters
                         // If we exceeded the expected number of output columns
                         // but if the remaining characters are empty or 
                         // all whitespace, we do not fail. 
                         // But instead simply ignore.
                         if (delimiter_is_space) {
                           if (len == 0) return true;
                           for (size_t i = 0;i < len; ++i) {
                             if (buf[i] != ' ') return false;
                           }
                           return true;
                         }
                         return false;
                       }
                       flex_type_enum outtype = output[ctr].get_type();
                       // some types we permit UNDEFINED values when the
                       // length is 0. Except string. which will become an
                       // empty string.
                       if (len == 0 && permit_undefined &&
                           outtype != flex_type_enum::STRING) {
                         output[ctr].reset(flex_type_enum::UNDEFINED);
                         ++ctr;
                         return true;
                       } else {
                         bool success = parse_as(&buf, len, output[ctr]);
                         if (success) ++ctr;
                         return success;
                       }
                     },
                     [&](const char** buf, const char* bufend)->bool {
                       if (ctr >= output.size()) return false;
                       if (output[ctr].get_type() == flex_type_enum::STRING) {
                         // now. this is actually quite annoying.
                         // This means I hit a '[' or a '{'.
                         const char* prevbuf = (*buf);
                         parser->general_flexible_type_parse(buf, bufend - (*buf));
                         if ((*buf) != prevbuf) {
                           // take the parsed section and turn it into a string
                           std::string str = std::string(prevbuf, ((*buf) - prevbuf));
                           unescape_string(str, escape_char);
                           output[ctr] = std::move(str);
                           ++ctr;
                           return true;
                         }
                         return false;
                       }
                       bool success = parse_as(buf, bufend - (*buf), output[ctr]);
                       if (success) ++ctr;
                       return success;
                     },
                     [&](){
                       --ctr;
                     });

  if (!success) return 0;
  return ctr;
};


bool csv_line_tokenizer::parse_as(const char** buf, size_t len, flexible_type& out) {
  bool parse_success;
  // special handling for "NA"
  switch(out.get_type()) {
   case flex_type_enum::INTEGER:
     std::tie(out, parse_success) = parser->int_parse(buf, len);
     break;
   case flex_type_enum::FLOAT:
     std::tie(out, parse_success) = parser->double_parse(buf, len);
     break;
   case flex_type_enum::VECTOR:
     std::tie(out, parse_success) = parser->vector_parse(buf, len);
     break;
   case flex_type_enum::STRING:
     // STRING
     // right trim of the buffer. The
     // whitespace management of the parser already
     // takes care of the left trim
     while(len > 0 && (*buf)[len - 1] == ' ') len--;
     if (len >= 2 && (*buf)[len-1] == '\"' && (*buf)[0] == '\"') {
       out.mutable_get<flex_string>() = std::string((*buf)+1, len-2);
     } else {
       out.mutable_get<flex_string>() = std::string(*buf, len);
     }
     parse_success = true;
     unescape_string(out.mutable_get<flex_string>(), escape_char);
     break;
   case flex_type_enum::DICT:
     std::tie(out, parse_success) = parser->dict_parse(buf, len);
     break;
   case flex_type_enum::LIST:
     std::tie(out, parse_success) = parser->recursive_parse(buf, len);
     break;
   case flex_type_enum::UNDEFINED:
     std::tie(out, parse_success) = parser->general_flexible_type_parse(buf, len);
     break;
   default:
     parse_success = false;
     return false;
  }

  if (!na_values.empty()) {
    // process missing values
    if ((parse_success == false && out.get_type() != flex_type_enum::STRING) || 
        (parse_success == true && out.get_type() == flex_type_enum::STRING)) {
      while(len > 0 && (*buf)[len - 1] == ' ') len--;
      for (const auto& na_value: na_values) {
        if (na_value.length() == len && strncmp(*buf, na_value.c_str(), len) == 0) {
          out.reset(flex_type_enum::UNDEFINED);
          parse_success = true;
          break;
        }
      }
    }
  }
  return parse_success;
}



// reset the contents of the field buffer
#define BEGIN_FIELD() field_buffer_len = 0;

// insert a character into the field buffer. resizing it if necessary
#define PUSH_CHAR(c) if (field_buffer_len >= field_buffer.size()) field_buffer.resize(field_buffer.size() * 2); \
                     field_buffer[field_buffer_len++] = c;  \
                     escape_sequence = (c == escape_char);

// Finished parsing a field buffer. insert the token and reset the buffer
#define END_FIELD() if (!add_token(&(field_buffer[0]), field_buffer_len)) { good = false; keep_parsing = false; break; } \
                    field_buffer_len = 0;

// Reached the end of the line. stop
#define END_LINE() keep_parsing = false; break;

// current character matches first character of delimiter
// and delimiter is either a single character, or we need to do a 
// more expensive test.
#define DELIMITER_TEST() ((*buf) == delimiter_first_character) &&     \
      (delimiter_is_singlechar ||     \
       test_is_delimiter(buf, bufend, delimiter_begin, delimiter_end))

static inline bool test_is_delimiter(const char* c, const char* end, 
                                const char* delimiter, const char* delimiter_end) {
  // if I have more delimiter characters than the length of the string
  // quit.
  if (delimiter_end - delimiter > end - c) return false; 
  while (delimiter != delimiter_end) {
    if ((*c) != (*delimiter)) return false;
    ++c; ++delimiter;
  }
  return true;
}

template <typename Fn, typename Fn2, typename Fn3>
bool csv_line_tokenizer::tokenize_line_impl(const char* str, 
                                            size_t len,
                                            Fn add_token,
                                            Fn2 lookahead,
                                            Fn3 canceltoken) {
  ASSERT_MSG(parser, "Uninitialized tokenizer.");
  const char* buf = &(str[0]);
  const char* bufend= buf + len;
  const char* delimiter_begin = delimiter.c_str();
  const char* delimiter_end = delimiter_begin + delimiter.length();
  bool good = true;
  bool keep_parsing = true;
  // we switched state to start_field by encountering a delimiter
  bool start_field_with_delimiter_encountered = false;
  // this is set to true for the character immediately after an escape character
  // and false all other times
  bool escape_sequence = false;
  tokenizer_state state = tokenizer_state::START_FIELD; 
  field_buffer_len = 0;
  if (delimiter_is_new_line) {
    add_token(str, len);
    return true;
  }

  // this is adaptive. It can be either " or ' as we encounter it

  while(keep_parsing && buf != bufend) {
    // Next character in file
    bool is_delimiter = DELIMITER_TEST();
    // since escape_sequence can only be true for one character after it is
    // set to true. I need a flag here. if reset_escape_sequence is true, the
    // at the end of the loop, I clear escape_sequence
    bool reset_escape_sequence = escape_sequence;
    // skip to the last character of the delimiter
    if (is_delimiter) buf += delimiter.length() - 1;

    char c = *buf++;
    switch(state) {

     case tokenizer_state::START_FIELD:
       /* expecting field */
       // clear the flag
       if (c == quote_char) {
         // start quoted field
         start_field_with_delimiter_encountered = false;
         if (preserve_quoting == false) {
           BEGIN_FIELD();
           PUSH_CHAR(c);
           state = tokenizer_state::IN_QUOTED_FIELD;
         } else {
           BEGIN_FIELD();
           PUSH_CHAR(c);
           state = tokenizer_state::IN_FIELD;
         }
       } else if (c == ' ' && skip_initial_space) {
         // do nothing
       } else if (is_delimiter) {
         /* save empty field */
         start_field_with_delimiter_encountered = true;
         // advance buffer
         BEGIN_FIELD();
         END_FIELD();
         // otherwise if we are joining consecutive delimiters, do nothing
       } else if (c == comment_char) {
         // comment line
         start_field_with_delimiter_encountered = false;
         END_LINE();
       } else if (c == '[' || c == '{') {
         const char* prev = buf;
         start_field_with_delimiter_encountered = false;
         buf--; // shift back so we are on top of the bracketing character
         if (lookahead(&buf, bufend)) {
           // ok we have successfully parsed a field.
           // drop whitespace
           while(*buf == ' ') ++buf;
           if (buf == bufend) { 
             continue;
           } else if (DELIMITER_TEST()) { 
             start_field_with_delimiter_encountered = true;
             // skip past the delimiter
             buf += delimiter.length();
             continue;
           } else if(delimiter_is_space) {
             // the lookahead parser may absorb whitespace
             // so if the delimiter is a whitespace, we immediately
             // advance to the next field
             continue;
           } else {
             // bad. the lookahead picked up a whole field. But
             // we do not see a delimiter.
             // fail the lookahead
             canceltoken();
             buf = prev;
             goto REGULAR_CHARACTER;
           }
         } else {
           buf = prev;
           // interpret as a regular character
           goto REGULAR_CHARACTER;
         }
       } else {
REGULAR_CHARACTER:
         start_field_with_delimiter_encountered = false;
         /* begin new unquoted field */
         PUSH_CHAR(c);
         state = tokenizer_state::IN_FIELD;
       }
       break;

     case tokenizer_state::IN_FIELD:
       /* in unquoted field */
       if (is_delimiter) {
         // End of field. End of line not reached yet
         END_FIELD();
         // advance buffer
         start_field_with_delimiter_encountered = true;
         state = tokenizer_state::START_FIELD;
       } else if (c == comment_char) {
         // terminate this field
         END_FIELD();
         state = tokenizer_state::START_FIELD;
         END_LINE();
       } else {
         /* normal character - save in field */
         PUSH_CHAR(c);
       }
       break;

     case tokenizer_state::IN_QUOTED_FIELD:
       /* in quoted field */
       if (c == quote_char && !escape_sequence) {
         if (double_quote) {
           /* doublequote; " represented by "" */
           // look ahead one character
           if (buf + 1 < bufend && *buf == quote_char) {
             PUSH_CHAR(c);
             ++buf;
             break;
           }
         }
         /* end of quote part of field */
         PUSH_CHAR(c);
         state = tokenizer_state::IN_FIELD;
       }
       else {
         /* normal character - save in field */
         PUSH_CHAR(c);
       }
       break;
    }
    if (reset_escape_sequence) escape_sequence = false;
  }
  if (!good) return false;
  // cleanup 
  if (state != tokenizer_state::START_FIELD) {
    if (!add_token(&(field_buffer[0]), field_buffer_len)) { 
      return false;
    }
  } else {
    if (start_field_with_delimiter_encountered) {
      if (!add_token(NULL, 0)) { 
        return false;
      }
    }
  }
  return true;
}

void csv_line_tokenizer::init() {
  if (delimiter.length() == 0) {
    throw(std::string("Delimiter must be non-empty"));
  }
  parser.reset(new flexible_type_parser(delimiter, escape_char));

  delimiter_is_new_line = delimiter == "\n" || 
                          delimiter == "\r" || 
                          delimiter == "\r\n";

  delimiter_is_space = std::all_of(delimiter.begin(),
                                   delimiter.end(),
                                   [](char c)->bool {
                                     return std::isspace(c);
                                   });
  delimiter_first_character = delimiter[0];
  delimiter_is_singlechar = delimiter.length() == 1;
  
}

} // namespace graphlab
