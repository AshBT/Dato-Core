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
#ifndef GRAPHLAB_UNITY_LIB_SFRAME_CSV_LINE_TOKENIZER_HPP
#define GRAPHLAB_UNITY_LIB_SFRAME_CSV_LINE_TOKENIZER_HPP
#include <vector>
#include <string>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <flexible_type/flexible_type.hpp>
#include <parallel/mutex.hpp>

namespace graphlab {

class flexible_type_parser;

/**
 * CSV Line Tokenizer
 * 
 * This CSV tokenizer is adapted from Pandas. To use, simply set 
 * the appropriate options inside the struct, and use one of the 
 * tokenize_line functions to parse a line inside a CSV file.
 *
 * \note This parser at the moment only handles the case where each row of
 * the CSV is on one line. It is in fact very possible that this is not the 
 * case. Pandas in particular permits line breaks inside of quoted strings,
 * and vectors, and that is quite problematic. A more general parser is not 
 * difficult to construct however, we just need another tokenize_line_impl 
 * function which also handles file buffer management. It might also be faster
 * since we can do direct buffer reads and we do not pay the cost of fstream
 * searching for the line break characters.
 */
struct csv_line_tokenizer {
  /**
   * If set to true, quotes inside a field will be preserved (Default false). 
   * i.e. if set to true, the 2nd entry in the following row will be read as
   * ""hello world"" with the quote characters.
   * \verbatim
   *   1,"hello world",5
   * \endverbatim
   */
  bool preserve_quoting = false;

  /**
   * The character to use to identify the beginning of a C escape sequence 
   * (Defualt '\'). i.e. "\n" will be converted to the '\n' character, "\\"
   * will be converted to "\", etc. Note that only the single character 
   * escapes are converted. unicode (\Unnnn), octal (\nnn), hexadecimal (\xnn)
   * are not interpreted.
   */
  char escape_char = '\\';

  /**
   * If set to true, initial spaces before fields are ignored (Default true).
   */
  bool skip_initial_space = true;


  /**
   * The delimiter character to use to separate fields (Default ",")
   */
  std::string delimiter = ",";

  /**
   * The character used to begin a comment (Default '#'). An occurance of 
   * this character outside of quoted strings will cause the parser to
   * ignore the remainder of the line.
   * \verbatim
   * # this is a 
   * # comment
   * user,name,rating
   * 123,hello,45
   * 312,chu, 21
   * 333,zzz, 3 # this is also a comment
   * 444,aaa, 51
   * \endverbatim
   */
  char comment_char = '#';

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
   * The strings which will be parsed as missing values
   */
  std::vector<std::string> na_values;

  /**
   * Constructor. Does nothing but set up internal buffers.
   */
  csv_line_tokenizer();

  /**
   * called before any parsing functions are used. Initializes the spirit parser.
   */
  void init();

  /**
   * Tokenize a single CSV line into seperate fields.
   * The output vector will be cleared, and each field will be inserted into
   * the output vector. Returns true on success and false on failure.
   *
   * \param str Pointer to string to tokenize
   * \param len Length of string to tokenize
   * \param output Output vector which will contain the result
   *
   * \returns true on success, false on failure.
   */
  bool tokenize_line(const char* str, size_t len,
                     std::vector<std::string>& output);

  /**
   * Tokenize a single CSV line into seperate fields, calling a callback
   * for each parsed token.
   *
   * The function is of the form:
   * \code
   * bool receive_token(const char* buffer, size_t len) {
   *   // add the first len bytes of the buffer as the parsed token
   *   // return true on success and false on failure.
   *
   *   // if this function returns false, the tokenize_line call will also
   *   // return false
   *
   *   // The buffer may be modified
   * }
   * \endcode
   * 
   * For instance, to insert the parsed tokens into an output vector, the
   * following code could be used:
   * 
   * \code
   * return tokenize_line(str, 
   *                 [&](const char* buf, size_t len)->bool {
   *                   output.emplace_back(buf, len);
   *                   return true;
   *                 });
   * \endcode
   *
   * \param str Pointer to line to tokenize
   * \param len Length of line to tokenize
   * \param fn Callback function which is called on every token
   *
   * \returns true on success, false on failure.
   */
  bool tokenize_line(const char* str, size_t len,
                     std::function<bool (std::string&, size_t)> fn);

  /**
   * Tokenizes a line directly into array of flexible_type and type specifiers.
   * This version of tokenize line is strict, requiring that the length of 
   * the output vector matches up exactly with the number of columns, and the
   * types of the flexible_type be fully specified.
   *
   * For instance:
   * If my input line is
   * \verbatim
   *     1, hello world, 2.0
   * \endverbatim
   *
   * then output vector must have 3 elements. 
   *
   * If the types of the 3 elements in the output vector are: 
   * [flex_type_enum::INTEGER, flex_type_enum::STRING, flex_type_enum::FLOAT]
   * then, they will be parsed as such emitting an output of 
   * [1, "hello world", 2.0].
   *
   * However, if the types of the 3 elements in the output vector are: 
   * [flex_type_enum::STRING, flex_type_enum::STRING, flex_type_enum::STRING]
   * then, the output will contain be ["1", "hello world", "2.0"].
   *
   * Type interpretation failures will produce an error.
   * For instance if the types are
   * [flex_type_enum::STRING, flex_type_enum::INTEGER, flex_type_enum::STRING],
   * since the second element cannot be correctly interpreted as an integer,
   * the tokenization will fail.
   *
   * The types current supported are:
   *  - flex_type_enum::INTEGER
   *  - flex_type_enum::FLOAT
   *  - flex_type_enum::STRING
   *  - flex_type_enum::VECTOR (a vector of numbers specified like [1 2 3]
   *                            but allowing separators to be spaces, commas(,)
   *                            or semicolons(;). The separator should not
   *                            match the CSV separator since the parsers are 
   *                            independent)
   *
   * The tokenizer will not modify the types of the output vector. However,
   * if permit_undefined is specified, the output type can be set to
   * flex_type_enum::UNDEFINED for an empty non-string field. For instance:
   *
   *
   * If my input line is
   * \verbatim
   *     1, , 2.0
   * \endverbatim
   * If I have type specifiers
   * [flex_type_enum::INTEGER, flex_type_enum::STRING, flex_type_enum::FLOAT]
   * This will be parsed as [1, "", 2.0] regardless of permit_undefined.
   *
   * However if I have type specifiers
   * [flex_type_enum::INTEGER, flex_type_enum::INTEGER, flex_type_enum::FLOAT]
   * and permit_undefined == false, This will be parsed as [1, 0, 2.0].
   *
   * And if I have type specifiers
   * [flex_type_enum::INTEGER, flex_type_enum::INTEGER, flex_type_enum::FLOAT]
   * and permit_undefined == true, This will be parsed as [1, UNDEFINED, 2.0].
   *
   * \param str Pointer to line to tokenize
   * \param len Length of line to tokenize
   * \param output The output vector which is of the same length as the number
   * of columns, and has all the types specified.
   * \param permit_undefined Allows output vector to repr
   *
   * \returns the number of output entries filled.
   */
  size_t tokenize_line(const char* str, size_t len,
                       std::vector<flexible_type>& output,
                       bool permit_undefined);


  /**
   * Parse the buf content into flexible_type.
   * The type of the flexible_type is determined by the out variable.
   */
  bool parse_as(const char** buf, size_t len, flexible_type& out);


 private:
  // internal buffer
  std::string field_buffer;
  // current length of internal buffer
  size_t field_buffer_len = 0;

  // the state of the tokenizer state machine
  enum class tokenizer_state {
    START_FIELD, IN_FIELD, IN_QUOTED_FIELD
  };

  /*
   *
   * \param str Pointer to line to tokenize
   * \param len Length of line to tokenize
   * \param add_token Callback function which is called on every successful token
   * \param lookahead_fn Callback function which is called to look ahead
   *                     for the end of the token when bracketing [], {} is
   *                     encountered. it is called with a (char**, len) and return
   *                     true/false on success/failure.
   * \param undotoken Callback function which is called to undo the previously
   *                  parsed token. Only called when lookahead succeeds, but later
   *                  parsing fails, thus requiring cancellation of the lookahead.
   */
  template <typename Fn, typename Fn2, typename Fn3>
  bool tokenize_line_impl(const char* str, size_t len, 
                          Fn add_token,
                          Fn2 lookahead,
                          Fn3 undotoken);

  std::shared_ptr<flexible_type_parser> parser;

  // some precomputed information about the delimiter so we avoid excess
  // string comparisons of the delimiter value
  bool delimiter_is_new_line = false;
  bool delimiter_is_space = false;
  char delimiter_first_character;
  bool delimiter_is_singlechar = false;
};
} // namespace graphlab

#endif
