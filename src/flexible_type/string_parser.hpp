/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_STRING_PARSER_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_STRING_PARSER_HPP
#include <boost/algorithm/string.hpp>
#include <boost/spirit/include/qi.hpp>
#include <flexible_type/string_escape.hpp>

/*
 * Must of this is obtained from 
 * http://boost-spirit.com/home/articles/qi-example/creating-your-own-parser-component-for-spirit-qi/
 */

namespace parser_impl { 

/*
 * \internal
 * The string parsing configuration.
 *
 */
struct parser_config {
  // If any of these character occurs outside of quoted string, 
  // the string will be terminated
  std::string restrictions;
  // If the delimiter string is seen anywhere outside of a quoted string,
  // the string will be terminated.
  std::string delimiter;
  // The character to use for an escape character
  char escape_char = '\\';
  /* whether double quotes inside of a quote are treated as a single quote.
   * i.e. """hello""" => \"hello\"
   */
  char double_quote = true;
};

BOOST_SPIRIT_TERMINAL_EX(restricted_string); 

} // namespace parser_impl 

namespace boost { 
namespace spirit {


template <typename T1>
struct use_terminal<qi::domain, 
    terminal_ex<parser_impl::tag::restricted_string, fusion::vector1<T1> > >
  : mpl::true_ {};


} } // namespace spirit, boost

namespace parser_impl {

/*
 * \internal
 * This class defines a string parser which allows the parser writer to define
 * a list of characters which are not permitted in unquoted strings. Quoted 
 * strings have no restrictions on what characters they can contain.
 * Usage:
 * \code
 *   parser_impl::parser_config config;
 *   config.[set stuff up]
 *   rule = parser_impl::restricted_string(config);
 * \endcode
 */
struct string_parser
    : boost::spirit::qi::primitive_parser<string_parser> {
  // Define the attribute type exposed by this parser component
  template <typename Context, typename Iterator>
  struct attribute {
    typedef std::string type;
  };

  parser_config config;

  bool has_delimiter = false;
  char delimiter_first_char;
  bool delimiter_is_singlechar = false;

  string_parser(){}
  string_parser(parser_config config):config(config) {
    has_delimiter = config.delimiter.length() > 0;
    delimiter_is_singlechar = config.delimiter.length() == 1;
    if (has_delimiter) delimiter_first_char = config.delimiter[0];
  }

  enum class tokenizer_state {
    START_FIELD, IN_FIELD, IN_QUOTED_FIELD,
  };

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
#define PUSH_CHAR(c) ret = ret + c; escape_sequence = (c == config.escape_char);

// insert a character into the field buffer. resizing it if necessary

  // This function is called during the actual parsing process
  template <typename Iterator, typename Context, typename Skipper, typename Attribute>
  bool parse(Iterator& first, Iterator const& last, 
             Context&, Skipper const& skipper, Attribute& attr) const {
    boost::spirit::qi::skip_over(first, last, skipper);
    Iterator cur = first;
    std::string ret;
    const char* delimiter_begin = config.delimiter.c_str();
    const char* delimiter_end = delimiter_begin + config.delimiter.length();

    tokenizer_state state = tokenizer_state::START_FIELD; 
    bool keep_parsing = true;
    char quote_char = 0;
    // this is set to true for the character immediately after an escape character
    // and false all other times
    bool escape_sequence = false;
    while(keep_parsing && cur != last) {
      // since escape_sequence can only be true for one character after it is
      // set to true. I need a flag here. if reset_escape_sequence is true, the
      // at the end of the loop, I clear escape_sequence
      bool reset_escape_sequence = escape_sequence;

      // Next character in file
      char c = *cur;
      if(state != tokenizer_state::IN_QUOTED_FIELD && 
         config.restrictions.find(c) != std::string::npos) break;

      bool is_delimiter = 
          // current state is not in a quoted field since delimiters in quoted 
          // fields are fine.
          (state != tokenizer_state::IN_QUOTED_FIELD) &&
          // and there is a delimiter
          has_delimiter && 
          // and current character matches first character of delimiter
          // and delimiter is either a single character, or we need to do a 
          // more expensive test.
          delimiter_first_char == c &&
           (delimiter_is_singlechar || 
            test_is_delimiter(cur, last, delimiter_begin, delimiter_end));

      if (is_delimiter) break;

      ++cur;
      switch(state) {
       case tokenizer_state::START_FIELD:
         if (c == '\'' || c == '\"') {
           quote_char = c;
           state = tokenizer_state::IN_QUOTED_FIELD;
         } else {
           /* begin new unquoted field */
           PUSH_CHAR(c);
           state = tokenizer_state::IN_FIELD;
         }
         break;

       case tokenizer_state::IN_FIELD:
         /* normal character - save in field */
         PUSH_CHAR(c);
         break;

       case tokenizer_state::IN_QUOTED_FIELD:
         /* in quoted field */
         if (c == quote_char && !escape_sequence) {
           if (c == '\"' && config.double_quote) {
             /* doublequote; " represented by "" */
             // look ahead one character
             if (cur + 1 < last && *cur == quote_char) {
               PUSH_CHAR(c);
               ++cur;
               break;
             }
           }
           // we are done.
           keep_parsing = false;
         }
         else {
           /* normal character - save in field */
           PUSH_CHAR(c);
         }
         break;
      }
      if (reset_escape_sequence) escape_sequence = false;
    }
    if (cur == first) return false;
    else {
      if (!quote_char) boost::algorithm::trim_right(ret);
      graphlab::unescape_string(ret, config.escape_char);
      attr = std::move(ret);
      first = cur;
    }
    return true;
  }

// This function is called during error handling to create
// a human readable string for the error context.
  template <typename Context>
  boost::spirit::info what(Context&) const {
    return boost::spirit::info("string_parser");
  }
};
} // namespace parser_impl

namespace boost { 
namespace spirit { 
namespace qi {

// This is the factory function object invoked in order to create
// an instance of our iter_pos_parser.
template <typename Modifiers, typename T1>
struct make_primitive<terminal_ex<parser_impl::tag::restricted_string, fusion::vector1<T1>>, Modifiers> {
    typedef parser_impl::string_parser result_type;

    template <typename Terminal>
    result_type operator()(const Terminal& term, unused_type) const {
        return result_type(fusion::at_c<0>(term.args));
    }
};

}}} // namespace qi, spirit, boost

#undef PUSH_CHAR

#endif
