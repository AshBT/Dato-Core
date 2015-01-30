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
// #define BOOST_SPIRIT_DEBUG
#include <boost/bind.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/qi_numeric.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_container.hpp>
#include <boost/spirit/include/phoenix_statement.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>

namespace graphlab {

using namespace boost::spirit;

// a parser which requires the dot
template <typename T>
struct strict_real_policies : qi::real_policies<T>
{
    static bool const expect_dot = true;
};
qi::real_parser< double, strict_real_policies<double> > real;

template <typename Iterator, typename SpaceType>
struct flexible_type_parser_impl: qi::grammar<Iterator, flexible_type(), SpaceType> {
  flexible_type_parser_impl(std::string delimiter = ",", char escape_char = '\\') : 
      flexible_type_parser_impl::base_type(root_parser), delimiter(delimiter) {
    using qi::long_;
    using qi::double_;
    using qi::char_;
    using qi::_1;
    using qi::debug;
    using qi::lexeme;
    using qi::no_skip;
    using boost::phoenix::push_back;
    using boost::phoenix::construct;
    using boost::phoenix::begin;
    using boost::phoenix::end;
    namespace phx = boost::phoenix;

    /*
     * A parser which parses strings, and stops at all unquoted delimiters
     */
    parser_impl::parser_config recursive_element_string_parser;
    recursive_element_string_parser.restrictions = ",{}[]";
    recursive_element_string_parser.escape_char = escape_char;
    recursive_element_string_parser.double_quote = true;

    /*
     * A parser which parses strings, and stops at all unquoted delimiters
     * AND spaces.
     */
    parser_impl::parser_config dictionary_element_string_parser;
    dictionary_element_string_parser.restrictions = " ,\t{}[]:;";
    dictionary_element_string_parser.escape_char = escape_char;
    dictionary_element_string_parser.double_quote = true;

    parser_impl::parser_config root_flex_string;
    // when the delimiter is just one character, using the restrictions is faster.
    if (delimiter.length() <= 1) root_flex_string.restrictions = delimiter;
    else root_flex_string.delimiter = delimiter;

    root_flex_string.escape_char = escape_char;
    root_flex_string.double_quote = true;

    string = 
        (parser_impl::restricted_string(root_flex_string)[_val = _1]);

    root_parser = (real[_val = _1]) | 
                  (long_[_val = _1]) | 
                  (vec [_val = _1]) | 
                  (recursive [_val = _1]) |
                  (dict [_val = _1]) |
                  (parser_impl::restricted_string(root_flex_string)[_val = _1]) |
                  eps[_val = FLEX_UNDEFINED];

    // same as the root parser, but modified so that the string
    // parser will stop the following characters: ",{}[]"
    recursive_element_parser = real[_val = _1] | 
        long_[_val = _1] | 
        vec [_val = _1]  | 
        recursive [_val = _1] |
        dict [_val = _1] |
        (parser_impl::restricted_string(recursive_element_string_parser)) [_val = _1] |
        eps[_val = FLEX_UNDEFINED];

                 
    // same as the root parser, but modified so that the string
    // parser will stop the following characters: ",\t{}[]:;"
    dictionary_element_parser = real[_val = _1] | 
        long_[_val = _1] | 
        vec [_val = _1]  | 
        recursive [_val = _1] |
        dict [_val = _1] |
        (parser_impl::restricted_string(dictionary_element_string_parser)) [_val = _1] |
        eps[_val = FLEX_UNDEFINED];

    // used by the recursive types to help parsing of things like 
    // [1abc, ...] where the 1 gets captured by the greedy parser as an integer.
    // but if we capture "abc" we need to turn the result to an integer
    // These work by essentially going
    // --> read a flex type
    // --> if we hit a delimiter, quit
    // --> otherwise fail and reread everything as a string, stopping at the 
    //     appropriate delimiters.
    
    robust_recursive_val_parser = 
        (recursive_element_parser[_val = _1] >> &(char_(',') | char_(']')))
        | (parser_impl::restricted_string(recursive_element_string_parser)) [_val = _1];

    robust_dict_key_parser = 
        (dictionary_element_parser[_val = _1] >> &(char_(':')))
        | (parser_impl::restricted_string(dictionary_element_string_parser)) [_val = _1];


    robust_dict_val_parser = 
        (dictionary_element_parser[_val = _1] >> &no_skip[char_(',') | char_('}') | char_(' ')])
        | (parser_impl::restricted_string(dictionary_element_string_parser)) [_val = _1];


    dict = '{' >> -(key_value_pair %  no_skip[(*qi::space >> char_(',')) | char_(' ')]) >> '}';
    key_value_pair = robust_dict_key_parser >> ':' >> robust_dict_val_parser;

    recursive = (char_('[') >> ']') | ('['  >>
        (robust_recursive_val_parser[push_back(_val, _1)] % char_(','))
         >> ']');
    vec = (char_('[') >> ']') | ('['  >>
        (double_[push_back(_val, _1)] % 
         no_skip[(*qi::space >> char_(',')) | (*qi::space >> char_(';')) | char_(' ')])
          >> ']');

//      BOOST_SPIRIT_DEBUG_NODE(dict);
//      BOOST_SPIRIT_DEBUG_NODE(recursive_element_parser);
//      BOOST_SPIRIT_DEBUG_NODE(dictionary_element_parser);
//      BOOST_SPIRIT_DEBUG_NODE(vec);
//      BOOST_SPIRIT_DEBUG_NODE(recursive);
//      BOOST_SPIRIT_DEBUG_NODE(robust_recursive_val_parser);
//      BOOST_SPIRIT_DEBUG_NODE(robust_dict_key_parser);
//      BOOST_SPIRIT_DEBUG_NODE(robust_dict_val_parser);
//      BOOST_SPIRIT_DEBUG_NODE(key_value_pair);
//      BOOST_SPIRIT_DEBUG_NODE(root_parser);
  }
  // the root parser
  qi::rule<Iterator, flexible_type(), SpaceType> root_parser; 
  // matches flexible type values in recursive values. This has stricter 
  // string separation rules. (since we want to accept "{a:b, c:d}", an
  // unquoted string cannot include the "," or " " characters (as well as many others).
  qi::rule<Iterator, flexible_type(), SpaceType> recursive_element_parser;
  qi::rule<Iterator, flexible_type(), SpaceType> dictionary_element_parser;
  qi::rule<Iterator, flexible_type(), SpaceType> robust_recursive_val_parser;
  qi::rule<Iterator, flexible_type(), SpaceType> robust_dict_key_parser;
  qi::rule<Iterator, flexible_type(), SpaceType> robust_dict_val_parser;
  // parses a dictionary
  qi::rule<Iterator, std::vector<std::pair<flexible_type, flexible_type>>(), SpaceType> dict;
  // parses a key-value pair
  qi::rule<Iterator, std::pair<flexible_type, flexible_type>(), SpaceType> key_value_pair;
  // parses the recursive type
  qi::rule<Iterator, std::vector<flexible_type>(), SpaceType> recursive;
  // parses a regular numeric vector
  qi::rule<Iterator, std::vector<double>(), SpaceType> vec;

  qi::rule<Iterator, flexible_type(), SpaceType> string;

  std::string delimiter;
};



flexible_type_parser::flexible_type_parser(std::string separator, char escape_char):
    parser(new flexible_type_parser_impl<const char*, 
           decltype(qi::standard::space)>(separator, escape_char)), 
    non_space_parser(new flexible_type_parser_impl<const char*, 
                     decltype(qi::eoi)>(separator, escape_char)), 
    m_delimiter_has_space(delimiter_has_space(parser->delimiter))
    { }

bool flexible_type_parser::delimiter_has_space(const std::string& separator) {
  return std::any_of(separator.begin(), separator.end(),
                     [](char c)->bool{
                       return c == ' ' || c == '\t';
                     });
}

std::pair<flexible_type, bool>
flexible_type_parser::general_flexible_type_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  const char* prevstr = (*str);
  const char* strend = prevstr + len;
  if (m_delimiter_has_space == false) {
    ret.second = qi::phrase_parse((*str), (*str) + len, 
                                  *parser,
                                  qi::standard::space,
                                  ret.first);
  } else {
    ret.second = qi::phrase_parse((*str), (*str) + len, 
                                  *non_space_parser,
                                  qi::eoi,
                                  ret.first);
  }
  if (ret.second) {
    // we claimed success. Therefore
    // ok. either we consumed the entire len, OR we are at a delimiter.
    
    // consumed entire length
    if ((*str) - prevstr >= len) return ret;
    bool at_delimiter = 
        parser_impl::string_parser::test_is_delimiter(*str, strend, parser->delimiter.c_str(),
                                         parser->delimiter.c_str() + parser->delimiter.size());
    if (at_delimiter) return ret;
    // otherwise this is bad. the parse was incomplete
    // make this a string then
    (*str) = prevstr;
    return string_parse(str, len);

  }
  return ret;
}


std::pair<flexible_type, bool>
flexible_type_parser::dict_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                parser->dict,
                                qi::standard::space,
                                ret.first);
  if (ret.second == false) ret.first.reset(flex_type_enum::DICT);
  return ret;
}


std::pair<flexible_type, bool>
flexible_type_parser::recursive_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                parser->recursive,
                                qi::standard::space,
                                ret.first);
  if (ret.second == false) ret.first.reset(flex_type_enum::LIST);
  return ret;
}


std::pair<flexible_type, bool>
flexible_type_parser::vector_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                parser->vec,
                                qi::standard::space,
                                ret.first);
  if (ret.second == false) ret.first.reset(flex_type_enum::VECTOR);
  return ret;
}

std::pair<flexible_type, bool>
flexible_type_parser::double_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  double dblval;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                boost::spirit::qi::double_,
                                qi::standard::space,
                                dblval);
  if (ret.second) ret.first = dblval;
  return ret;
}

std::pair<flexible_type, bool>
flexible_type_parser::int_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  flex_int intval;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                boost::spirit::qi::long_,
                                qi::standard::space,
                                intval);
  if (ret.second) ret.first = intval;
  return ret;
}


std::pair<flexible_type, bool>
flexible_type_parser::string_parse(const char** str, size_t len) {
  std::pair<flexible_type, bool> ret;
  ret.second = qi::phrase_parse((*str), (*str) + len, 
                                parser->string,
                                qi::standard::space,
                                ret.first);
  return ret;
}

} // namespace graphlab
