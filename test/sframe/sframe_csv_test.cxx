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
#include <iostream>
#include <typeinfo>
#include <boost/filesystem.hpp>
#include <sframe/sframe.hpp>
#include <sframe/algorithm.hpp>
#include <sframe/sframe_from_flex_type_record_inserter.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe/parallel_csv_parser.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <flexible_type/string_escape.hpp>
#include <random/random.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab;
struct csv_test {
  csv_line_tokenizer tokenizer;
  bool header = true;
  std::string file;
  std::vector<std::vector<flexible_type> > values;
  std::vector<std::pair<std::string, flex_type_enum> > types;
};

csv_test basic(std::string dlm=",", std::string line_ending="\n") {
  // tests a basic parse of one of every CSV parseable type
  csv_test ret;
  ret.tokenizer.delimiter = dlm;
  std::stringstream strm;
  strm << "float" << dlm << "int" << dlm << "str" << dlm << "vec" << dlm << "dict" << dlm << "rec" << line_ending
       << "1.1" << dlm << "1" << dlm << "one" << dlm << "[1,1,1]" << dlm << "{1:1,\"a\":\"a\"}" << dlm << "[a,a]" << line_ending
       << "2.2" << dlm << "2" << dlm << "two" << dlm << "[2,2,2]" << dlm << "{2:2,\"b\":\"b\"}" << dlm << "[b,b]" << line_ending
       << "3.3" << dlm << "3" << dlm << "three" << dlm << "[3,3,3]" << dlm << "{3:3,\"c\":\"c\"}" << dlm << "[c,c]" << line_ending;
  ret.file = strm.str();
  ret.values.push_back({1.1,1,"one",flex_vec{1,1,1},flex_dict{{1,1},{"a","a"}},flex_list{"a","a"}});
  ret.values.push_back({2.2,2,"two",flex_vec{2,2,2},flex_dict{{2,2},{"b","b"}},flex_list{"b","b"}});
  ret.values.push_back({3.3,3,"three",flex_vec{3,3,3},flex_dict{{3,3},{"c","c"}},flex_list{"c","c"}});

  ret.types = {{"float", flex_type_enum::FLOAT},
              {"int", flex_type_enum::INTEGER},
              {"str", flex_type_enum::STRING},
              {"vec", flex_type_enum::VECTOR},
              {"dict",flex_type_enum::DICT},
              {"rec",flex_type_enum::LIST}};
  return ret;
}

csv_test test_type_inference(std::string dlm=",", std::string line_ending="\n") {
  // tests a basic parse of one of every CSV parseable type
  csv_test ret;
  ret.tokenizer.delimiter = dlm;
  std::stringstream strm;
  strm << "float" << dlm << "int" << dlm << "str" << dlm << "vec" << dlm << "dict" << dlm << "rec" << line_ending
       << "1.1" << dlm << "1" << dlm << "one" << dlm << "[1,1,1]" << dlm << "{1:1,\"a\":\"a\"}" << dlm << "[a,a]" << line_ending
       << "2.2" << dlm << "2" << dlm << "two" << dlm << "[2,2,2]" << dlm << "{2:2,\"b\":\"b\"}" << dlm << "[b,b]" << line_ending
       << "3.3" << dlm << "3" << dlm << "three" << dlm << "[3,3,3]" << dlm << "{3:3,\"c\":\"c\"}" << dlm << "[c,c]" << line_ending;
  ret.file = strm.str();
  ret.values.push_back({1.1,1,"one",flex_vec{1,1,1},flex_dict{{1,1},{"a","a"}},flex_list{"a","a"}});
  ret.values.push_back({2.2,2,"two",flex_vec{2,2,2},flex_dict{{2,2},{"b","b"}},flex_list{"b","b"}});
  ret.values.push_back({3.3,3,"three",flex_vec{3,3,3},flex_dict{{3,3},{"c","c"}},flex_list{"c","c"}});

  ret.types = {{"float", flex_type_enum::UNDEFINED},
              {"int", flex_type_enum::UNDEFINED},
              {"str", flex_type_enum::UNDEFINED},
              {"vec", flex_type_enum::UNDEFINED},
              {"dict",flex_type_enum::UNDEFINED},
              {"rec",flex_type_enum::UNDEFINED}};
  return ret;
}


csv_test test_embedded_strings(std::string dlm=",") {
  // tests a basic parse of one of every CSV parseable type
  csv_test ret;
  ret.tokenizer.delimiter = dlm;
  std::stringstream strm;
  strm << "str" << dlm << "vec" << "\n"
       << "[abc" << dlm << "[1,1,1]" << "\n"
       << "cde]" << dlm << "[2,2,2]" << "\n"
       << "a[a]b" << dlm << "[3,3,3]" << "\n"
       << "\"[abc\"" << dlm << "[1,1,1]" << "\n"
       << "\"cde]\"" << dlm << "[2,2,2]" << "\n"
       << "\"a[a]b\"" << dlm << "[3,3,3]" << "\n";
  ret.file = strm.str();
  ret.values.push_back({"[abc", flex_vec{1,1,1}});
  ret.values.push_back({"cde]", flex_vec{2,2,2}});
  ret.values.push_back({"a[a]b", flex_vec{3,3,3}});
  ret.values.push_back({"[abc", flex_vec{1,1,1}});
  ret.values.push_back({"cde]", flex_vec{2,2,2}});
  ret.values.push_back({"a[a]b", flex_vec{3,3,3}});

  ret.types = {{"str", flex_type_enum::STRING},
               {"vec", flex_type_enum::VECTOR}};
  return ret;
}
csv_test interesting() {
  csv_test ret;
  std::stringstream strm;
  strm << "#this is a comment\n"
       << "float;int;vec;str #this is another comment\n"
       << "1.1 ;1;[1 2 3];hello\\\\\n"
       << "2.2;2; [4 5 6];\"wor;ld\"\n"
       << " 3.3; 3;[9 2];\"\"\"w\"\"\"\n" // double quote
       << "Pokemon  ;;; NA "; // missing last value
  ret.file = strm.str();
  ret.tokenizer.delimiter = ";";
  ret.tokenizer.double_quote = true;
  ret.tokenizer.na_values = {"NA","Pokemon"};

  ret.values.push_back({1.1,1,flex_vec{1,2,3},"hello\\"});
  ret.values.push_back({2.2,2,flex_vec{4,5,6},"wor;ld"});
  ret.values.push_back({3.3,3,flex_vec{9,2},"\"w\""});
  ret.values.push_back({flex_undefined(), flex_undefined(), flex_undefined(), flex_undefined()});

  ret.types = {{"float", flex_type_enum::FLOAT},
               {"int", flex_type_enum::INTEGER},
               {"vec", flex_type_enum::VECTOR},
               {"str", flex_type_enum::STRING}};
  return ret;
}

csv_test excess_white_space() {
  // tests a basic parse of one of every CSV parseable type
  csv_test ret;
  ret.tokenizer.delimiter = " "; 
  std::stringstream strm;
  std::string dlm = " ";
  // interestingly.... we do not correctly handle excess spaces in the header?
  strm << "float" << dlm << "int" << dlm << "str " << dlm << "vec   " << dlm << "dict" << dlm << "rec" << "\n" 
       << "  1.1" << dlm << " 1" << dlm << "one  " << dlm << "[1,1,1] " << dlm << " {1 : 1 , \"a\"  : \"a\"}   " << dlm << "[a,a]" << "\n" 
       << " 2.2" << dlm << "2" << dlm << "two" << dlm << "  [2,2,2]" << dlm << "{2:2,\"b\":\"b\"}" << dlm << "[b,b]" << "\n" 
       << "3.3" << dlm << "3" << dlm << "three" << dlm << "[3,3,3]" << dlm << " {3:3,  \"c\":\"c\"}" << dlm << "[c,c]  \t" << "\n";
  ret.file = strm.str();
  ret.values.push_back({1.1,1,"one",flex_vec{1,1,1},flex_dict{{1,1},{"a","a"}},flex_list{"a","a"}});
  ret.values.push_back({2.2,2,"two",flex_vec{2,2,2},flex_dict{{2,2},{"b","b"}},flex_list{"b","b"}});
  ret.values.push_back({3.3,3,"three",flex_vec{3,3,3},flex_dict{{3,3},{"c","c"}},flex_list{"c","c"}});

  ret.types = {{"float", flex_type_enum::FLOAT},
              {"int", flex_type_enum::INTEGER},
              {"str", flex_type_enum::STRING},
              {"vec", flex_type_enum::VECTOR},
              {"dict",flex_type_enum::DICT},
              {"rec",flex_type_enum::LIST}};
  return ret;
}

csv_test wierd_bracketing_thing() {
  csv_test ret;
  std::stringstream strm;
  strm << "str1 str2 str3\n"
       << "{    }    }\n"
       << "[    :    ]\n";
  ret.file = strm.str();
  ret.tokenizer.delimiter = " ";
  ret.tokenizer.double_quote = false;

  ret.values.push_back({"{","}","}"});
  ret.values.push_back({"[",":","]"});
  ret.values.push_back({flex_undefined(), flex_undefined(), flex_undefined(), flex_undefined()});

  ret.types = {{"float", flex_type_enum::FLOAT},
               {"int", flex_type_enum::INTEGER},
               {"vec", flex_type_enum::VECTOR},
               {"str", flex_type_enum::STRING}};
  return ret;
}


csv_test another_wierd_bracketing_thing_issue_1514() {
  csv_test ret;
  std::stringstream strm;
  strm << "X1\tX2\tX3\tX4\tX5\tX6\tX7\tX8\tX9\n"
       << "1\t{\t()\t{}\t{}\t(}\t})\t}\tdebugging\n"
       << "3\t--\t({})\t{()}\t{}\t({\t{)\t}\tdebugging\n";

  ret.file = strm.str();
  ret.tokenizer.delimiter = "\t";

  ret.values.push_back({"1","{","()","{}","{}","(}","})","}","debugging"});
  ret.values.push_back({"3","--","({})","{()}","{}","({","{)","}","debugging"});

  ret.types = {{"X1", flex_type_enum::STRING},
               {"X2", flex_type_enum::STRING},
               {"X3", flex_type_enum::STRING},
               {"X4", flex_type_enum::STRING},
               {"X5", flex_type_enum::STRING},
               {"X6", flex_type_enum::STRING},
               {"X7", flex_type_enum::STRING},
               {"X8", flex_type_enum::STRING},
               {"X9", flex_type_enum::STRING}};
  return ret;
}


csv_test string_integers() {
  csv_test ret;
  std::stringstream strm;
  strm << "int,str\n"
       << "1,\"1\"\n"
       << "2,\"2\"\n";
  ret.file = strm.str();
  ret.tokenizer.delimiter = ",";

  ret.values.push_back({1,"1"});
  ret.values.push_back({2,"2"});

  ret.types = {{"int", flex_type_enum::UNDEFINED},
               {"str", flex_type_enum::UNDEFINED}};
  return ret;
}



csv_test escape_parsing() {
  csv_test ret;
  std::stringstream strm;
  strm << "str1 str2\n"
       << "\\n  \"\\n\"\n"
       << "\\t  \\0abf\n"
       << "\\\"a  \"\\\"b\"\n"
       << "{\"a\":\"\\\"\"} [a,\"b\",\\\"c]\n";
  ret.file = strm.str();
  ret.tokenizer.delimiter = " ";

  ret.values.push_back({"\n","\n"});
  ret.values.push_back({"\t","\\0abf"});
  ret.values.push_back({"\"a","\"b"});
  ret.values.push_back({flex_dict({{"a","\""}}),flex_list({"a","b","\"c"})});

  ret.types = {{"str1", flex_type_enum::UNDEFINED},
               {"str2", flex_type_enum::UNDEFINED}};
  return ret;
}

csv_test escape_parsing_string_hint() {
  csv_test ret;
  std::stringstream strm;
  strm << "str1 str2\n"
       << "\\n  \"\\n\"\n"
       << "\\t  \\0abf\n";
  ret.file = strm.str();
  ret.tokenizer.delimiter = " ";

  ret.values.push_back({"\n","\n"});
  ret.values.push_back({"\t","\\0abf"});

  ret.types = {{"str1", flex_type_enum::STRING},
               {"str2", flex_type_enum::STRING}};
  return ret;
}


struct test_equality_visitor {
  template <typename T, typename U>
  void operator()(T& t, const U& u) const { TS_FAIL("type mismatch"); }
  void operator()(flex_image t, flex_image u) const { TS_FAIL("Cannot compare images"); }
  void operator()(flex_undefined t, flex_undefined u) const { }
  void operator()(flex_int t, flex_int u) const { TS_ASSERT_EQUALS(t, u); }
  void operator()(flex_float t, flex_float u) const { TS_ASSERT_DELTA(t, u, 1E-5); }
  void operator()(flex_string t, flex_string u) const { TS_ASSERT_EQUALS(t, u); }
  void operator()(flex_date_time t, flex_date_time u) const { 
    TS_ASSERT_EQUALS(t.first, u.first); 
    TS_ASSERT_EQUALS(t.second, u.second); 
  }
  void operator()(flex_vec t, flex_vec u) const { 
    TS_ASSERT_EQUALS(t.size(), u.size());
    for (size_t i = 0;i < t.size(); ++i) {
      TS_ASSERT_DELTA(t[i], u[i], 1E-5);
    }
  }
  void operator()(flex_list t, flex_list u) const { 
    TS_ASSERT_EQUALS(t.size(), u.size());
    for (size_t i = 0;i < t.size(); ++i) {
      t[i].apply_visitor(*this, u[i]);
    }
  }
  void operator()(flex_dict t, flex_dict u) const { 
    TS_ASSERT_EQUALS(t.size(), u.size());
    for (size_t i = 0;i < t.size(); ++i) {
      t[i].first.apply_visitor(*this, u[i].first);
      t[i].second.apply_visitor(*this, u[i].second);
    }
  }
};

class sframe_test : public CxxTest::TestSuite {
 public:
   // helper for the test below
   void evaluate(const csv_test& data) {
     std::string filename = get_temp_name() + ".csv";
     std::ofstream fout(filename);
     fout << data.file;
     fout.close();

     csv_line_tokenizer tokenizer = data.tokenizer;
     tokenizer.init();
     sframe frame;
     std::map<std::string, flex_type_enum> typelist(data.types.begin(), data.types.end());

     frame.init_from_csvs(filename,
                          tokenizer,
                          data.header,
                          false, // continue on failure
                          false,  // do not store errors
                          typelist);

     TS_ASSERT_EQUALS(frame.num_rows(), data.values.size());
     TS_ASSERT_EQUALS(frame.num_columns(), data.types.size());
     for (size_t i = 0;i < data.types.size(); ++i) {
       TS_ASSERT_EQUALS(frame.column_name(i), data.types[i].first);
       TS_ASSERT_EQUALS(frame.column_type(i), data.types[i].second);
     }

     std::vector<std::vector<flexible_type> > vals;
     graphlab::copy(frame, std::inserter(vals, vals.end()));

     TS_ASSERT_EQUALS(vals.size(), data.values.size());
     for (size_t i = 0;i < vals.size(); ++i) {
       TS_ASSERT_EQUALS(vals[i].size(), data.values[i].size());
       for (size_t j = 0; j < vals[i].size(); ++j) {
         vals[i][j].apply_visitor(test_equality_visitor(), data.values[i][j]);
       }
     }
   }

   void test_string_escaping() {
     std::string s = "hello";
     unescape_string(s, '\\');
     TS_ASSERT_EQUALS(s, "hello");

     s = "\\\"world\\\"";
     unescape_string(s, '\\');
     TS_ASSERT_EQUALS(s, "\"world\"");

     s = "\\\\world\\\\";
     unescape_string(s, '\\');
     TS_ASSERT_EQUALS(s, "\\world\\");

     s = "\\";
     unescape_string(s, '\\');
     TS_ASSERT_EQUALS(s, "\\");

     s = "\\\'\\\"\\\\\\/\\b\\r\\n";
     unescape_string(s, '\\');
     TS_ASSERT_EQUALS(s, "\'\"\\/\b\r\n");

   }

   void test_csvs() {
     evaluate(basic());
     evaluate(basic(",", "\r"));
     evaluate(basic(",", "\r\n"));
     evaluate(basic(" "));
     evaluate(basic(" ", "\r"));
     evaluate(basic(" ", "\r\n"));
     evaluate(basic(";"));
     evaluate(basic(";", "\r"));
     evaluate(basic(";", "\r\n"));
     evaluate(basic("::"));
     evaluate(basic("  "));
     evaluate(basic("\t\t"));
     evaluate(interesting());
     evaluate(excess_white_space());
     evaluate(test_embedded_strings(","));
     evaluate(test_embedded_strings(" "));
     evaluate(test_embedded_strings("\t"));
     evaluate(test_embedded_strings("\t\t"));
     evaluate(test_embedded_strings("  "));
     evaluate(test_embedded_strings("::"));
     evaluate(another_wierd_bracketing_thing_issue_1514());
     evaluate(test_type_inference(","));
     evaluate(string_integers());
     evaluate(escape_parsing());
     evaluate(escape_parsing_string_hint());
   }
};
