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
#include <cxxtest/TestSuite.h>
#include <boost/range/combine.hpp>
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

using namespace graphlab;

const flexible_type None = FLEX_UNDEFINED;

class gl_sarray_test: public CxxTest::TestSuite {
  public:
    void test_constructor() {
      gl_sarray sa;
      _assert_sarray_equals(sa, {});

      gl_sarray sa2({1,2,3});
      _assert_sarray_equals(sa2, {1,2,3});
    }

    void test_from_const() {
      gl_sarray sa = gl_sarray::from_const(1, 3);
      _assert_sarray_equals(sa, {1,1,1});
      gl_sarray sb = gl_sarray::from_const("abc", 3);
      _assert_sarray_equals(sb, {"abc","abc","abc"});
      gl_sarray sc = gl_sarray::from_const(FLEX_UNDEFINED, 3);
      _assert_sarray_equals(sc, {FLEX_UNDEFINED, FLEX_UNDEFINED, FLEX_UNDEFINED});
      TS_ASSERT_EQUALS((int)sc.dtype(), (int)flex_type_enum::FLOAT);
    }

    void test_from_sequence() {
      gl_sarray sa = gl_sarray::from_sequence(1,4);
      _assert_sarray_equals(sa, {1,2,3});
    }

    void test_operators() {
      gl_sarray sa({1,2,3});
      gl_sarray ones({1,1,1});
      gl_sarray zeros({0,0,0});

      _assert_sarray_equals(sa + sa, {2,4,6});
      _assert_sarray_equals(sa - sa, {0,0,0});
      _assert_sarray_equals(sa * sa, {1,4,9});
      _assert_sarray_equals(sa / sa, {1,1,1});

      _assert_sarray_equals(sa + 2, {3,4,5});
      _assert_sarray_equals(sa - 2, {-1,0,1});
      _assert_sarray_equals(sa * 2, {2,4,6});
      _assert_sarray_equals(sa / 2.0, {.5,1,1.5});

      _assert_sarray_equals(sa > ones, {0,1,1});
      _assert_sarray_equals(sa < ones, {0,0,0});
      _assert_sarray_equals(sa >= ones, {1,1,1});
      _assert_sarray_equals(sa <= ones, {1,0,0});
      _assert_sarray_equals(sa == ones, {1,0,0});

      _assert_sarray_equals(sa > 1, {0,1,1});
      _assert_sarray_equals(sa < 1, {0,0,0});
      _assert_sarray_equals(sa >= 1, {1,1,1});
      _assert_sarray_equals(sa <= 1, {1,0,0});
      _assert_sarray_equals(sa == 1.0, {1,0,0});

      _assert_sarray_equals(sa & zeros, {0,0,0});
      _assert_sarray_equals(sa && zeros, {0,0,0});
      _assert_sarray_equals(sa | ones, {1,1,1});
      _assert_sarray_equals(sa || ones, {1,1,1});


      gl_sarray tmp(sa);
      _assert_sarray_equals(tmp += sa, {2,4,6}); tmp = sa;
      _assert_sarray_equals(tmp -= sa, {0,0,0}); tmp = sa;
      _assert_sarray_equals(tmp *= sa, {1,4,9}); tmp = sa;
      _assert_sarray_equals(tmp /= sa, {1,1,1}); tmp = sa;

      _assert_sarray_equals(tmp += 2, {3,4,5}); tmp = sa;
      _assert_sarray_equals(tmp -= 2, {-1,0,1}); tmp = sa;
      _assert_sarray_equals(tmp *= 2, {2,4,6}); tmp = sa;
      _assert_sarray_equals(tmp /= 2.0, {.5,1,1.5}); tmp = sa;
    }

    void test_head() {
      gl_sarray sa = gl_sarray::from_sequence(0, 10);
      _assert_sarray_equals(sa.head(5), {0,1,2,3,4});
      _assert_sarray_equals(sa.head(0), {});
      _assert_sarray_equals(sa.head(10), _to_vec(sa));
    }

    void test_tail() {
      gl_sarray sa = gl_sarray::from_sequence(0, 10);
      _assert_sarray_equals(sa.tail(5), {5,6,7,8,9});
      _assert_sarray_equals(sa.tail(0), {});
      _assert_sarray_equals(sa.tail(10), _to_vec(sa));
    }

    void test_astype() {
      gl_sarray sa({1,2,3});
      _assert_sarray_equals(sa.astype(flex_type_enum::FLOAT), {1., 2., 3.});
      _assert_sarray_equals(sa.astype(flex_type_enum::STRING), {"1", "2", "3"});
    }

    void test_sort() {
      gl_sarray sa({4,5,6,1,2,3});
      _assert_sarray_equals(sa.sort(true), {1,2,3,4,5,6});
      _assert_sarray_equals(sa.sort(false), {6,5,4,3,2,1});

      gl_sarray sa_str({"a", "b", "c", "d", "e", "f"});
      _assert_sarray_equals(sa_str.sort(true), {"a", "b", "c", "d", "e", "f"});
    }

    void test_max_min_sum_mean_std() {
      gl_sarray sa({1,2,3,1,2,3});
      TS_ASSERT_EQUALS(sa.min(), 1);
      TS_ASSERT_EQUALS(sa.max(), 3);
      TS_ASSERT_DELTA((double)sa.mean(), 2.0, 1E-6);
      TS_ASSERT_EQUALS(sa.sum(), 12);
      TS_ASSERT(std::abs<double>((double)(sa.std()-std::sqrt(4/6.0))) < 1e-6);
    }

    void test_any_all() {
      TS_ASSERT(gl_sarray({0,0,1}).any());
      TS_ASSERT(!gl_sarray({0,0,0}).any());
      TS_ASSERT(gl_sarray({1,1,1}).all());
      TS_ASSERT(!gl_sarray({0,1,1}).all());
    }

    void test_apply() {
      gl_sarray sa({1,2,3,4,5});
      _assert_sarray_equals(
          sa.apply([](const flexible_type& x) { return x * 2; }, flex_type_enum::INTEGER),
          {2,4,6,8,10});
    }

    void test_filter() {
      gl_sarray sa({1,2,3,4,5});
      _assert_sarray_equals(
          sa.filter([](const flexible_type& x) { return x % 2; }),
          {1,3,5});
    }
    void test_append() {
      auto sa = gl_sarray({1, 2, 3});
      auto sa2 = gl_sarray({4, 5, 6});
      _assert_sarray_equals(sa.append(sa2), {1,2,3,4,5,6});
    }

    void test_unique() {
      gl_sarray sa({1,1,1,2,2,3});
      _assert_sarray_equals(sa.unique().sort(), {1,2,3});
    }

    void test_sample() { 
      // This test does not check the sample fraction correctness
      // Even with seed, the answer could be non-deterministic across platform.
      gl_sarray sa({1,1,1,2,2,2,3,3,3,4,4,4});
      TS_ASSERT(sa.sample(0.1, 0).size() < 10);
      _assert_sarray_equals(sa.sample(0.2, 0), _to_vec(sa.sample(0.2,0)));


      auto  sa2 = gl_sarray::from_sequence(0,10);
      std::cout <<  sa2.sample(.3, 12345);
    }

    void test_nnz_num_missing() { 
      gl_sarray sa({1,2,3,None,None});
      TS_ASSERT_EQUALS(sa.nnz(), 3);
      TS_ASSERT_EQUALS(sa.num_missing(), 2);
    }

    void test_clip_lower_upper() {
      gl_sarray sa({1,2,3,4,5,6});
      _assert_sarray_equals(sa.clip(3,4), {3,3,3,4,4,4});
      _assert_sarray_equals(sa.clip_lower(3), {3,3,3,4,5,6});
      _assert_sarray_equals(sa.clip_upper(3), {1,2,3,3,3,3});
    }

    void test_dropna_fillna() { 
      gl_sarray sa({1,2,3,None,None});
      _assert_sarray_equals(sa.dropna(), {1,2,3});
      _assert_sarray_equals(sa.fillna(0), {1,2,3,0,0});
    }

    void test_topk_index() {
      gl_sarray sa({4,5,6,1,2,3});
      _assert_sarray_equals(sa.topk_index(3), {1,1,1,0,0,0});
    }

    void test_dict_trim_by_keys_values() { 
      typedef flex_dict dict;
      std::vector<flexible_type> array;
      array.push_back(dict{ {"A", 65}, {"a",97} });
      array.push_back(dict{ {"B", 66}, {"b",98} });
      array.push_back(dict{ {"C", 67}, {"c",99} });
      gl_sarray sa(array);

      _assert_sarray_equals(
          sa.dict_trim_by_keys({"a", "b","c"}, false), // include
          { dict{{"a",97}}, dict{{"b",98}}, dict{{"c",99}} });

      _assert_sarray_equals(
          sa.dict_trim_by_keys({"a", "b","c"}, true), // exclude 
          { dict{{"A",65}}, dict{{"B",66}}, dict{{"C",67}} });

      _assert_sarray_equals(
          sa.dict_trim_by_values(97, 99),
          { dict{{"a",97}}, dict{{"b",98}}, dict{{"c",99}} });
    }

    void test_dict_keys_values() {
      typedef flex_list list;
      typedef flex_dict dict;
      std::vector<flexible_type> array;
      array.push_back(dict{ {"A", 65}, {"a",97} });
      array.push_back(dict{ {"B", 66}, {"b",98} });
      array.push_back(dict{ {"C", 67}, {"c",99} });
      gl_sarray sa(array);

      _assert_sarray_equals(sa.dict_keys(), { list{"A", "a"}, list{"B", "b"}, list{"C", "c"} });
      _assert_sarray_equals(sa.dict_values(), { list{65,97}, list{66,98}, list{67,99} });
    }

    void test_has_any_all_keys() {
      typedef flex_dict dict;
      std::vector<flexible_type> array;
      array.push_back(dict{ {"A", 65}, {"a",97}, {"common", 0} });
      array.push_back(dict{ {"B", 66}, {"b",98}, {"common", 0} });
      array.push_back(dict{ {"C", 67}, {"c",99}, {"common", 1} });
      gl_sarray sa(array);
    }

    void test_count_words() {
      typedef flex_dict dict;
      gl_sarray sa({ "a", "b,b", "c,c,c"});
      _assert_sarray_equals(sa.count_words(), { dict{{"a",1}}, dict{{"b",2}}, dict{{"c",3}} });
    }

    void test_count_ngrams() {
      typedef flex_dict dict;
      gl_sarray sa({ "a", "b,b", "c,c,c"});
      _assert_sarray_equals(sa.count_ngrams(2), { dict{}, dict{{"b b",1}}, dict{{"c c",2}} });
    }

    void test_datetime() { 
      boost::posix_time::ptime t(boost::gregorian::date(2011, 1, 1));
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,1,1));
      auto x = (t - epoch).total_seconds();

      auto sa = gl_sarray({flex_date_time(x, 0)});
      std::cout <<  sa.datetime_to_str("%e %b %Y");

      auto  sa2 = gl_sarray({"20-Oct-2011 09:30:10 GMT-05:30"});
      std::cout <<  sa2.str_to_datetime("%d-%b-%Y %H:%M:%S %ZP");
    }

    void test_datetime_to_from_str() { 
      typedef flex_date_time date_time;
      gl_sarray sa{ date_time(0, 0), date_time(1, 0), date_time(2, 0) };
      _assert_sarray_equals(sa.datetime_to_str(),
          {"1970-01-01T00:00:00GMT+00", "1970-01-01T00:00:01GMT+00", "1970-01-01T00:00:02GMT+00"});
      _assert_sarray_equals(sa.datetime_to_str().str_to_datetime(),
          _to_vec(sa));
    }

    void test_item_length() {
      auto sa = gl_sarray({flex_dict{{"is_restaurant", 1}, {"is_electronics", 0}},
        flex_dict{{"is_restaurant", 1}, {"is_retail", 1}, {"is_electronics", 0}},
        flex_dict{{"is_restaurant", 0}, {"is_retail", 1}, {"is_electronics", 0}},
        flex_dict{{"is_restaurant", 0}},
        flex_dict{{"is_restaurant", 1}, {"is_electronics", 1}},
        FLEX_UNDEFINED});
      std::cout << sa.item_length();
    }

    void test_split_datetime() {
      // TODO fix datatime split: this test throws exception

      typedef flex_date_time date_time;
      gl_sarray sa{ date_time(0, 0), date_time(1, 0), date_time(2, 0) };
      gl_sframe sf = sa.split_datetime();
      _assert_sarray_equals(sf["X.year"], {1970, 1970, 1970});
      _assert_sarray_equals(sf["X.month"], {1, 1, 1});
      _assert_sarray_equals(sf["X.day"], {1, 1, 1});
      _assert_sarray_equals(sf["X.hour"], {0, 0, 0});
      _assert_sarray_equals(sf["X.minute"], {0, 0, 0});
      _assert_sarray_equals(sf["X.second"], {0, 1, 2});
    }

    void test_split_datetime2() {
      auto sa = gl_sarray({"20-Oct-2011", "10-Jan-2012"});
      auto date_sarray = sa.str_to_datetime("%d-%b-%Y");
      auto split_sf = date_sarray.split_datetime("", {"day","year"});
      std::cout << split_sf;
    }

    void test_unpack() {
      typedef flex_dict dict;
      std::vector<flexible_type> array;
      array.push_back(dict{ {"a", 0}, {"common", 0} });
      array.push_back(dict{ {"b", 1}, {"common", 1} });
      array.push_back(dict{ {"c", 2}, {"common", 2} });
      gl_sarray sa(array);
      gl_sframe sf = sa.unpack("X");
      TS_ASSERT_EQUALS(sf.num_columns(), 4);
      _assert_sarray_equals(sf["X.a"], {0, None, None});
      _assert_sarray_equals(sf["X.b"], {None, 1, None});
      _assert_sarray_equals(sf["X.c"], {None, None, 2});
      _assert_sarray_equals(sf["X.common"], {0, 1, 2});
    }

    void test_unpack2() {
      std::cout << "unpack2:\n\n";
      auto sa = gl_sarray({flex_dict{{"word", "a"},{"count", 1}}, 
        flex_dict{{"word", "cat"},{"count", 2}}, 
        flex_dict{{"word", "is"},{"count", 3}}, 
        flex_dict{{"word", "coming"},{"count", 4}}});
      std::cout <<  sa.unpack("");
      std::cout <<  sa.unpack("X", {}, FLEX_UNDEFINED, {"word"});

      auto  sa2 = gl_sarray({flex_vec{1, 0, 1}, 
        flex_vec{1, 1, 1}, 
        flex_vec{0, 1}});
      std::cout <<  sa2.unpack("X", {flex_type_enum::INTEGER, 
                flex_type_enum::INTEGER, 
                flex_type_enum::INTEGER}, 0);
    }


    void test_basic_indexing_and_ranges() {
      graphlab::gl_sarray a{1,2,3,4,5,6,7,8,9,10};
      a += 1;
      auto t = a[a > 2 && a <= 8];

      std::cout << a << "\n" 
                << t << "\n";

      t = t + 1;

      graphlab::gl_sarray expected{4,5,6,7,8,9};

      // indexing teset
      for (size_t i = 0; i < t.size(); ++i) {
        TS_ASSERT_EQUALS(t[i], expected[i]);
      }

      size_t ctr = 2;
      for(const auto& vals : a.range_iterator()) {
        TS_ASSERT_EQUALS((int)vals, ctr);
        ++ctr;
      }
      TS_ASSERT_EQUALS(ctr, 12);

      // range iterator test
      TS_ASSERT_EQUALS(t.size(), expected.size());
      auto range1 = t.range_iterator();
      auto range2 = expected.range_iterator();
      auto start1 = range1.begin();
      auto start2 = range2.begin();
      while(start1 != range1.end()) {
        TS_ASSERT_EQUALS((*start1), (*start2));
        ++start1;
        ++start2;
      }

      auto b = a.sort(false)[{1, 8}].sort();
      std::cout << b;
      for (size_t i = 0; i < t.size(); ++i) {
        TS_ASSERT_EQUALS(b[i], expected[i]);
      }
    }

    void test_writer() {
      gl_sarray_writer writer(flex_type_enum::INTEGER);
      // write one integer into each segment, so we get [0, 1, 2, 3 ... #segments - 1]
      // then write a few more integers to the end
      for (size_t i = 0;i < writer.num_segments(); ++i) {
        writer.write(i, i);
      }
      std::vector<size_t> values;
      for (size_t i = writer.num_segments(); i < 100; ++i) values.push_back(i);
      writer.write(values.begin(), values.end(), writer.num_segments() - 1);
      gl_sarray array = writer.close();

      // done!
      gl_sarray range_values = gl_sarray::from_sequence(0, 100);
      _assert_sarray_equals(array, _to_vec(range_values));
    }

    void test_slice() {
      gl_sarray a{1,2,3,4,5,6,7,8,9,10};

      std::cout << "\n" << a[{1,4}];  // start at index 1, end at index 4
      // ret is the array [2,3,4]
      std::cout << "\n" << a[{1,2,8}];  // start at index 1, end at index 8 with step size 2
      // ret is the array [2,4,6,8]

      std::cout << "\n" << a[{-3,-1}];  // start at end - 3, end at index end - 1
      // ret is the array [8,9]
    }
  private:

    std::vector<flexible_type> _to_vec(gl_sarray sa) {
      std::vector<flexible_type> ret;
      for (auto& v: sa.range_iterator()) { ret.push_back(v); }
      return ret;
    }

    void _assert_sarray_equals(gl_sarray sa, const std::vector<flexible_type>& vec) {
      TS_ASSERT_EQUALS(sa.size(), vec.size());
      for (size_t i = 0; i < vec.size(); ++i) {
        TS_ASSERT_EQUALS(sa[i], vec[i]);
      }
    }
};
