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
#include <cstdio>
#include <iostream>
#include <algorithm>
#include <fileio/temp_files.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <sframe/dataframe.hpp>
#include <sframe/algorithm.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sarray.hpp>
#include <cxxtest/TestSuite.h>
#include <sframe/sframe_config.hpp>
using namespace graphlab;

class unity_sframe_test: public CxxTest::TestSuite {
 private:
  dataframe_t _create_test_dataframe() {
    dataframe_t testdf;
    std::vector<flexible_type> a;
    std::vector<flexible_type> b;
    std::vector<flexible_type> c;
    // create a simple dataframe of 3 columns of 3 types
    for (size_t i = 0;i < 100; ++i) {
      a.push_back(i);
      b.push_back((float)i);
      c.push_back(std::to_string(i));
    }
    testdf.set_column("a", a, flex_type_enum::INTEGER);
    testdf.set_column("b", b, flex_type_enum::FLOAT);
    testdf.set_column("c", c, flex_type_enum::STRING);
    return testdf;
  }
 public:
  unity_sframe_test() {
    global_logger().set_log_level(LOG_WARNING);
  }

  void test_array_construction() {
    dataframe_t testdf = _create_test_dataframe();
    // create a unity_sframe
    auto sframe = std::make_shared<unity_sframe>();
    sframe->construct_from_dataframe(testdf);

    // check basic stats
    TS_ASSERT_EQUALS(sframe->size(), 100);
    TS_ASSERT_EQUALS(sframe->num_columns(), 3);

    // check types match
    std::vector<flex_type_enum> dtypes = sframe->dtype();
    TS_ASSERT_EQUALS(dtypes[0], flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dtypes[1], flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dtypes[2], flex_type_enum::STRING);

    // check names match
    std::vector<std::string> names = sframe->column_names();
    TS_ASSERT_EQUALS(names[0], "a");
    TS_ASSERT_EQUALS(names[1], "b");
    TS_ASSERT_EQUALS(names[2], "c");

    // get the first 50 and check that newdf == testdf for the first 50 rows
    // and that newdf is well formed
    dataframe_t newdf = sframe->_head(50);
    TS_ASSERT_EQUALS(newdf.ncols(), 3);
    TS_ASSERT_EQUALS(newdf.nrows(), 50);

    TS_ASSERT_EQUALS(newdf.names[0], "a");
    TS_ASSERT_EQUALS(newdf.names[1], "b");
    TS_ASSERT_EQUALS(newdf.names[2], "c");

    TS_ASSERT_EQUALS(newdf.values["a"].size(), 50);
    TS_ASSERT_EQUALS(newdf.values["b"].size(), 50);
    TS_ASSERT_EQUALS(newdf.values["c"].size(), 50);

    for (size_t i = 0;i < 50; ++i) {
      TS_ASSERT_EQUALS(newdf.values["a"][i], testdf.values["a"][i]);
      TS_ASSERT_EQUALS(newdf.values["b"][i], testdf.values["b"][i]);
      TS_ASSERT_EQUALS(newdf.values["c"][i], testdf.values["c"][i]);
    }

    // check the tail end too
    dataframe_t taildf = sframe->_tail(50);
    TS_ASSERT_EQUALS(taildf.ncols(), 3);
    TS_ASSERT_EQUALS(taildf.nrows(), 50);

    TS_ASSERT_EQUALS(taildf.names[0], "a");
    TS_ASSERT_EQUALS(taildf.names[1], "b");
    TS_ASSERT_EQUALS(taildf.names[2], "c");

    TS_ASSERT_EQUALS(taildf.values["a"].size(), 50);
    TS_ASSERT_EQUALS(taildf.values["b"].size(), 50);
    TS_ASSERT_EQUALS(taildf.values["c"].size(), 50);

    for (size_t i = 0;i < 50; ++i) {
      TS_ASSERT_EQUALS(taildf.values["a"][i], testdf.values["a"][i+50]);
      TS_ASSERT_EQUALS(taildf.values["b"][i], testdf.values["b"][i+50]);
      TS_ASSERT_EQUALS(taildf.values["c"][i], testdf.values["c"][i+50]);
    }
  }

  std::shared_ptr<sarray<flexible_type>> _write_sarray(
      std::vector<flexible_type> data,
      flex_type_enum type) {
    auto wr = std::make_shared<sarray<flexible_type>>();
    wr->open_for_write();
    wr->set_type(type);
    graphlab::copy(data.begin(), data.end(), *wr);
    wr->close();
    return wr;
  }

  void test_logical_filter() {
    // Write some test sarrays
    std::vector<flexible_type> test_data{1,3,5,7,8,9,23,64,42,52};
    std::vector<flexible_type> empty_vec{};
    std::vector<flexible_type> one_vec;
    std::vector<flexible_type> zero_vec;
    std::vector<flexible_type> flipflop_vec;
    for(size_t i = 0; i < test_data.size(); ++i) {
      one_vec.push_back(1);
      zero_vec.push_back(0);
      if(i % 2 == 0) {
        flipflop_vec.push_back("hello");
      } else {
        flipflop_vec.push_back("");
      }
    }

    // Make unity_sarrays
    std::shared_ptr<unity_sarray> unity_int_data(new unity_sarray());
    std::shared_ptr<unity_sarray> unity_float_data(new unity_sarray());
    std::shared_ptr<unity_sarray> unity_one(new unity_sarray());
    std::shared_ptr<unity_sarray> unity_zero(new unity_sarray());
    std::shared_ptr<unity_sarray> unity_flipflop(new unity_sarray());
    unity_int_data->construct_from_vector(test_data, flex_type_enum::INTEGER);
    unity_float_data->construct_from_vector(test_data, flex_type_enum::FLOAT);
    unity_one->construct_from_vector(one_vec, flex_type_enum::INTEGER);
    unity_zero->construct_from_vector(zero_vec, flex_type_enum::INTEGER);
    unity_flipflop->construct_from_vector(flipflop_vec, flex_type_enum::STRING);

    // Empty sframe
    auto sf = std::make_shared<unity_sframe>();
    auto sa = std::make_shared<unity_sarray>();
    auto ret_sf = sf->logical_filter(unity_int_data);
    auto ret_sa = sa->logical_filter(unity_int_data);
    TS_ASSERT_EQUALS(ret_sf->size(), 0);
    TS_ASSERT_EQUALS(ret_sa->size(), 0);

    // One empty column
    dataframe_t df;
    df.set_column("empty", empty_vec, flex_type_enum::STRING);
    sf->construct_from_dataframe(df);
    sa->construct_from_vector(empty_vec, flex_type_enum::STRING);
    TS_ASSERT_THROWS_ANYTHING(sf->logical_filter(unity_float_data));
    TS_ASSERT_THROWS_ANYTHING(sa->logical_filter(unity_float_data));

    // Fill sframe with test data
    sf->remove_column(0);
    sf->add_column(unity_int_data, std::string("intstuff"));
    sf->add_column(unity_float_data, std::string("floatstuff"));
    sa->construct_from_vector(test_data, flex_type_enum::FLOAT);

    // Filter by all 1's
    auto res_ptr = sf->logical_filter(unity_one);
    auto sa_res_ptr = sa->logical_filter(unity_one);
    auto tmp_df = res_ptr->_head(10);
    auto tmp_vec = sa_res_ptr->_head(10);
    for(size_t i = 0; i < test_data.size(); ++i) {
      TS_ASSERT_EQUALS(tmp_df.values["intstuff"][i], test_data[i]);
      TS_ASSERT_EQUALS(tmp_df.values["floatstuff"][i], flex_float(test_data[i]));
      TS_ASSERT_EQUALS(tmp_vec[i], flex_float(test_data[i]));
    }
    tmp_df.clear();
    tmp_vec.clear();

    // Filter by all 0's
    res_ptr = sf->logical_filter(unity_zero);
    sa_res_ptr = sa->logical_filter(unity_zero);
    TS_ASSERT_EQUALS(res_ptr->size(), 0);
    TS_ASSERT_EQUALS(sa_res_ptr->size(), 0);

    // Filter ints and floats by string (...say, every other one)
    res_ptr = sf->logical_filter(unity_flipflop);
    sa_res_ptr = sa->logical_filter(unity_flipflop);
    tmp_df = res_ptr->_head(10);
    tmp_vec = sa_res_ptr->_head(10);
    for(size_t i = 0; i < 5; ++i) {
      TS_ASSERT_EQUALS(test_data[i*2], tmp_df.values["intstuff"][i]);
      TS_ASSERT_EQUALS(flex_float(test_data[i*2]), tmp_df.values["floatstuff"][i]);
      TS_ASSERT_EQUALS(flex_float(test_data[i*2]), tmp_vec[i]);
    }

    // ***Bad stuff***
    // Bad pointer to sarray
    TS_ASSERT_THROWS_ANYTHING(sf->logical_filter(NULL));
    TS_ASSERT_THROWS_ANYTHING(sa->logical_filter(NULL));

    // Misaligned (but same size) TODO

    // "Aligned" but different size
    auto unity_empty = std::make_shared<unity_sarray>();
    unity_empty->construct_from_vector(empty_vec,flex_type_enum::INTEGER);
    TS_ASSERT_THROWS_ANYTHING(sf->logical_filter(unity_empty));
    TS_ASSERT_THROWS_ANYTHING(sa->logical_filter(unity_empty));

    // Stress test (save for python)
  }

  // tests add_column(s), select_column(s)
  void test_column_ops() {
    dataframe_t testdf = _create_test_dataframe();

    // an empty sframe
    auto sf = std::make_shared<unity_sframe>();
    std::vector<std::string> col_names{"a","c"};
    TS_ASSERT_THROWS_ANYTHING(sf->select_columns(col_names)->size());

    // Write a test sarray
    std::vector<flexible_type> data;
    for(size_t i = 0; i < 30; ++i) {
      data.push_back(flex_int(i*2));
    }
    auto sarray_ptr = _write_sarray(data, flex_type_enum::INTEGER);
    std::shared_ptr<unity_sarray> us_ptr(new unity_sarray());
    us_ptr->construct_from_sarray(sarray_ptr);

    // Add to an empty sframe
    sf->add_column(us_ptr, std::string("testname"));

    // Check size and contents
    TS_ASSERT_EQUALS(sf->num_columns(), 1);
    TS_ASSERT_EQUALS(sf->size(), 30);
    auto headdf = sf->_head(30);
    for(size_t i = 0; i < headdf.nrows(); ++i) {
      TS_ASSERT_EQUALS(flex_int(headdf.values["testname"][i]), flex_int(i*2));
    }

    // Add same column
    sf->add_column(us_ptr, std::string("testname-copy"));
    auto two_col_head = sf->_head(30);
    for(size_t i = 0; i < two_col_head.nrows(); ++i) {
      TS_ASSERT_EQUALS(flex_int(two_col_head.values["testname-copy"][i]), flex_int(i*2));
    }

    // Add misaligned column (write to only one segment)
    auto wr = std::make_shared<sarray<flexible_type>>();
    wr->open_for_write();
    wr->set_type(flex_type_enum::INTEGER);
    auto zero_iter = wr->get_output_iterator(0);
    for(size_t i = 0; i < 30; ++i) {
      *zero_iter = i;
      ++zero_iter;
    }
    wr->close();
    std::shared_ptr<unity_sarray> ma_ptr(new unity_sarray());
    ma_ptr->construct_from_sarray(wr);
    sf->add_column(ma_ptr, std::string("misalign"));
    auto mis_head = sf->_head(30);
    for(size_t i = 0; i < mis_head.nrows(); ++i) {
      TS_ASSERT_EQUALS(flex_int(mis_head.values["misalign"][i]), flex_int(i));
    }

    // Wrong size column
    data.push_back(flex_int(9999));
    sarray_ptr = _write_sarray(data, flex_type_enum::INTEGER);
    std::shared_ptr<unity_sarray> w_ptr(new unity_sarray());
    w_ptr->construct_from_sarray(sarray_ptr);

    TS_ASSERT_THROWS_ANYTHING(sf->add_column(w_ptr, std::string("testname")));

    // add multiple columns
    std::list<std::shared_ptr<unity_sarray_base>> multiple_cols {ma_ptr, us_ptr};
    std::vector<std::string> empty_vec{};
    sf->add_columns(multiple_cols, empty_vec);
    auto mult_col_head = sf->_head(30);
    for(size_t i = 0; i < mult_col_head.nrows(); ++i) {
      TS_ASSERT(mult_col_head.values.count("X4"));
      TS_ASSERT(mult_col_head.values.count("X5"));
      TS_ASSERT_EQUALS(flex_int(mult_col_head.values["X4"][i]), flex_int(i));
      TS_ASSERT_EQUALS(flex_int(mult_col_head.values["X5"][i]), flex_int(i*2));
    }

    // Throw exception
    multiple_cols.back() = NULL;
    TS_ASSERT_THROWS_ANYTHING(sf->add_columns(multiple_cols, empty_vec));

    // duplicate columns
    TS_ASSERT_THROWS_ANYTHING(sf->select_columns({"a", "b","a"}));

    // check size
    sf->construct_from_dataframe(testdf);
    auto sub_sf = sf->select_columns(col_names);
    auto sub_col = sf->select_column(std::string("b"));
    TS_ASSERT_EQUALS(sub_sf->num_columns(), 2);
    TS_ASSERT_EQUALS(sub_sf->size(), testdf.nrows());
    TS_ASSERT_EQUALS(testdf.nrows(), sub_col->size());

    // check names
    auto sub_names = sub_sf->column_names();
    TS_ASSERT_EQUALS(col_names[0], sub_names[0]);
    TS_ASSERT_EQUALS(col_names[1], sub_names[1]);

    // check content
    dataframe_t newdf = sub_sf->_head(100);
    auto head_col = sub_col->_head(100);
    for(size_t i = 0; i < 100; ++i) {
      TS_ASSERT_EQUALS(newdf.values["a"][i], testdf.values["a"][i]);
      TS_ASSERT_EQUALS(newdf.values["c"][i], testdf.values["c"][i]);
      TS_ASSERT_EQUALS(head_col[i], testdf.values["b"][i]);
    }

  }

  void test_append_name_mismatch() {
    std::vector<flexible_type> test_data1;
    std::vector<flexible_type> test_data2;

    std::shared_ptr<unity_sframe> sf1(new unity_sframe());
    std::shared_ptr<unity_sframe> sf2(new unity_sframe());

    std::shared_ptr<unity_sarray> sa1(new unity_sarray());
    std::shared_ptr<unity_sarray> sa2(new unity_sarray());
    sa1->construct_from_vector(test_data1, flex_type_enum::INTEGER);
    sa2->construct_from_vector(test_data2, flex_type_enum::STRING);

    sf1->add_column(sa1, "a");
    sf1->add_column(sa2, "b");

    sf2->add_column(sa2, "b");
    sf2->add_column(sa1, "a");

    TS_ASSERT_THROWS_ANYTHING(sf1->append(sf2));
  }

  void test_append_type_mismatch() {
    std::vector<flexible_type> test_data1;
    std::vector<flexible_type> test_data2;

    std::shared_ptr<unity_sframe> sf1(new unity_sframe());
    std::shared_ptr<unity_sframe> sf2(new unity_sframe());

    std::shared_ptr<unity_sarray> sa1(new unity_sarray());
    std::shared_ptr<unity_sarray> sa2(new unity_sarray());
    sa1->construct_from_vector(test_data1, flex_type_enum::INTEGER);
    sa2->construct_from_vector(test_data2, flex_type_enum::STRING);

    sf1->add_column(sa1, "a");
    sf1->add_column(sa2, "b");

    sf2->add_column(sa2, "a");
    sf2->add_column(sa1, "b");

    TS_ASSERT_THROWS_ANYTHING(sf1->append(sf2));
  }

  void test_append() {
    dataframe_t testdf = _create_test_dataframe();

    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    sf1->construct_from_dataframe(testdf);
    sf2->construct_from_dataframe(testdf);
    auto sf3 = sf1->append(sf2);
    TS_ASSERT_EQUALS(sf3->size(), sf1->size() + sf2->size());

    auto sf3_value = sf3->_head((size_t)(-1));

    for (size_t i = 0; i < sf1->size(); i++) {
      TS_ASSERT_EQUALS(sf3_value.values["a"][i], testdf.values["a"][i]);
      TS_ASSERT_EQUALS(sf3_value.values["b"][i], testdf.values["b"][i]);
      TS_ASSERT_EQUALS(sf3_value.values["c"][i], testdf.values["c"][i]);
    }

    for (size_t i = sf1->size(); i < sf3->size(); i++) {
      TS_ASSERT_EQUALS(sf3_value.values["a"][i], testdf.values["a"][i - sf1->size()]);
      TS_ASSERT_EQUALS(sf3_value.values["b"][i], testdf.values["b"][i - sf1->size()]);
      TS_ASSERT_EQUALS(sf3_value.values["c"][i], testdf.values["c"][i - sf1->size()]);
    }
  }

  void test_append_empty() {
    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    auto sf3 = sf1->append(sf2);
    TS_ASSERT_EQUALS(sf3->size(), 0);
  }

  void test_append_left_empty() {
    dataframe_t testdf = _create_test_dataframe();

    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    sf2->construct_from_dataframe(testdf);
    auto sf3 = sf1->append(sf2);
    TS_ASSERT_EQUALS(sf3->size(), sf2->size());
  }

  void test_append_right_empty() {
    dataframe_t testdf = _create_test_dataframe();

    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    sf1->construct_from_dataframe(testdf);
    auto sf3 = sf1->append(sf2);
    TS_ASSERT_EQUALS(sf3->size(), sf1->size());
  }

  void test_append_one_column() {
    size_t num_items = 100000;
    std::vector<flexible_type> test_data1;
    std::vector<flexible_type> test_data2;
    for(size_t i = 0; i < num_items; i++) {
      test_data1.push_back(i);
      test_data2.push_back(i + num_items);
    }

    for(size_t i = 0; i < num_items/2; i++) {
      test_data2.push_back(i + num_items);
    }

    // Make unity_sarrays
    std::shared_ptr<unity_sarray> sa1(new unity_sarray());
    std::shared_ptr<unity_sarray> sa2(new unity_sarray());
    sa1->construct_from_vector(test_data1, flex_type_enum::INTEGER);
    sa2->construct_from_vector(test_data2, flex_type_enum::INTEGER);

    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    sf1->add_column(sa1, "something");
    sf2->add_column(sa2, "something");

    auto sf3 = sf1->append(sf2);
    auto sf3_value = sf3->_head((size_t)(-1));
    TS_ASSERT_EQUALS(sf3->size(), sf1->size() + sf2->size());

    for (size_t i = 0; i < sf1->size(); i++) {
      TS_ASSERT_EQUALS(sf3_value.values["something"][i], test_data1[i]);
    }

    for (size_t i = sf1->size(); i < sf3->size(); i++) {
      TS_ASSERT_EQUALS(sf3_value.values["something"][i], test_data2[i - sf1->size()]);
    }
  }

  // disable this test as it takes too long (several minutes), could be used for performance
  // bench marking
  void _test_append_many_columns() {
    size_t num_columns = 1000;
    size_t num_items = 100;
    dataframe_t testdf = _create_test_dataframe();
    std::vector<flexible_type> test_data1;
    std::vector<flexible_type> test_data2;

    for(size_t i = 0; i < num_items; i++) {
      test_data1.push_back(i);
      test_data2.push_back(i + num_items);
    }

    std::shared_ptr<unity_sarray> sa1(new unity_sarray());
    std::shared_ptr<unity_sarray> sa2(new unity_sarray());
    sa1->construct_from_vector(test_data1, flex_type_enum::INTEGER);
    sa2->construct_from_vector(test_data2, flex_type_enum::INTEGER);

    auto sf1 = std::make_shared<unity_sframe>();
    auto sf2 = std::make_shared<unity_sframe>();
    for(size_t i = 0; i < num_columns; i++) {
      std::cout << "appending column " << i << std::endl;
      sf1->add_column(sa1, std::to_string(i));
      sf2->add_column(sa2, std::to_string(i));
    }

    std::cout << "appending two sframes " << std::endl;

    auto sf3 = sf1->append(sf2);

    // check only first column to save time
    auto sf3_values = sf3->select_column("1")->_head((size_t)(-1));

    std::cout << "done appending two sframes " << std::endl;

    TS_ASSERT_EQUALS(sf3->size(), sf1->size() + sf2->size())
    for (size_t i = 0; i < num_items; i++) {
      TS_ASSERT_EQUALS(sf3_values[i], test_data1[i]);
      TS_ASSERT_EQUALS(sf3_values[i + num_items], test_data2[i]);
    }
  }

  void test_group_by() {
    std::vector<flexible_type> user_id;
    std::vector<flexible_type> movie_id;
    std::vector<flexible_type> rating;

    size_t n_unique_user = 1000;
    // create users {0, 1, 2, ...}
    // each user i rates move j = {0, ... i}, with rating (i + j) % 31
    for (size_t i = 0; i < n_unique_user; ++i) {
      for (size_t j = 0; j <= i; ++j) {
        user_id.push_back(i);
        movie_id.push_back(j);
        rating.push_back((i + j) % 31);
      }
    }

    std::shared_ptr<unity_sarray> user_col(new unity_sarray());
    std::shared_ptr<unity_sarray> movie_col(new unity_sarray());
    std::shared_ptr<unity_sarray> rating_col(new unity_sarray());

    user_col->construct_from_vector(user_id, flex_type_enum::INTEGER);
    movie_col->construct_from_vector(movie_id, flex_type_enum::STRING);
    rating_col->construct_from_vector(rating, flex_type_enum::INTEGER);

    std::shared_ptr<unity_sframe> sf(new unity_sframe);
    sf->add_column(user_col, "userid");
    sf->add_column(movie_col, "movieid");
    sf->add_column(rating_col, "rating");

    auto sf_by_user = std::dynamic_pointer_cast<unity_sframe>(sf->group("userid"));
    auto sf_by_movie = std::dynamic_pointer_cast<unity_sframe>(sf->group("movieid"));
    auto sf_by_rating = std::dynamic_pointer_cast<unity_sframe>(sf->group("rating"));

    std::vector<std::shared_ptr<unity_sframe>> grouped_sf{sf_by_user, sf_by_movie, sf_by_rating};

    auto validate_row = [&](const std::vector<flexible_type>& row) {
      if (row.size() != 3) {
        return false;
      } else {
        size_t user_id = row[0];
        size_t movie_id = std::stoi(row[1]);
        size_t rating = row[2];
        return (movie_id <= user_id) && (rating == ((user_id + movie_id) % 31));
      }
    };

    std::for_each(grouped_sf.begin(), grouped_sf.end(),
        [=](std::shared_ptr<unity_sframe> new_sf) {
          TS_ASSERT_EQUALS(new_sf->size(), sf->size());
          TS_ASSERT_EQUALS(new_sf->num_columns(), sf->num_columns());
          for (size_t i = 0; i < new_sf->num_columns(); ++i) {
            TS_ASSERT_EQUALS(new_sf->column_names()[i], sf->column_names()[i]);
            TS_ASSERT_EQUALS(new_sf->dtype()[i], sf->dtype()[i]);
          }
          size_t batch_iter_size = 1000;
          std::vector<std::vector<flexible_type>> read_buffer;
          new_sf->begin_iterator();
          while(true) {
            read_buffer = new_sf->iterator_get_next(batch_iter_size);
            std::for_each(read_buffer.begin(), read_buffer.end(), validate_row);
            if (read_buffer.size() < batch_iter_size) {
              break;
            }
          }
        });
  }

  // This is how toolkit wants to use sframe, so make the scenario work
  void test_empty_sframe() {
    unity_sframe us;
    sframe& sframe = *(us.get_underlying_sframe());
    TS_ASSERT_EQUALS(sframe.size(), 0);
    TS_ASSERT_EQUALS(sframe.num_columns(), 0);
  }

  void _create_test_dataframe_for_sort(
    dataframe_t& testdf,
    bool all_same_value,
    std::vector<std::vector<flexible_type>>& values) {
    testdf.clear();
    values.clear();

    std::vector<flexible_type> a;
    std::vector<flexible_type> b;
    std::vector<flexible_type> c;
    // create a simple dataframe of 3 columns of 3 types
    for (size_t i = 0;i < 100000; ++i) {
      if (all_same_value) {
        a.push_back(1);
        b.push_back(float(1));
        c.push_back(std::to_string(1));
      } else {
        // generate some partial sorted partition
        if (i % 2 == 0) {
          a.push_back(0);
          b.push_back(float(0));
          c.push_back(std::to_string(0));
        } else {
          a.push_back(rand() % 100);
          b.push_back(float(rand() % 100));
          c.push_back(std::to_string(rand() % 100));
        }

        // inject missing values
        if (i % 100 == 0) { a.back()  = FLEX_UNDEFINED; }
        if (i % 200 == 0) { b.back()  = FLEX_UNDEFINED; }
        if (i % 400 == 0) { c.back()  = FLEX_UNDEFINED; }
      }
      values.push_back({a.back(), b.back(), c.back()});
    }
    testdf.set_column("a", a, flex_type_enum::INTEGER);
    testdf.set_column("b", b, flex_type_enum::FLOAT);
    testdf.set_column("c", c, flex_type_enum::STRING);
  }

  void __test_one_sort(
    const std::vector<std::vector<flexible_type>>& values,
    const dataframe_t& testdf,
    const std::vector<std::string>& keys,
    const std::vector<int>& orders) {

    std::vector<std::string> all_keys = {"a", "b", "c"};
    std::vector<size_t> key_indexes;
    for(auto key: keys) {
      for(size_t idx = 0; idx < all_keys.size(); idx++) {
        if (key == all_keys[idx]) {
          key_indexes.push_back(idx);
          break;
        }
      }
    }

    std::cout << "Testing sort by " << std::endl;
    for(size_t i = 0; i < keys.size(); i++) {
      std::cout << keys[i] << ((orders[i] == 1) ? ": ascending":": descending") << ", ";
    }
    std::cout << std::endl;

    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    timer mytimer; mytimer.start();
    std::shared_ptr<unity_sframe_base> result = sframe.sort(keys, orders);
    // do a tail will cause it to materialize
    result->tail(1);
    std::cout << "Sort takes " << mytimer.current_time() << " seconds" << std::endl;

    result->begin_iterator();
    std::vector<flexible_type> prev = {};
    while (true) {
      std::vector<std::vector<flexible_type>> rows = result->iterator_get_next(1);
      if (rows.size() == 0) break;

      if (prev.size() > 0) {
        for(size_t i = 0; i < key_indexes.size(); i++) {
          size_t key_idx = key_indexes[i];
          auto cur_val = rows[0][key_idx];
          auto prev_val = prev[key_idx];


          if (cur_val == FLEX_UNDEFINED) {
            if (prev_val == FLEX_UNDEFINED) continue;
            // descending
            TS_ASSERT(orders[i] == 0);
            break;
          } else {
            if (prev_val == FLEX_UNDEFINED) {
              // ascending
              TS_ASSERT(orders[i] == 1);
              break;
            } else {
              if (cur_val != prev_val) {
                TS_ASSERT_EQUALS(cur_val > prev_val, orders[i] == 1);
                break;
              }
            }
          }
        }

      }
      prev = rows[0];
    }
  }

  void test_sort() {
    dataframe_t testdf;
    std::vector<std::vector<flexible_type>> values;
    _create_test_dataframe_for_sort(testdf, false, values);

    // change sort buffer size to smaller to speed up testing
    graphlab::sframe_config::SFRAME_SORT_BUFFER_SIZE = 1024 * 1024;

    std::vector<std::string> sort_keys;
    std::vector<int> sort_orders;

    std::cout << "testing random sframe" << std::endl;

    sort_keys = {"a", "b"};
    sort_orders = {1, 1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a"};
    sort_orders = {1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a"};
    sort_orders = {0};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b"};
    sort_orders = {0, 0};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b"};
    sort_orders = {0, 1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b"};
    sort_orders = {1, 0};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b", "c"};
    sort_orders = {1, 0, 1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"b", "c", "a"};
    sort_orders = {1, 0, 1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b", "c"};
    sort_orders = {1, 1, 1};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    sort_keys = {"a", "b", "c"};
    sort_orders = {0, 0, 0};
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    // all sorted
    sort_keys = {"b", "c", "a"};
    sort_orders = {1, 0, 1};
    std::cout << "testing all sorted sframe" << std::endl;
    _create_test_dataframe_for_sort(testdf, true, values);
    __test_one_sort(values, testdf,  sort_keys, sort_orders);

    // missing values

  }

  void test_sort_exception() {
    auto sa = std::make_shared<unity_sarray>();
    auto sf = std::make_shared<unity_sframe>();

    std::vector<flexible_type> vec_val;
    for(size_t i = 0; i < 100; i++) {
      flex_vec vec = {flexible_type(i)};
      vec_val.push_back(flexible_type(vec));
    }

    sa->construct_from_vector(vec_val, flex_type_enum::LIST);
    sf->add_column(sa, "a");
    TS_ASSERT_THROWS_ANYTHING(sf->sort(std::vector<std::string>({"a"}), std::vector<int>({0})));

    sa->construct_from_vector(vec_val, flex_type_enum::VECTOR);
    sf->add_column(sa, "b");
    TS_ASSERT_THROWS_ANYTHING(sf->sort(std::vector<std::string>({"b"}), std::vector<int>({0})));
  }
};
