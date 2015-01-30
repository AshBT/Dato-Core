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

#include <cxxtest/TestSuite.h>

#include <unistd.h>

#include <fileio/temp_files.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>
using namespace graphlab;

class unity_sarray_test: public CxxTest::TestSuite {
 public:
  unity_sarray_test() {
    global_logger().set_log_level(LOG_FATAL);
  }

  void test_array_construction() {
    unity_sarray dbl;
    std::vector<flexible_type> vec;
    for (size_t i = 0;i < 20; ++i) vec.emplace_back((double)i);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);

    unity_sarray fail;
    TS_ASSERT_EQUALS(fail.dtype(), flex_type_enum::UNDEFINED);

    // this will succeed. float can cast to string
    dbl.construct_from_vector(vec, flex_type_enum::STRING);
    std::vector<flexible_type> retvec = dbl._head(20);
    for (size_t i = 0;i < 20; ++i) {
      std::string left = static_cast<std::string>(vec[i]);
      TS_ASSERT_EQUALS(retvec[i].get_type(), flex_type_enum::STRING);
      std::string right = static_cast<std::string>(retvec[i]);
      TS_ASSERT_EQUALS(left, right);
    }

    // this will succeed. float can cast to integer
    dbl.construct_from_vector(vec, flex_type_enum::INTEGER);
    retvec = dbl._head(20);
    for (size_t i = 0;i < 20; ++i) {
      int left = static_cast<int>(vec[i]);
      TS_ASSERT_EQUALS(retvec[i].get_type(), flex_type_enum::INTEGER);
      int right = static_cast<int>(retvec[i]);
      TS_ASSERT_EQUALS(left, right);
    }
    TS_ASSERT_THROWS_ANYTHING(fail.construct_from_vector(vec, flex_type_enum::VECTOR));
    // add a string
    std::vector<flexible_type> vec2;
    vec2.emplace_back(std::string("hello world"));
    TS_ASSERT_THROWS_ANYTHING(fail.construct_from_vector(vec2, flex_type_enum::FLOAT));
    TS_ASSERT_THROWS_ANYTHING(fail.construct_from_vector(vec2, flex_type_enum::INTEGER));
    TS_ASSERT_THROWS_ANYTHING(fail.construct_from_vector(vec2, flex_type_enum::VECTOR));

    retvec = dbl._head(20);
    TS_ASSERT_EQUALS(vec.size(), retvec.size());
    for (size_t i = 0;i < 20; ++i) {
      TS_ASSERT_EQUALS(vec[i], retvec[i]);
    }
  }

  void test_array_head() {
    unity_sarray dbl;
    std::vector<flexible_type> vec;
    std::vector<flexible_type> vec_out;

    for (size_t i = 0;i < graphlab::sframe_config::SFRAME_READ_BATCH_SIZE * 2.5; ++i) vec.emplace_back((double)i);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);

    // test larger size, internally we read a batch of 100, so test with bigger number
    size_t items_to_read = graphlab::sframe_config::SFRAME_READ_BATCH_SIZE * 1.5;
    vec_out = dbl._head(items_to_read);
    for(size_t i =0; i < items_to_read; i++) {
        TS_ASSERT_EQUALS(i, vec_out[i]);
    }

    items_to_read = 10;
    vec_out = dbl._head(items_to_read);
    for(size_t i =0; i < items_to_read; i++) {
        TS_ASSERT_EQUALS(i, vec_out[i]);
    }
  }

  void array_construction_from_file(size_t nlines,
                                    bool compress,
                                    flex_type_enum type = flex_type_enum::INTEGER) {
    // write a file with 'nlines' lines. Attach a .gz to the end of the filename
    // if we are compressing.
    std::string tempfile = get_temp_name();
    if (compress) tempfile = tempfile + ".gz";
    graphlab::general_ofstream fout(tempfile.c_str());

    std::function<flexible_type(size_t)> flex_type_from_size_t = [&](size_t val) {
      flexible_type flex_val(type);
      switch(type) {
       case flex_type_enum::INTEGER:
         flex_val = val;
         break;
       case flex_type_enum::FLOAT:
         flex_val = (float)val;
         break;
       case flex_type_enum::STRING:
         flex_val = std::to_string(val);
         break;
       case flex_type_enum::VECTOR:
         flex_val.mutable_get<flex_vec>().push_back(val);
         break;
       case flex_type_enum::DICT:
         flex_val.mutable_get<flex_dict>().push_back({0, val});
         break;
       case flex_type_enum::LIST:
         flex_val.mutable_get<flex_list>().push_back(val);
         break;
       default:
         throw ("Unknown flexible_type");
      }
      return flex_val;
    };

    for (size_t i = 0;i < nlines ; ++i) {
      size_t val = ((i * 93563) % 100000);
      fout << flex_type_from_size_t(val) << "\n";
    }
    fout.close();

    // read it back in
    unity_sarray strarray;
    strarray.construct_from_files(tempfile, type);
    TS_ASSERT_EQUALS(strarray.dtype(), type);
    std::vector<flexible_type> vals = strarray._head(nlines);
    TS_ASSERT_EQUALS(vals.size(), nlines);

    // make sure we read it back in the correct order
    for (size_t i = 0;i < vals.size(); ++i) {
      TS_ASSERT_EQUALS(vals[i].get_type(), type);
      size_t val = ((i * 93563) % 100000);
      flexible_type actual = vals[i];
      flexible_type expected = flex_type_from_size_t(val);
      if (actual != expected) {
        std::cout << actual << " not equals to "
                  << expected << std::endl;
      }
      TS_ASSERT_EQUALS(vals[i], flex_type_from_size_t(val));
    }
  }

  void test_array_construction_from_file() {
    std::vector<flex_type_enum> types{
      flex_type_enum::INTEGER,
      flex_type_enum::FLOAT,
      flex_type_enum::STRING,
      flex_type_enum::VECTOR,
      flex_type_enum::DICT,
      flex_type_enum::LIST
      };

    for (flex_type_enum t : types) {
      // test create from file at a variety of lengths
      array_construction_from_file(1, false, t);
      array_construction_from_file(16, false, t);
      array_construction_from_file(128, false, t);
      array_construction_from_file(1024, false, t);

      // At these number of lines, we should be able read everything
      // even if the file is compressed
      array_construction_from_file(1, true, t);
      array_construction_from_file(16, true, t);
      array_construction_from_file(128, true, t);
      array_construction_from_file(1024, true, t);
    }
  }

  void test_any_all() {
    unity_sarray dbl;
    std::vector<flexible_type> vec;

    TS_ASSERT_EQUALS(dbl.dtype(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl.any(), false);
    TS_ASSERT_EQUALS(dbl.all(), true);

    // empty vector
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl.any(), false);
    TS_ASSERT_EQUALS(dbl.all(), true);

    // create an array of all zeros
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back(0.0);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl.any(), false);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all zeros with a single 1 somwhere in between
    vec[11] = 1.0;
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all ones
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back(1.0);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), true);

    // create an array of all empty strings
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back("");
    dbl.construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_EQUALS(dbl.any(), false);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all empty strings except for one
    vec[4] = "hello world";
    dbl.construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all non-empty strings
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back("hello");
    dbl.construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), true);


    // create an array of all empty vectors
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back(std::vector<double>());
    dbl.construct_from_vector(vec, flex_type_enum::VECTOR);
    TS_ASSERT_EQUALS(dbl.any(), false);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all empty vectors except for one
    vec[4] = std::vector<double>{1,2,3};
    dbl.construct_from_vector(vec, flex_type_enum::VECTOR);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), false);

    // create an array of all non-empty vectors
    vec.clear();
    for (size_t i = 0;i < 20; ++i) vec.emplace_back(std::vector<double>{1,2,3});
    dbl.construct_from_vector(vec, flex_type_enum::VECTOR);
    TS_ASSERT_EQUALS(dbl.any(), true);
    TS_ASSERT_EQUALS(dbl.all(), true);
  }
  void test_std_var() {
    unity_sarray dbl;
    std::vector<flexible_type> vec;

    // empty sarray
    TS_ASSERT_EQUALS(dbl.std().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl.var().get_type(), flex_type_enum::UNDEFINED);

    // empty vector
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl.std().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl.var().get_type(), flex_type_enum::UNDEFINED);

    // an array of increasing ints
    for(size_t i = 0; i < 10; ++i) vec.push_back(i);
    dbl.construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_DELTA(dbl.var(), flex_float(8.25), 1e-7);
    TS_ASSERT_DELTA(dbl.std(), flex_float(2.87228), flex_float(.00001));

    // an array of decreasing floats
    vec.clear();
    for(size_t i = 35; i > 7; --i) vec.push_back(i);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_DELTA(dbl.var(), flex_float(65.25), 1e-7);
    TS_ASSERT_DELTA(dbl.std(), flex_float(8.07775), flex_float(.00001));

    // missing values
    vec.emplace_back(flex_type_enum::UNDEFINED);
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_DELTA(dbl.var(), flex_float(65.25), 1e-7);
    TS_ASSERT_DELTA(dbl.std(), flex_float(8.07775), flex_float(.00001));

    // a more interesting variance of floats
    vec.clear();
    size_t cntr = 0;
    for(double i = -6.4; i < 20.0; i += cntr*.2, ++cntr) {
      vec.push_back(i);
    }
    dbl.construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_DELTA(dbl.var(), flex_float(58.56), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.std(), flex_float(7.65245), flex_float(.00001));

    // Some legal values of ddof
    TS_ASSERT_DELTA(dbl.var(1), flex_float(62.22), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.std(1), flex_float(7.88797), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.var(2), flex_float(66.368), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.std(2), flex_float(8.14665), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.var(cntr-6), flex_float(165.92), flex_float(.00001));
    TS_ASSERT_DELTA(dbl.std(cntr-6), flex_float(12.88099), flex_float(.00001));

    // Illegal values of ddof
    TS_ASSERT_THROWS_ANYTHING(dbl.var(cntr));
    TS_ASSERT_THROWS_ANYTHING(dbl.std(cntr+1));

    // other bad stuff...
    dbl.construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_THROWS_ANYTHING(dbl.var());
    TS_ASSERT_THROWS_ANYTHING(dbl.std());

    // an overflow test
    vec.clear();
    vec.push_back(1);
    vec.push_back(std::numeric_limits<flex_int>::max());
    dbl.construct_from_vector(vec, flex_type_enum::INTEGER);

    TS_ASSERT_DELTA(dbl.var(), flex_float(21267647932558653957237540927630737409.0), 1e-7);
    TS_ASSERT_DELTA(dbl.std(), flex_float(4611686018427387900.0), flex_float(100.0));
  }

  void test_max_min_sum_mean() {
    auto dbl = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec;

    // empty sarray
    TS_ASSERT_EQUALS(dbl->dtype(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->max().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->min().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->sum().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->mean().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->topk_index(10, false)->size(), 0);

    // empty vector
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl->max().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->min().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->sum().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->mean().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_EQUALS(dbl->topk_index(10, false)->size(), 0);

    // an array of increasing ints
    for(size_t i = 0; i < 20; ++i) vec.push_back(i);
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), flex_int(19));
    TS_ASSERT_EQUALS(dbl->min(), flex_int(0));
    TS_ASSERT_EQUALS(dbl->sum(), flex_int(190));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(9.5), 1e-7);

    auto us_ptr = dbl->topk_index(10, false);
    TS_ASSERT_EQUALS(us_ptr->size(), 20);
    auto contents = us_ptr->_head(20);
    for(size_t i = 0; i < 20; ++i) {
      if(i > 9) {
        TS_ASSERT_EQUALS(contents[i], 1);
      } else {
        TS_ASSERT_EQUALS(contents[i], 0);
      }
    }
    contents.clear();

    // an array of increasing floats
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl->max(), flex_float(19.0));
    TS_ASSERT_EQUALS(dbl->min(), flex_float(0.0));
    TS_ASSERT_EQUALS(dbl->sum(), flex_float(190.0));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(9.5), 1e-7);
    us_ptr = dbl->topk_index(10, false);
    TS_ASSERT_EQUALS(us_ptr->size(), 20);
    contents = us_ptr->_head(20);
    for(size_t i = 0; i < 20; ++i) {
      if(i > 9) {
        TS_ASSERT_EQUALS(contents[i], 1);
      } else {
        TS_ASSERT_EQUALS(contents[i], 0);
      }
    }
    contents.clear();

    // an array of decreasing ints
    vec.clear();
    for(size_t i = 35; i > 7; --i) vec.push_back(i);
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), flex_int(35));
    TS_ASSERT_EQUALS(dbl->min(), flex_int(8));
    TS_ASSERT_EQUALS(dbl->sum(), flex_int(602));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(21.5), 1e-7);

    // an array of decreasing floats
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(dbl->max(), flex_float(35.0));
    TS_ASSERT_EQUALS(dbl->min(), flex_float(8.0));
    TS_ASSERT_EQUALS(dbl->sum(), flex_float(602.0));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(21.5), 1e-7);

    // invalid type
    dbl->construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_THROWS_ANYTHING(dbl->max());
    TS_ASSERT_THROWS_ANYTHING(dbl->min());
    TS_ASSERT_THROWS_ANYTHING(dbl->sum());
    TS_ASSERT_THROWS_ANYTHING(dbl->mean());
    us_ptr = dbl->topk_index(3, false);
    contents = us_ptr->_head(us_ptr->size());
    for(size_t i = 0; i < contents.size(); ++i) {
      if(i > 25 || i ==0) {
        TS_ASSERT_EQUALS(contents[i], 1);
      } else {
        TS_ASSERT_EQUALS(contents[i], 0);
      }
    }
    contents.clear();

    // an array of all negative numbers
    vec.clear();
    for(int i = -15; i < 0; ++i) vec.push_back(i);
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), flex_int(-1));
    TS_ASSERT_EQUALS(dbl->min(), flex_int(-15));
    TS_ASSERT_EQUALS(dbl->sum(), flex_int(-120));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(-8.0), 1e-7);
    us_ptr = dbl->topk_index(6, false);
    contents = us_ptr->_head(15);
    for(size_t i = 0; i < contents.size(); ++i) {
      if(i > 8) {
        TS_ASSERT_EQUALS(contents[i], 1);
      } else {
        TS_ASSERT_EQUALS(contents[i], 0);
      }
    }
    contents.clear();

    // too much k
    us_ptr = dbl->topk_index(4000, false);
    contents = us_ptr->_head(15);
    for(const auto &i : contents) {
      TS_ASSERT_EQUALS(i, 1);
    }
    contents.clear();

    // an array of mixed negative/positive
    vec.clear();
    for(int i = -4; i < 5; ++i) vec.push_back(i);
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), flex_int(4));
    TS_ASSERT_EQUALS(dbl->min(), flex_int(-4));
    TS_ASSERT_EQUALS(dbl->sum(), flex_int(0));
    TS_ASSERT_DELTA(dbl->mean(), flex_float(0.0), 1e-7);

    // a large array
    vec.clear();

    std::srand(std::time(0));
    int max_place = 1 + (std::rand() % 10000);
    int min_place = 1 + (std::rand() % 10000);
    while(max_place == min_place) {
      min_place = 1 + (std::rand() % 10000);
    }

    for(int i = 0; i < 10000; ++i) {
      if(i == max_place) {
        vec.push_back(std::numeric_limits<flex_int>::max());
      } else if(i == min_place) {
        vec.push_back(std::numeric_limits<flex_int>::lowest());
      } else {
        vec.push_back(flex_int(std::rand()));
      }
    }

    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), std::numeric_limits<flex_int>::max());
    TS_ASSERT_EQUALS(dbl->min(), std::numeric_limits<flex_int>::lowest());
    us_ptr = dbl->topk_index(1, false);
    contents = us_ptr->_head(10000);

    for(size_t i = 0; i < 10000; ++i) {
      if(i == size_t(max_place)) {
        TS_ASSERT_EQUALS(contents[i], 1);
      } else {
        TS_ASSERT_EQUALS(contents[i], 0);
      }
    }
    contents.clear();


    // missing values
    vec.emplace_back(flex_type_enum::UNDEFINED);
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);

    TS_ASSERT_EQUALS(dbl->max(), std::numeric_limits<flex_int>::max());
    TS_ASSERT_EQUALS(dbl->min(), std::numeric_limits<flex_int>::lowest());
    TS_ASSERT_DIFFERS(dbl->sum().get_type(), flex_type_enum::UNDEFINED);
    TS_ASSERT_DIFFERS(dbl->mean().get_type(), flex_type_enum::UNDEFINED);

    // overflow!
    vec.clear();
    vec.push_back(1);
    vec.push_back(std::numeric_limits<flex_int>::max());
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    TS_ASSERT_EQUALS(dbl->max(), std::numeric_limits<flex_int>::max());
    TS_ASSERT_EQUALS(dbl->min(), 1);
    // Yes, we are expecting an overflow here.  Once we solve this problem
    // (if we ever do) replace this with the correct behavior.
    TS_ASSERT_EQUALS(dbl->sum(), std::numeric_limits<flex_int>::lowest());

    // These shouldn't overflow
    TS_ASSERT_DELTA(dbl->mean(), flex_float(4611686018427387904.0), 1e-7);

    // overflow double
    vec.clear();
    vec.push_back(flex_float(1));
    vec.push_back(flex_float(std::numeric_limits<flex_float>::max()));
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);
    TS_ASSERT_DELTA(dbl->mean(), flex_float((vec[1]/2.0)+(vec[0]/2.0)), 1e-7);
  }

  void test_astype() {
    auto dbl = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec{24,25,26};
    std::vector<flexible_type> fvec{24.2,25.8,26.2};
    //std::vector<flexible_type> vec_vec{flex_vec(vec),flex_vec(fvec)};
    std::vector<flexible_type> empty_vec;
    size_t cntr = 0;

    flex_vec a;
    flex_vec b;
    a.push_back(24);
    a.push_back(25);
    b.push_back(24.2);
    b.push_back(25.8);
    std::vector<flexible_type> vec_vec{a,b};

    // Empty vector
    dbl->construct_from_vector(empty_vec, flex_type_enum::INTEGER);
    auto out = dbl->astype(flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(out->dtype(), flex_type_enum::FLOAT);

    // Illegal cast
    TS_ASSERT_THROWS_ANYTHING(dbl->astype(flex_type_enum::VECTOR));

    // float -> int (should truncate)
    dbl->construct_from_vector(fvec, flex_type_enum::FLOAT);
    out = dbl->astype(flex_type_enum::INTEGER);
    auto vals = out->_head(3);

    for(const auto &i : vals) {
      TS_ASSERT_EQUALS(vec[cntr], i);
      ++cntr;
    }
    cntr = 0;

    // float -> string
    out = dbl->astype(flex_type_enum::STRING);
    vals.clear();
    vals = out->_head(3);
    for(const auto &i : vals) {
      if(cntr == 0) {
        TS_ASSERT_EQUALS(i, "24.2");
      } else if(cntr == 1) {
        TS_ASSERT_EQUALS(i, "25.8");
      } else if(cntr == 2) {
        TS_ASSERT_EQUALS(i, "26.2");
      } else {
        // Shouldn't get here
        TS_ASSERT_EQUALS(0,1);
      }
      ++cntr;
    }

    cntr = 0;

    // int -> float
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    out = dbl->astype(flex_type_enum::FLOAT);
    vals.clear();
    vals = out->_head(3);
    for(const auto &i : vals) {
      TS_ASSERT_EQUALS(i, flex_float(vec[cntr]));
      ++cntr;
    }

    cntr = 0;

    // vector -> string
    dbl->construct_from_vector(vec_vec, flex_type_enum::VECTOR);
    out = dbl->astype(flex_type_enum::STRING);
    vals.clear();
    vals = out->_head(2);

    for(const auto &i : vals) {
      if(cntr == 0) {
        TS_ASSERT_EQUALS(i, "[24 25]");
      } else if(cntr == 1) {
        TS_ASSERT_EQUALS(i, "[24.2 25.8]");
      } else {
        // Shouldn't get here
        TS_ASSERT_EQUALS(0,1);
      }
      ++cntr;
    }

    dbl->construct_from_vector(vals, flex_type_enum::STRING);
    out = dbl->astype(flex_type_enum::VECTOR);
    vals.clear();
    vals = out->_head(2);
    TS_ASSERT_EQUALS(vals.size(), 2);
    TS_ASSERT_EQUALS(vals[0].size(), 2);
    TS_ASSERT_EQUALS(vals[0][0], 24);
    TS_ASSERT_EQUALS(vals[0][1], 25);
    TS_ASSERT_EQUALS(vals[1].size(), 2);
    TS_ASSERT_DELTA(vals[1][0], 24.2, 1E-7);
    TS_ASSERT_DELTA(vals[1][1], 25.8, 1E-7);

    // TODO: recursive?? cell??
  }

  void test_tail() {
    auto dbl = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec;

    // Empty sarray
    auto tail_out = dbl->_tail();
    TS_ASSERT_EQUALS(tail_out.size(), 0);

    // Empty vector
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    tail_out.clear();
    tail_out = dbl->_tail();
    TS_ASSERT_EQUALS(tail_out.size(), 0);

    for(size_t i = 0; i < 20; ++i) {
      vec.push_back(i);
    }

    // standard tail (expect last 10)
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    tail_out.clear();
    tail_out = dbl->_tail();

    for(size_t i = 0; i < 10; ++i) {
      TS_ASSERT_EQUALS(vec[i+10], tail_out[i]);
    }

    // a smaller amount
    tail_out.clear();
    tail_out = dbl->_tail(3);

    for(size_t i = 0; i < 3; ++i) {
      TS_ASSERT_EQUALS(vec[i+17], tail_out[i]);
    }

    // a too big amount
    tail_out.clear();
    tail_out = dbl->_tail(21);

    TS_ASSERT_EQUALS(tail_out.size(), 20);

    for(size_t i = 0; i < 20; ++i) {
      TS_ASSERT_EQUALS(tail_out[i], vec[i]);
    }

    // test bigger size
    vec.clear();
    for(size_t i = 0; i < graphlab::sframe_config::SFRAME_READ_BATCH_SIZE * 2.5; i++) { vec.emplace_back(i); }
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    size_t items_to_read = graphlab::sframe_config::SFRAME_READ_BATCH_SIZE * 1.5;
    tail_out = dbl->_tail(items_to_read);
    TS_ASSERT_EQUALS(items_to_read, tail_out.size());
    for(size_t i = 0; i < tail_out.size(); i++) {
        TS_ASSERT_EQUALS(tail_out[i], vec[vec.size() - items_to_read + i]);
    }
  }

  void test_nonzero() {
    auto dbl = std::make_shared<unity_sarray>();
    size_t cntr = 0;
    std::vector<flexible_type> vec{0,0,2,3,9,0,0,6};
    std::vector<flexible_type> zero_vec{0,0,0,0,0,0,0,0};
    std::vector<flexible_type> nonzero_vec{1,1,1,1,1,1,1};
    std::vector<flexible_type> empty_vec;
    std::vector<flexible_type> string_vec{std::string("hi"),
      std::string("hello"), std::string("hello!"), std::string("")};

    // Empty sarray
    auto nz_out = dbl->nonzero()->to_vector();
    TS_ASSERT_EQUALS(nz_out.size(), 0);
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());

    // Empty vector
    dbl->construct_from_vector(empty_vec, flex_type_enum::INTEGER);
    nz_out.clear();
    nz_out = dbl->nonzero()->to_vector();
    TS_ASSERT_EQUALS(nz_out.size(), 0);
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());

    // normal vec
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    nz_out.clear();
    nz_out = dbl->nonzero()->to_vector();
    std::vector<size_t> exp_val{2,3,4,7};
    TS_ASSERT_EQUALS(exp_val.size(), nz_out.size());
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());

    for(size_t i = 0; i < exp_val.size(); ++i) {
      TS_ASSERT_EQUALS(exp_val[i], nz_out[i]);
    }

    // nonzero vec
    dbl->construct_from_vector(nonzero_vec, flex_type_enum::INTEGER);
    nz_out.clear();
    nz_out = dbl->nonzero()->to_vector();
    TS_ASSERT_EQUALS(nz_out.size(), nonzero_vec.size());
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());
    for(size_t i = 0; i < nonzero_vec.size(); ++i, ++cntr) {
      TS_ASSERT_EQUALS(cntr, nz_out[i]);
    }
    cntr = 0;

    // zero vec
    dbl->construct_from_vector(zero_vec, flex_type_enum::INTEGER);
    nz_out.clear();
    nz_out = dbl->nonzero()->to_vector();
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());
    TS_ASSERT_EQUALS(nz_out.size(), 0);

    // different type
    dbl->construct_from_vector(string_vec, flex_type_enum::STRING);
    nz_out.clear();
    nz_out = dbl->nonzero()->to_vector();
    TS_ASSERT_EQUALS(nz_out.size(), 3);
    TS_ASSERT_EQUALS(dbl->nnz(), nz_out.size());
    for(size_t i = 0; i < 3; ++i) {
      TS_ASSERT_EQUALS(nz_out[i], i);
    }
  }

  void test_clip() {
    auto dbl = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec{24,25,26};
    std::vector<flexible_type> empty_vec;
    size_t cntr = 0;

    // sarray of strings
    dbl->construct_from_vector(vec, flex_type_enum::STRING);
    TS_ASSERT_THROWS_ANYTHING(dbl->clip(25));

    // int w/int threshold
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    std::shared_ptr<unity_sarray_base> out = dbl->clip(flex_int(25), flex_int(25));
    auto clipped_vals = out->_head(3);
    for(const auto &i : clipped_vals) {
      TS_ASSERT_EQUALS(25, i);
    }

    // clip only lower
    out = dbl->clip(flex_int(25));
    clipped_vals.clear();
    clipped_vals = out->_head(3);

    for(const auto &i : clipped_vals) {
      if(cntr > 1) {
        TS_ASSERT_EQUALS(26, i);
      } else {
        TS_ASSERT_EQUALS(25, i);
      }

      ++cntr;
    }

    // clip only higher
    out = dbl->clip(flex_undefined(), flex_int(25));
    clipped_vals.clear();
    clipped_vals = out->_head(3);
    cntr = 0;

    for(const auto &i : clipped_vals) {
      if(cntr == 0) {
        TS_ASSERT_EQUALS(24, i);
      } else {
        TS_ASSERT_EQUALS(25, i);
      }

      ++cntr;
    }

    // int w/float threshold
    out = dbl->clip(flex_float(24.8), flex_float(25.2));
    clipped_vals.clear();
    clipped_vals = out->_head(3);
    flexible_type exp_val = flex_float(24.8);

    for(const auto &i : clipped_vals) {
      TS_ASSERT_DELTA(i, exp_val, 1e-7);
      exp_val += 0.2;
    }

    // float w/ int threshold
    dbl->construct_from_vector(vec, flex_type_enum::FLOAT);
    out = dbl->clip(flex_int(25), flex_int(25));
    clipped_vals.clear();
    clipped_vals = out->_head(3);

    for(const auto &i : clipped_vals) {
      TS_ASSERT_DELTA(i, flex_float(25.0), 1e-7);
    }

    // float w/ float threshold
    out = dbl->clip(flex_float(24.8), flex_float(25.2));
    clipped_vals.clear();
    clipped_vals = out->_head(3);
    exp_val = flex_float(24.8);

    for(const auto &i : clipped_vals) {
      TS_ASSERT_DELTA(i, exp_val, 1e-7);
      exp_val += 0.2;
    }

    // Errors/special cases
    TS_ASSERT_THROWS_ANYTHING(dbl->clip(26, 25));
    TS_ASSERT_THROWS_ANYTHING(dbl->clip(flex_string("hello")));

    out = dbl->clip();
    TS_ASSERT_EQUALS(out.get(), dbl.get());

    // No change to the output, so same object returned
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    out = dbl->clip(flex_float(23.0), flex_float(27.0));
    clipped_vals.clear();
    clipped_vals = out->_head(3);
    TS_ASSERT_EQUALS(out.get(), dbl.get());
    TS_ASSERT_EQUALS(out->dtype(), flex_type_enum::INTEGER);
    cntr = 0;
    for(const auto &i : clipped_vals) {
      TS_ASSERT_DELTA(i, flex_int(vec[cntr]), 1e-7);
      ++cntr;
    }

  }

  void test_drop_missing() {
    std::vector<flexible_type> vec{1,2,3,4,5,6,7,8,9};
    // set every 3rd value to missing
    for (size_t i = 0; i < vec.size(); i += 3){
      vec[i] = flexible_type(flex_type_enum::UNDEFINED);
    }
    auto dbl = std::make_shared<unity_sarray>();
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    auto ret = dbl->drop_missing_values();

    std::vector<flexible_type> dropped_vector = ret->_head(size_t(-1));
    TS_ASSERT_EQUALS(dropped_vector.size(), 6);
    // compare values
    size_t j = 0;
    for (size_t i = 0; i < vec.size(); i += 3){
      if (vec[i].get_type() == flex_type_enum::UNDEFINED) continue;
      TS_ASSERT_EQUALS(vec[i],  dropped_vector[j]);
      ++j;
    }
  }


  void __test_numeric_ops_values_and_clean(std::shared_ptr<unity_sarray_base> s,
                                          flexible_type expectedval) {
    std::vector<flexible_type> vec = s->_head(size_t(-1));
    TS_ASSERT_EQUALS(vec.size(), 10);
    TS_ASSERT_EQUALS(vec[0].get_type(), flex_type_enum::UNDEFINED);
    for (size_t i = 1;i < vec.size(); ++i) {
      TS_ASSERT(vec[i].identical(expectedval));
    }
  }


  void __test_numeric_ops_values_and_clean_no_missing(std::shared_ptr<unity_sarray_base> s,
                                                      flexible_type zeroval,
                                                      flexible_type expectedval) {
    std::vector<flexible_type> vec = s->_head(size_t(-1));
    TS_ASSERT_EQUALS(vec.size(), 10);
    TS_ASSERT(vec[0].identical(zeroval));
    for (size_t i = 1;i < vec.size(); ++i) {
      TS_ASSERT(vec[i].identical(expectedval));
    }
  }

  void test_integer_scalar_ops() {
    // make a vector with an UNDEFINED first value
    std::vector<flexible_type> vec{2,2,2,2,2,2,2,2,2,2};
    vec[0] = flexible_type(flex_type_enum::UNDEFINED);

    unity_sarray dbl;
    dbl.construct_from_vector(vec, flex_type_enum::INTEGER);

    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(1, "+"), 3);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(1, "-"), 1);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "*"), 4);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "/"), 1.0);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, ">"), 0);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, ">="), 1);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "<"), 0);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "<="), 1);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "=="), 1);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(1, "!="), 1);
    __test_numeric_ops_values_and_clean(dbl.left_scalar_operator(2, "!="), 0);

    // these do not change types
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1, "+"), 3);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1, "-"), -1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "*"), 4);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, ">"), 0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, ">="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "<"), 0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "<="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "=="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1, "!="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "!="), 0);

    // these change types
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2, "/"), 1.0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1.0, "+"), 3.0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1.0, "-"), -1.0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "*"), 4.0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "/"), 1.0);
    // these still do not change types
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, ">"), 0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, ">="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "<"), 0);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "<="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "=="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(1.0, "!="), 1);
    __test_numeric_ops_values_and_clean(dbl.right_scalar_operator(2.0, "!="), 0);
  }


  void test_integer_vector_ops() {
    // make a vector with an UNDEFINED first value
    std::vector<flexible_type> vec{2,2,2,2,2,2,2,2,2,2};
    std::vector<flexible_type> vec2{4,4,4,4,4,4,4,4,4,4};
    // one missing at 0 to test missing propagation
    vec[0] = flexible_type(flex_type_enum::UNDEFINED);

    auto dbl = std::make_shared<unity_sarray>();
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);

    auto dbl2 = std::make_shared<unity_sarray>();
    dbl2->construct_from_vector(vec2, flex_type_enum::INTEGER);

    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "+"), 6);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "-"), -2);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "*"), 8);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "/"), 0.5);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, ">"), 0);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, ">="), 0);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "<"), 1);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "<="), 1);
    __test_numeric_ops_values_and_clean_no_missing(dbl->vector_operator(dbl2, "=="), 0, 0);
    __test_numeric_ops_values_and_clean_no_missing(dbl->vector_operator(dbl2, "!="), 1, 1);
  }


  void test_logical_vector_ops() {
    // make a vector with an UNDEFINED first value
    std::vector<flexible_type> vec{0,0,0,0,1,1,1,1};
    std::vector<flexible_type> vec2{1,0,1,0,1,0,1,0};
    // one missing at 0 to test missing propagation
    vec[0] = flexible_type(flex_type_enum::UNDEFINED);

    auto dbl = std::make_shared<unity_sarray>();
    dbl->construct_from_vector(vec, flex_type_enum::INTEGER);
    auto dbl2 = std::make_shared<unity_sarray>();
    dbl2->construct_from_vector(vec2, flex_type_enum::INTEGER);

    // logical and
    {
      auto ret = dbl->vector_operator(dbl2, "&");
      std::vector<flexible_type> vecret = ret->_head(size_t(-1));
      TS_ASSERT_EQUALS(vecret.size(), vec.size());
      TS_ASSERT_EQUALS(vec[0].get_type(), flex_type_enum::UNDEFINED);
      for (size_t i = 1;i < vecret.size(); ++i) {
        TS_ASSERT_EQUALS(vecret[i], int(vec[i]) & int(vec2[i]));
      }
    }

    // logical or
    {
      auto ret = dbl->vector_operator(dbl2, "|");
      std::vector<flexible_type> vecret = ret->_head(size_t(-1));
      TS_ASSERT_EQUALS(vecret.size(), vec.size());
      TS_ASSERT_EQUALS(vec[0].get_type(), flex_type_enum::UNDEFINED);
      for (size_t i = 1;i < vecret.size(); ++i) {
        TS_ASSERT_EQUALS(vecret[i], int(vec[i]) | int(vec2[i]));
      }
    }
  }

  void test_string_scalar_ops() {
    // make a vector with an UNDEFINED first value
    std::vector<flexible_type> vec{"a","a","a","a","a","a","a","a","a","a"};
    // one missing at 0 to test missing propagation
    vec[0] = flexible_type(flex_type_enum::UNDEFINED);

    auto dbl = std::make_shared<unity_sarray>();
    dbl->construct_from_vector(vec, flex_type_enum::STRING);

    auto dbl2 = std::make_shared<unity_sarray>();
    dbl2->construct_from_vector(vec, flex_type_enum::STRING);

    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "+"), "aa");
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", "+"), "ab");
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, ">"), 0);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, ">="), 1);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "<"), 0);
    __test_numeric_ops_values_and_clean(dbl->vector_operator(dbl2, "<="), 1);
    __test_numeric_ops_values_and_clean_no_missing(dbl->vector_operator(dbl2, "=="), 1, 1);
    __test_numeric_ops_values_and_clean_no_missing(dbl->vector_operator(dbl2, "!="), 0, 0);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", "<"), 1);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", ">"), 0);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", "<="), 1);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", ">="), 0);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", "=="),  0);
    __test_numeric_ops_values_and_clean(dbl->left_scalar_operator("b", "!="), 1);
  }

  void test_groupby() {
    std::vector<flexible_type> vec;
    for (size_t i = 0; i < 10*1024*1024/128; ++i) {
      for (size_t j = 0; j < 128; ++j) {
        vec.push_back(j);
      }
    }
    unity_sarray sa1;
    sa1.construct_from_vector(vec, flex_type_enum::INTEGER);
    auto sa2 = std::dynamic_pointer_cast<unity_sarray>(sa1.group());

    std::vector<flexible_type> grouped_vec = sa2->_head(size_t(-1));

    TS_ASSERT_EQUALS(grouped_vec.size(), vec.size());

    auto iter = std::unique(grouped_vec.begin(), grouped_vec.end());
    TS_ASSERT_EQUALS(std::distance(grouped_vec.begin(), iter), 128);
  }

  void test_append() {
    auto sa1 = std::make_shared<unity_sarray>();
    auto sa2 = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec1, vec2;
    for (size_t i = 0;i < 20; ++i) vec1.emplace_back((double)i);
    sa1->construct_from_vector(vec1, flex_type_enum::FLOAT);

    for (size_t i = 0;i < 10; ++i) vec2.emplace_back((double)i);
    sa2->construct_from_vector(vec2, flex_type_enum::FLOAT);

    std::shared_ptr<unity_sarray_base> sa3 = sa1->append(sa2);

    vec1.insert(vec1.end(), vec2.begin(), vec2.end());
    _assert_sarray_equals(sa3, vec1);

    std::shared_ptr<unity_sarray_base> sa1_transform = sa1->left_scalar_operator(1, "+");
    std::shared_ptr<unity_sarray_base> sa2_transform = sa2->left_scalar_operator(1, "+");
    std::shared_ptr<unity_sarray_base> sa3_transform = sa1_transform->append(sa2_transform);
    for (auto& v : vec1) ++v;
    _assert_sarray_equals(sa3_transform, vec1);
  }

  void test_append_exception() {
    auto sa1 = std::make_shared<unity_sarray>();
    auto sa2 = std::make_shared<unity_sarray>();
    std::vector<flexible_type> vec1, vec2;
    for (size_t i = 0;i < 20; ++i) vec1.emplace_back((int)i);
    sa1->construct_from_vector(vec1, flex_type_enum::INTEGER);

    for (size_t i = 0;i < 10; ++i) vec2.emplace_back((double)i);
    sa2->construct_from_vector(vec2, flex_type_enum::FLOAT);

    TS_ASSERT_THROWS_ANYTHING(sa1->append(sa2));
  }

  void test_sparse_vector_save_load() {
    flexible_type vector_v = flex_vec{1,2,3};

    std::vector<std::pair<flexible_type, flexible_type>> m{
      std::make_pair(flexible_type("foo"), flexible_type(1.0)),
      std::make_pair(flexible_type(123), flexible_type("string")),
      std::make_pair(vector_v, vector_v),
      std::make_pair("name1",1),
      std::make_pair("name2",2),
    };

    std::vector<flexible_type> vec;
    for(size_t i = 0; i < 100; i++) {
        vec.push_back(m);
    }

    unity_sarray sa1;
    sa1.construct_from_vector(vec, flex_type_enum::DICT);
    std::string tempfile = get_temp_name() + ".sidx";
    sa1.save_array(tempfile);

    unity_sarray sa2;
    sa2.construct_from_sarray_index(tempfile);

    TS_ASSERT_EQUALS(sa2.size(), sa1.size());

    auto sa1_values = sa1._head((size_t)(-1));
    auto sa2_values = sa2._head((size_t)(-1));
    for(size_t i = 0; i < sa2.size(); i++) {
      std::vector<std::pair<flexible_type, flexible_type>> v1 = sa1_values[i];
      std::vector<std::pair<flexible_type, flexible_type>> v2 = sa2_values[i];
      TS_ASSERT_EQUALS(v1.size(), v2.size());
      for(size_t j = 0; j < v1.size(); j++) {
        TS_ASSERT_EQUALS(v1[j], v2[j]);
        TS_ASSERT_EQUALS(v1[j].first, v2[j].first);
        TS_ASSERT_EQUALS(v1[j].second, v2[j].second);
      }
    }
  }


  void _assert_sarray_equals(std::shared_ptr<unity_sarray_base> sa, std::vector<flexible_type> vec) {
   auto all_items = sa->_head((size_t)(-1));
   TS_ASSERT_EQUALS(all_items.size(), vec.size());
   for(size_t i = 0; i < all_items.size(); i++) {
     TS_ASSERT_EQUALS(all_items[i], vec[i]);
   }
  }
};
