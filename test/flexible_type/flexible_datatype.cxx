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
/*
 * Copyright (c) 2013 GraphLab Inc.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://graphlab.com
 *
 */

#include <vector>
#include <iostream>
#include <typeinfo>       // operator typeid

#include <cxxtest/TestSuite.h>

#include <flexible_type/flexible_type.hpp>
#include <flexible_type/flexible_type_record.hpp>
#include <flexible_type/flexible_type_registry.hpp>
#include <flexible_type/flexible_type_converter.hpp>

using namespace graphlab;

#include <graphlab/macros_def.hpp>
class flexible_datatype_test : public CxxTest::TestSuite {

  public:

    void test_registry_register() {
      std::cout << sizeof(flexible_type) << "\n";
      std::cout << std::endl;
      std::cout << "Testing flexible datatypes - registry" << std::endl;

      // Arrange
      flexible_type_registry registry;

      // Act
      int id_idx = registry.register_field("id", (int64_t)1);
      int pg_idx = registry.register_field("pagerank", 1.0);
      int name_idx = registry.register_field("name", std::string("empty"));
      std::vector<double> latent = {1.0};
      int latent_idx = registry.register_field("latent", latent);

      // Assert
      TS_ASSERT_EQUALS(id_idx, 0);
      TS_ASSERT_EQUALS(pg_idx, 1);
      TS_ASSERT_EQUALS(name_idx, 2);
      TS_ASSERT_EQUALS(latent_idx, 3);
    }

    void test_registry_unregister() {
      // Arrange
      flexible_type_registry registry;

      // Act
      int pg_idx = registry.register_field("pagerank", 4.0);
      registry.unregister_field("pagerank");

      int name_idx = registry.register_field("pagerank", std::string("something"));

      // Assert
      // unregistering does not decrement the counter, so expect 1
      // as index here
      TS_ASSERT_EQUALS(0, pg_idx);
      TS_ASSERT_EQUALS(1, name_idx);

    }

    void test_registry_get_field_id() {
      // Arrange
      flexible_type_registry registry;

      // Act
      int id_idx = registry.register_field("id", (int64_t)1);
      int again_idx = registry.get_field_id("id");
      int fail_id = registry.get_field_id("blah");

      // Assert
      TS_ASSERT_EQUALS(id_idx, again_idx);
      TS_ASSERT_EQUALS(fail_id, -1);
    }

    void test_basic_record() {
      // Arrange
      std::cout << std::endl;
      std::cout << "Testing flexible datatypes - record" << std::endl;

      flexible_type_registry registry;
      int int_idx = registry.register_field("a", (int64_t)0);
      registry.register_field("b", (int64_t)0);
      registry.register_field("c", std::string("hello"));
      registry.register_field("d", 0.0);
      TS_ASSERT_EQUALS(registry.register_field("b", 0.0), field_id_type(-1));

      flexible_type_record one;

      // Act
      int field_idx = one.add_field(int_idx, (int64_t)101110);

      // Assert
      TS_ASSERT_EQUALS(0, field_idx);
    }

    void test_record_add_field_search_and_remove() {
      flexible_type_record record;

      record.add_field(0, 5.5);
      record.add_field(1, std::string("hello"));
      record.add_field(5, 6);

      TS_ASSERT(record.has_field(0));
      TS_ASSERT(record.has_field(1));
      TS_ASSERT(record.has_field(5));
      TS_ASSERT_EQUALS(record[0].get_type(), flex_type_enum::FLOAT);
      TS_ASSERT_EQUALS(record[1].get_type(), flex_type_enum::STRING);
      TS_ASSERT_EQUALS(record[5].get_type(), flex_type_enum::INTEGER);

      record[10] = "mu";
      TS_ASSERT(record.has_field(10));
      TS_ASSERT_EQUALS(record[10].get_type(), flex_type_enum::STRING);
      TS_ASSERT_EQUALS(record[0], 5.5);
      TS_ASSERT_EQUALS((std::string)record[1], "hello");
      TS_ASSERT_EQUALS(record[5], 6);
      TS_ASSERT_EQUALS((std::string)record[10], "mu");

      //remove a string field
      record.remove_field(1);


      // make sure all the reamining fields are there
      // and values are still good
      TS_ASSERT(record.has_field(0));
      TS_ASSERT(!record.has_field(1));
      TS_ASSERT(record.has_field(5));
      TS_ASSERT(record.has_field(10));
      TS_ASSERT_EQUALS(record[0], 5.5);
      TS_ASSERT_EQUALS(record[5], 6);
      TS_ASSERT_EQUALS((std::string)record[10], "mu");

      // add the field back
      record[1] = std::string("hello");


      TS_ASSERT(record.has_field(0));
      TS_ASSERT(record.has_field(1));
      TS_ASSERT(record.has_field(5));
      TS_ASSERT(record.has_field(10));
      TS_ASSERT_EQUALS(record[0], 5.5);
      TS_ASSERT_EQUALS((std::string)record[1], "hello");
      TS_ASSERT_EQUALS(record[5], 6);
      TS_ASSERT_EQUALS((std::string)record[10], "mu");

      // serialize the record
      std::stringstream strm;
      oarchive oarc(strm);
      oarc << record;
      // remove everything
      record.remove_field(0);
      record.remove_field(1);
      record.remove_field(5);
      record.remove_field(10);
      TS_ASSERT(!record.has_field(0));
      TS_ASSERT(!record.has_field(1));
      TS_ASSERT(!record.has_field(5));
      TS_ASSERT(!record.has_field(10));
      // load it back
      iarchive iarc(strm);
      iarc >> record;

      TS_ASSERT(record.has_field(0));
      TS_ASSERT(record.has_field(1));
      TS_ASSERT(record.has_field(5));
      TS_ASSERT(record.has_field(10));
      TS_ASSERT_EQUALS(record[0], 5.5);
      TS_ASSERT_EQUALS((std::string)record[1], "hello");
      TS_ASSERT_EQUALS(record[5], 6);
      TS_ASSERT_EQUALS((std::string)record[10], "mu");
    }

    void test_containers() {
      std::vector<flexible_type> f;
      f.emplace_back(123);
      f.emplace_back("hello world");
      std::map<flexible_type, std::vector<flexible_type> > m;
      m["123"].push_back(123);

      flexible_type e("234");
      m[e].push_back(e);

    }

  void test_types_long() {
    flexible_type f = 1;
    flexible_type f2 = 2;

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::INTEGER);

    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT_EQUALS(f, 1);

    TS_ASSERT_DIFFERS(f, f2);
    TS_ASSERT_DIFFERS(f2, 1);

    long x = f;
    TS_ASSERT_EQUALS(x, 1);

    double xd = f;
    TS_ASSERT_EQUALS(xd, 1);
  }

  void test_types_double() {
    flexible_type f = 1.0;
    flexible_type f2 = 2.0;

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::FLOAT);

    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT_EQUALS(f, 1.0);
    TS_ASSERT_DIFFERS(f, f2);
    TS_ASSERT_DIFFERS(f2, 1.0);

    double x = f;

    TS_ASSERT_EQUALS(x, 1.0);
  }

  void test_types_string() {
    flexible_type f = "Hey man!";
    flexible_type f2 = "Hay man!";

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::STRING);

    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT_EQUALS(f, "Hey man!");
    TS_ASSERT_DIFFERS(f, f2);
    TS_ASSERT_DIFFERS(f2, "Hey man!");

    std::string s = f;

    TS_ASSERT_EQUALS(s, "Hey man!");
  }

  void test_types_vector() {
    std::vector<double> v = {1.0, 2.0};
    std::vector<double> v2 = {2.0, 1.0};

    flexible_type f = v;
    flexible_type f2 = v2;

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::VECTOR);

    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT_EQUALS(f[0], 1.0);
    TS_ASSERT_EQUALS(f[1], 2.0);
    TS_ASSERT_DIFFERS(f, f2);

    std::vector<double> v3 = f;
    TS_ASSERT(v == v3);
  }
  void test_types_recursive() {
    std::vector<flexible_type> v = {flexible_type(1.0), flexible_type("hey")};
    std::vector<flexible_type> v2 = {flexible_type("hey"), flexible_type(1.0)};

    flexible_type f = v;
    flexible_type f2 = v2;

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::LIST);

    if (f != f) {
      std::cout << f << std::endl;
    }
    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT(f(0) == 1.0);
    TS_ASSERT(f(1) == std::string("hey") );
    TS_ASSERT_DIFFERS(f, f2);

    std::vector<flexible_type> v3 = f;

    TS_ASSERT(v == v3);

    v = {flexible_type(1.0), flexible_type("hey")};
    v2 = {flexible_type(2.0), flexible_type("hoo")};
    f = v;
    f2 = v2;
    TS_ASSERT(f < f2);
    TS_ASSERT(!(f2 < f));

    v = {flexible_type(1.0), flexible_type("hey")};
    v2 = {flexible_type(1.0), flexible_type("hey")};
    f = v;
    f2 = v2;
    TS_ASSERT(f == f2);
    TS_ASSERT(!(f2 < f));
    TS_ASSERT(!(f2 > f));

    v = {flexible_type(1.0), flexible_type("hey")};
    v2 = {flexible_type(1.0), flexible_type("hoo")};
    f = v;
    f2 = v2;
    TS_ASSERT(f != f2);
    TS_ASSERT(f < f2);
    TS_ASSERT(!(f > f2));

    v = {flexible_type(1.0), flexible_type("hey")};
    v2 = {flexible_type(1.0)};
    f = v;
    f2 = v2;
    TS_ASSERT(f != f2);
    TS_ASSERT(f > f2);
    TS_ASSERT(!(f < f2));

    v = {flexible_type(1.0)};
    v2 = {flexible_type(1.0), flexible_type("hey")};
    f = v;
    f2 = v2;
    TS_ASSERT(f != f2);
    TS_ASSERT(f < f2);
    TS_ASSERT(!(f > f2));

    v = {flexible_type(1.0)};
    v2 = {flexible_type(1.0)};
    f = v;
    f2 = v2;
    TS_ASSERT(f == f2);
    TS_ASSERT(!(f < f2));
    TS_ASSERT(!(f > f2));
  }

  void test_types_dict() {
    flexible_type vector_v = flex_vec{1,2,3};

    std::vector<std::pair<flexible_type, flexible_type>> m{
      std::make_pair(flexible_type("foo"), flexible_type(1.0)),
      std::make_pair(flexible_type(123), flexible_type("string")),
      std::make_pair(vector_v, vector_v)
    };

    // same as m but different order
    std::vector<std::pair<flexible_type, flexible_type>> m2{
      std::make_pair(vector_v, vector_v),
      std::make_pair(flexible_type(123), flexible_type("string")),
      std::make_pair(flexible_type("foo"), flexible_type(1.0))
    };

    // different length
    std::vector<std::pair<flexible_type, flexible_type>> m3{
      std::make_pair(flexible_type("foo"), flexible_type(1.0)),
    };

    // same length but different key
    std::vector<std::pair<flexible_type, flexible_type>> m4{
      std::make_pair(flexible_type("fooo"), flexible_type(2.0)),
      std::make_pair(flexible_type(1234), flexible_type("string2")),
      std::make_pair(vector_v, vector_v)
    };

    // same key but different value
    std::vector<std::pair<flexible_type, flexible_type>> m5{
      std::make_pair(flexible_type("foo"), flexible_type(2.0)),
      std::make_pair(flexible_type(123), flexible_type("string2")),
      std::make_pair(vector_v, flexible_type(1))
    };


    flexible_type f = m;
    flexible_type f2 = m2;
    flexible_type f3 = m3;
    flexible_type f4 = m4;
    flexible_type f5 = m5;

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::DICT);

    TS_ASSERT_EQUALS(f, f);
    TS_ASSERT_EQUALS(f2, f2);
    TS_ASSERT_EQUALS(f3, f3);
    TS_ASSERT_EQUALS(f4, f4);
    TS_ASSERT_EQUALS(f5, f5);

    TS_ASSERT_EQUALS(f, f2);

    TS_ASSERT_DIFFERS(f, f3);
    TS_ASSERT_DIFFERS(f, f4);
    TS_ASSERT_DIFFERS(f, f5);

    std::vector<std::pair<flexible_type, flexible_type>> new_f = f.get<flex_dict>();

    TS_ASSERT(new_f == m);

    flexible_type v1 = f.dict_at("foo");
    flexible_type v2 = f.dict_at(123);
    flexible_type v3 = f.dict_at(vector_v);
    TS_ASSERT_THROWS_ANYTHING(f.dict_at("non exist key"));

    TS_ASSERT_EQUALS(v1, 1.0);
    TS_ASSERT_EQUALS(v2, "string");
    TS_ASSERT_EQUALS(v3, vector_v);

    TS_ASSERT_EQUALS(v1.get_type(), flex_type_enum::FLOAT);
    TS_ASSERT_EQUALS(v2.get_type(), flex_type_enum::STRING);
    TS_ASSERT_EQUALS(v3.get_type(), flex_type_enum::VECTOR);

    // erase
    f.erase("foo");
    TS_ASSERT_EQUALS(f.dict_at(123), "string");
    TS_ASSERT_THROWS_ANYTHING(f.dict_at("foo"));
    TS_ASSERT_THROWS_ANYTHING(f.dict_at("123"));

  }

  void test_types_enum() {

    // For use in variant_type 
    
    enum class TestEnum {A, B, C};

    flexible_type_converter<TestEnum> converter; 
    
    flexible_type f = converter.set(TestEnum::A);
    flexible_type f2 = converter.set(TestEnum::A);
    flexible_type f3 = converter.set(TestEnum::B);

    TS_ASSERT_EQUALS(f.get_type(), flex_type_enum::INTEGER);

    TS_ASSERT(f == f2);
    TS_ASSERT(f != f3);

    TestEnum x = converter.get(f);
    TestEnum x2 = converter.get(f2);
    TestEnum x3 = converter.get(f3);

    TS_ASSERT_EQUALS(x, TestEnum::A);
    TS_ASSERT_EQUALS(x2, TestEnum::A);
    TS_ASSERT_EQUALS(x3, TestEnum::B);
  }

}; // class

#include <graphlab/macros_undef.hpp>
