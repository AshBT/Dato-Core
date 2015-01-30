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
#include <string>
#include <random>
#include <set>
#include <sstream>
#include <vector>
#include <algorithm>
#include <util/cityhash_gl.hpp>
#include <util/hash_value.hpp>

using namespace graphlab;

static const size_t test_chain_length = 100000;

template <typename H, typename V>
class hash_tracker {
 public:
  void check_and_add(H h, V v) {

    auto it = seen_hashes.find(h);

    if(it != seen_hashes.end()) {

      V old_value = it->second;

      if(old_value != v) {
        std::ostringstream ss;

        ss << "Hash of '" << old_value << "' and '" << v << "' map collide.";

        TS_FAIL(ss.str());
      }
    }

    seen_hashes[h] = v;
  }

 private:
  std::map<H, V> seen_hashes;
};

class hash_function_test : public CxxTest::TestSuite {
 public:

  hash_function_test()
      : values(4*test_chain_length)
  {
    std::default_random_engine generator(0);

    for(size_t i = 0; i < test_chain_length; ++i) {
      values[i] = long(i);
    }

    for(size_t i = 0; i < test_chain_length; ++i) {
      int bit = std::uniform_int_distribution<int>(0,8*sizeof(long))(generator);

      values[test_chain_length + i] = values[test_chain_length + i - 1] ^ long(1UL << bit);
    }

    for(size_t i = 0; i < 2*test_chain_length; ++i) {
      values[2*test_chain_length + i] = i;
    }
  }

  std::vector<long> values;

  void test_string_hashes_128() {
    hash_tracker<uint128_t, std::string> htest;

    for(long v : values) {
      std::string s = std::to_string(v);

      uint128_t h1 = hash128(s);
      uint128_t h2 = hash128(s.c_str(), s.size());

      TS_ASSERT_EQUALS(h1, h2);

      htest.check_and_add(h1, s);
    }
  }

  void test_string_hashes_128_by_hash_value() {
    hash_tracker<hash_value, std::string> htest;

    for(long v : values) {
      std::string s = std::to_string(v);

      htest.check_and_add(s, s);
    }
  }
  
  void test_string_hashes_64() {
    hash_tracker<uint64_t, std::string> htest;

    for(long v : values) {
      std::string s = std::to_string(v);

      uint64_t h1 = hash64(s);
      uint64_t h2 = hash64(s.c_str(), s.size());

      TS_ASSERT_EQUALS(h1, h2);

      htest.check_and_add(h1, s);
    }
  }

  void test_integer_hashes_128() {
    hash_tracker<uint128_t, long> htest;

    for(long v : values) {
      htest.check_and_add(hash128(v), v);
    }
  }

  void test_integer_hashes_128_by_hash_value() {
    hash_tracker<hash_value, long> htest;

    for(long v : values) {
      htest.check_and_add(v, v);
    }
  }
  
  void test_integer_hashes_64() {
    hash_tracker<uint64_t, long> htest;

    for(long v : values) {
      htest.check_and_add(hash64(v), v);
    }
  }  
};
