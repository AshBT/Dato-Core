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
#ifndef GRAPHLAB_INI_BOOST_PROPERTY_TREE_UTILS_HPP
#define GRAPHLAB_INI_BOOST_PROPERTY_TREE_UTILS_HPP

#define BOOST_SPIRIT_THREADSAFE

#include <map>
#include <vector>
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <logger/logger.hpp>
namespace graphlab {
namespace ini {

/**
 * Reads a key in an ini file as a sequence of values. In the ini file this will
 * be represented as 
 *
 * [key]
 * 0000 = "hello"
 * 0001 = "pika"
 * 0002 = "chu"
 * 
 * This will return a 3 element vector containing {"hello", "pika", "chu"}
 *
 * \see write_sequence_section
 */
template <typename T>
std::vector<T> read_sequence_section(const boost::property_tree::ptree& data, 
                                     std::string key,
                                     size_t expected_elements) {
  std::vector<T> ret;
  if (expected_elements == 0) return ret;
  const boost::property_tree::ptree& section = data.get_child(key);
  ret.resize(expected_elements);

  // loop through the children of the column_names section
  for(auto val: section) {
    size_t sid = std::stoi(val.first);
    if (sid >= ret.size()) {
      log_and_throw(std::string("Invalid ID in ") + key + " section."
                    "Segment IDs are expected to be sequential.");
    }
    ret[sid] = val.second.get_value<T>();
  }
  return ret;
}




/**
 * Reads a key in an ini file as a dictionary of values. In the ini file this will
 * be represented as 
 *
 * [key]
 * fish = "hello"
 * and = "pika"
 * chips = "chu"
 *
 * This will return a 3 element map containing 
 * {"fish":"hello", "and":"pika", "chips":"chu"}.
 *
 * \see write_dictionary_section
 */
template <typename T>
std::map<std::string, T> read_dictionary_section(const boost::property_tree::ptree& data, 
                                                 std::string key) {
  std::map<std::string, T> ret;
  // no section found
  if (data.count(key) == 0) {
    return ret;
  }
  const boost::property_tree::ptree& section = data.get_child(key);

  // loop through the children of the column_names section
  for(auto val: section) {
      ret.insert(std::make_pair(val.first,
                                val.second.get_value<T>()));
  }
  return ret;
}


/**
 * Writes a vector of values into an ini file as a section. 
 *
 * For instance, given a 3 element vector containing {"hello", "pika", "chu"}
 * The vector be represented as 
 *
 * [key]
 * 0000 = "hello"
 * 0001 = "pika"
 * 0002 = "chu"
 *
 * \see read_sequence_section
 * 
 */
template <typename T>
void write_sequence_section(boost::property_tree::ptree& data, 
                            const std::string& key,
                            const std::vector<T>& values) {
  for (size_t i = 0; i < values.size(); ++i) {
    // make the 4 digit suffix
    std::stringstream strm;
    strm.fill('0'); strm.width(4); strm << i;
    data.put(key + "." + strm.str(), values[i]);
  }
}

/**
 * Writes a dictionary of values into an ini file as a section. 
 * For instance, given a 3 element map containing 
 * {"fish":"hello", "and":"pika", "chips":"chu"}.
 *
 * In the ini file this will be represented as:
 *
 * [key]
 * fish = "hello"
 * and = "pika"
 * chips = "chu"
 *
 * \see read_dictionary_section
 *
 */
template <typename T>
void write_dictionary_section(boost::property_tree::ptree& data, 
                            const std::string& key,
                            const std::map<std::string, T>& values) {
  // Write out metadata
  std::string m_heading = key + ".";
  for(const auto& map_entry : values) {
    std::string mkey(m_heading);
    mkey += map_entry.first;
    data.put(mkey, map_entry.second);
  }
}


} // ini
} // namespace graphlab
#endif
