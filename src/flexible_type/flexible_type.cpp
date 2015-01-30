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
#include <boost/date_time/posix_time/posix_time.hpp>
#include <flexible_type/flexible_type.hpp>

// contains some of the bigger functions I do not want to inline
namespace graphlab {

namespace flexible_type_impl {


boost::posix_time::ptime my_from_time_t(std::time_t offset){
  static const boost::posix_time::ptime time_t_epoch = boost::posix_time::from_time_t(0);
  static const int32_t max_int32 = std::numeric_limits<int32_t>::max();
  static const int32_t min_int32 = (std::numeric_limits<int32_t>::min() + 1);
  boost::posix_time::ptime accum = time_t_epoch;
  if(offset < 0 ){

    while (offset < min_int32)
    {   
      accum  += boost::posix_time::seconds(min_int32);
      offset -= min_int32;
    }
    accum += boost::posix_time::seconds(offset);
  } else { 

    while (offset > max_int32)
    {   
      accum  += boost::posix_time::seconds(max_int32);
      offset -= max_int32;
    }  
    accum += boost::posix_time::seconds(offset);
  }   

  return accum;
}

flex_int my_to_time_t(const boost::posix_time::ptime & time) { 
  boost::posix_time::time_duration dur = time - boost::posix_time::from_time_t(0);
  return  static_cast<long>(dur.ticks() / dur.ticks_per_second());
}


std::string date_time_to_string(const flex_date_time& i) {
 return to_iso_string(my_from_time_t(i.first + (i.second * 1800))); 
}
flex_string get_string_visitor::operator()(const flex_vec& vec) const {
  std::stringstream strm; strm << "[";
  for (size_t i = 0; i < vec.size(); ++i) {
    strm << vec[i];
    if (i + 1 < vec.size()) strm << " ";
  }
  strm << "]";
  return strm.str();
}

flex_string get_string_visitor::operator()(const flex_date_time& i) const { 
  return date_time_to_string(i);
}
flex_string get_string_visitor::operator()(const flex_list& vec) const {
  std::stringstream strm; strm << "[";
  for (size_t i = 0; i < vec.size(); ++i) {
    if (vec[i].get_type() == flex_type_enum::STRING) {
      strm << "\"" << (flex_string)(vec[i]) << "\"";
    } else {
      strm << (flex_string)(vec[i]);
    }
    if (i + 1 < vec.size()) strm << ",";
  }
  strm << "]";
  return strm.str();
}

flex_string get_string_visitor::operator()(const flex_dict& vec) const {
  std::stringstream strm; strm << "{";
  size_t vecsize = vec.size();
  size_t i = 0;
  for(const auto& val: vec) {
    if (val.first.get_type() == flex_type_enum::STRING) {
      strm << "\"" << (flex_string)(val.first) << "\"";
    } else {
      strm << (flex_string)(val.first);
    }
    strm << ":";
    if (val.second.get_type() == flex_type_enum::STRING) {
      strm << "\"" << (flex_string)(val.second) << "\"";
    } else {
      strm << (flex_string)(val.second);
    }
    if (i + 1 < vecsize) strm << ", ";
    ++i;
  }
  strm << "}";
  return strm.str();
}

 flex_string get_string_visitor::operator() (const flex_image& img) const {
  std::stringstream strm; 
  strm << "Height: " << img.m_height;
  strm << " Width: " << img.m_width;
  
  return strm.str();
 }

 flex_vec get_vec_visitor::operator() (const flex_image& img) const {
  flex_vec vec;
  ASSERT_MSG(img.m_format == Format::RAW_ARRAY, "Cannot convert encoded image to array");
  for (size_t i = 0 ; i < img.m_image_data_size; ++i){
    vec.push_back(static_cast<double>(static_cast<unsigned char>(img.m_image_data[i])));
  }
  return vec;
 }


void soft_assignment_visitor::operator()(flex_list& t, const flex_vec& u) const {
  t.resize(u.size(), flexible_type(flex_type_enum::FLOAT));
  for (size_t i = 0;i < u.size(); ++i) {
    t[i] = u[i];
  }
}

bool approx_equality_operator::operator()(const flex_dict& t, const flex_dict& u) const {
    if (t.size() != u.size()) return false;

    std::unordered_multimap<flexible_type, flexible_type> d1(t.begin(), t.end());
    std::unordered_multimap<flexible_type, flexible_type> d2(u.begin(), u.end());

    return d1 == d2;
}

bool approx_equality_operator::operator()(const flex_list& t, const flex_list& u) const {
  if (t.size() != u.size()) return false;
  for (size_t i = 0; i < t.size(); ++i) if (t[i] != u[i]) return false;
  return true;
}

size_t city_hash_visitor::operator()(const flex_list& t) const {
  size_t h = 0;
  for (size_t i = 0; i < t.size(); ++i) {
    h = hash64_combine(h, t[i].hash());
  }
  // The final hash is needed to distinguish nested types
  return graphlab::hash64(h);
}

size_t city_hash_visitor::operator()(const flex_dict& t) const {
  size_t key_hash = 0;
  size_t value_hash = 0;
  for(const auto& val: t) {
    // Use xor to ignore the order of the pairs.
    key_hash |= val.first.hash();
    value_hash |= val.first.hash();
  }
  return graphlab::hash64_combine(key_hash, value_hash);
}

uint128_t city_hash128_visitor::operator()(const flex_list& t) const {
  uint128_t h = 0;
  for (size_t i = 0; i < t.size(); ++i) {
    h = hash128_combine(h, t[i].hash128());
  }
  // The final hash is needed to distinguish nested types
  return graphlab::hash128(h);
}

uint128_t city_hash128_visitor::operator()(const flex_dict& t) const {
  uint128_t key_hash = 0;
  uint128_t value_hash = 0;
  for(const auto& val: t) {
    // Use xor to ignore the order of the pairs.
    key_hash |= val.first.hash128();
    value_hash |= val.first.hash128();
  }
  return graphlab::hash128_combine(key_hash, value_hash);
}
} // namespace flexible_type_impl


void flexible_type::erase(const flexible_type& index) {
  ensure_unique();
  switch(get_type()) {
   case flex_type_enum::DICT: {
     flex_dict& value = val.dictval->second;
     for(auto iter = value.begin(); iter != value.end(); iter++) {
       if ((*iter).first == index) {
         value.erase(iter);
         break;
       }
     }
    return;
   }
   default:
     FLEX_TYPE_ASSERT(false);
  }
}

bool flexible_type::is_zero() const {
  switch(get_type()) {
    case flex_type_enum::INTEGER:
      return get<flex_int>() == 0;
    case flex_type_enum::FLOAT:
      return get<flex_float>() == 0.0;
    case flex_type_enum::STRING:
      return get<flex_string>().empty();
    case flex_type_enum::VECTOR:
      return get<flex_vec>().empty();
    case flex_type_enum::LIST:
      return get<flex_list>().empty();
    case flex_type_enum::DICT:
      return get<flex_dict>().empty();
    case flex_type_enum::IMAGE:
      return get<flex_image>().m_format == Format::UNDEFINED;
    case flex_type_enum::UNDEFINED:
      return true;
    default:
      log_and_throw("Unexpected type!");
  };
  __builtin_unreachable();
}

bool flexible_type::is_na() const {
  auto the_type = get_type();
  return (the_type == flex_type_enum::UNDEFINED) ||
          (the_type == flex_type_enum::FLOAT && std::isnan(get<flex_float>()));
}


void flexible_type_fail(bool success) {
  if(!success) log_and_throw("Invalid type conversion");
}
} // namespace graphlab
