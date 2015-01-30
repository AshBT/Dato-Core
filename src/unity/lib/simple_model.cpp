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
#include <fileio/general_fstream.hpp>
#include <unity/lib/simple_model.hpp>
#include <unity/lib/variant_deep_serialize.hpp>

namespace graphlab {

std::vector<std::string> simple_model::list_keys() {
  std::vector<std::string> keys;
  for(auto &kv: params) {
    keys.push_back(kv.first);
  }
  return keys;
}

variant_type simple_model::get_value(std::string key, variant_map_type& opts) {
  if (params.count(key)) {
    return params[key];
  } else {
    log_and_throw("Key " + key + " not found in model.");
  }
}

simple_model::~simple_model() { }

size_t simple_model::get_version() const {
  return SIMPLE_MODEL_VERSION;
}

void simple_model::save_impl(oarchive& oarc) const {
    oarc << params.size();
    for (const auto& kv : params) {
      oarc << kv.first;
      variant_deep_save(params.at(kv.first), oarc);
    }
}

void simple_model::load_version(iarchive& iarc, size_t version) {
  ASSERT_MSG(version == SIMPLE_MODEL_VERSION, "This model version cannot be loaded. Please re-save your model.");
  size_t size;
  iarc >> size;
  for (size_t i = 0; i < size; ++i) {
    std::string key;
    iarc >> key;
    variant_deep_load(params[key], iarc);
  }
}

} // namespace graphlab
