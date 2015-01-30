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
#include <cstdlib>
#include <boost/variant.hpp>
#include <boost/filesystem.hpp>
#include <logger/logger.hpp>
#include <flexible_type/flexible_type.hpp>
#include <globals/globals.hpp>
namespace graphlab {

std::string GLOBALS_MAIN_PROCESS_BINARY;
std::string GLOBALS_MAIN_PROCESS_PATH;

REGISTER_GLOBAL(std::string, GLOBALS_MAIN_PROCESS_BINARY, false);
REGISTER_GLOBAL(std::string, GLOBALS_MAIN_PROCESS_PATH, false);

namespace globals {

template <typename T>
struct value_and_value_check {
  value_and_value_check() = default;
  value_and_value_check(const value_and_value_check<T>&) = default;
  value_and_value_check(value_and_value_check<T>&&) = default;
  value_and_value_check& operator=(const value_and_value_check<T>&) = default;
  value_and_value_check& operator=(value_and_value_check<T>&&) = default;

  value_and_value_check(T* value, std::function<bool(T)> value_check): 
      value(value), value_check(value_check) { }

  T* value = NULL;
  std::function<bool(T)> value_check;

  bool perform_check(const T& new_value) const {
    if (!value_check) return true;
    else return value_check(new_value);
  }

  T get_value() const {
    if (value) return *value;
    else return T();
  }

  bool set_value(const T& new_value) {
    if (!perform_check(new_value) || value == NULL) return false;
    (*value) = new_value;
    return true;
  }
};


struct get_value_visitor: public boost::static_visitor<flexible_type> {
  template <typename Value>
  flexible_type operator()(const Value& val) const {
    return flexible_type(val.get_value());
  }
};

struct set_value_visitor: public boost::static_visitor<bool> {
  flexible_type new_value;
  bool operator()(value_and_value_check<double>& val) const {
    if (new_value.get_type() == flex_type_enum::INTEGER || 
        new_value.get_type() == flex_type_enum::FLOAT) {
      return val.set_value((double)new_value);
    } else {
      return false;
    }
  }
  bool operator()(value_and_value_check<int64_t>& val) const {
    if (new_value.get_type() == flex_type_enum::INTEGER || 
        new_value.get_type() == flex_type_enum::FLOAT) {
      return val.set_value((int64_t)new_value);
    } else {
      return false;
    }
  }
  bool operator()(value_and_value_check<std::string>& val) const {
    if (new_value.get_type() == flex_type_enum::STRING) {
      return val.set_value((std::string)new_value);
    } else {
      return false;
    }
  }
};


struct set_value_from_string_visitor: public boost::static_visitor<bool> {
  std::string new_value;
  bool operator()(value_and_value_check<double>& val) const {
    try {
      return val.set_value(std::stod(new_value));
    } catch (...) {
      return false;
    }
  }
  bool operator()(value_and_value_check<int64_t>& val) const {
    try {
      return val.set_value(std::stol(new_value));
    } catch (...) {
      return false;
    }
  }
  bool operator()(value_and_value_check<std::string>& val) const {
    return val.set_value(new_value);
  }
};

struct global_value {
  std::string name;
  boost::variant<value_and_value_check<double>,
                 value_and_value_check<int64_t>,
                 value_and_value_check<std::string>> value;
  bool runtime_modifiable;
};

std::vector<global_value>& get_global_registry() {
  static std::vector<global_value> global_registry;
  return global_registry;
}

std::map<std::string, size_t>& get_global_registry_map() {
  static std::map<std::string, size_t> global_registry_map;
  return global_registry_map;
}

register_global<double>::register_global(std::string name, 
                             double* value, 
                             bool runtime_modifiable,
                             std::function<bool(double)> value_check) {
  get_global_registry_map()[name] = get_global_registry().size();
  get_global_registry().push_back(
      global_value{name, 
                   value_and_value_check<double>{value, value_check}, 
                   runtime_modifiable});
  if (runtime_modifiable) {
    logstream(LOG_INFO) << "Registering runtime modifiable configuration variable " << name 
                        << " = " << (*value) << " (double)" << std::endl;
  } else {
    logstream(LOG_INFO) << "Registering environment modifiable configuration variable " << name 
                        << " = " << (*value) << " (double)" << std::endl;
  }
}


register_global<int64_t>::register_global(std::string name, 
                             int64_t* value, 
                             bool runtime_modifiable,
                             std::function<bool(int64_t)> value_check) {
  get_global_registry_map()[name] = get_global_registry().size();
  get_global_registry().push_back(
      global_value{name, 
                   value_and_value_check<int64_t>{value, value_check}, 
                   runtime_modifiable});
  if (runtime_modifiable) {
    logstream(LOG_INFO) << "Registering runtime modifiable configuration variable " << name 
                        << " = " << (*value) << " (int64_t)" << std::endl;
  } else {
    logstream(LOG_INFO) << "Registering environment modifiable configuration variable " << name 
                        << " = " << (*value) << " (int64_t)" << std::endl;
  }
}

register_global<std::string>::register_global(std::string name, 
                             std::string* value, 
                             bool runtime_modifiable,
                             std::function<bool(std::string)> value_check) {
  get_global_registry_map()[name] = get_global_registry().size();
  get_global_registry().push_back(
      global_value{name, 
                   value_and_value_check<std::string>{value, value_check}, 
                   runtime_modifiable});
  if (runtime_modifiable) {
    logstream(LOG_INFO) << "Registering runtime modifiable configuration variable " << name 
                        << " = " << (*value) << " (string)" << std::endl;
  } else {
    logstream(LOG_INFO) << "Registering environment modifiable configuration variable " << name 
                        << " = " << (*value) << " (string)" << std::endl;
  }
}


std::vector<std::pair<std::string, flexible_type> > list_globals(bool runtime_modifiable) {
  std::vector<std::pair<std::string, flexible_type> > ret;
  for (auto& i: get_global_registry()) {
    if (i.runtime_modifiable == runtime_modifiable) {
      ret.push_back({i.name, boost::apply_visitor(get_value_visitor(), i.value)});
    }
  }
  return ret;
}

flexible_type get_global(std::string name) {
  if (get_global_registry_map().count(name) == 0) return FLEX_UNDEFINED;
  size_t idx = get_global_registry_map()[name];
  return boost::apply_visitor(get_value_visitor(), get_global_registry()[idx].value);
}

bool set_global_impl(std::string name, flexible_type val) {
  size_t idx = get_global_registry_map()[name];
  set_value_visitor visitor;
  visitor.new_value = val;
  return boost::apply_visitor(visitor, get_global_registry()[idx].value);
}

set_global_error_codes set_global(std::string name, flexible_type val) {
  if (get_global_registry_map().count(name) == 0) {
    logstream(LOG_INFO) << "Unable to change value of " << name << " to " << val
                        << ". No such configuration variable." << std::endl;
    return set_global_error_codes::NO_NAME;
  }
  size_t idx = get_global_registry_map()[name];
  if (get_global_registry()[idx].runtime_modifiable == false) {
    logstream(LOG_INFO) << "Unable to change value of " << name << " to " << val
                        << ". Variable is not runtime modifiable." << std::endl;
    return set_global_error_codes::NOT_RUNTIME_MODIFIABLE;
  }
  if (set_global_impl(name, val) == false) {
    logstream(LOG_INFO) << "Unable to change value of " << name << " to " << val
                        << ". Invalid value." << std::endl;
    return set_global_error_codes::INVALID_VAL;
  }
  return set_global_error_codes::SUCCESS;
}


void initialize_globals_from_environment(std::string argv0) {
  for (auto& i: get_global_registry()) {
    std::string envname = i.name;
    char* envval = getenv(envname.c_str());
    if (envval) {
      set_value_from_string_visitor visitor;
      visitor.new_value = envval;
      if (i.value.apply_visitor(visitor)) {
        logstream(LOG_EMPH) << "Setting configuration variable " << i.name 
                            << " to " << visitor.new_value << std::endl;
      } else {
        logstream(LOG_EMPH) << "Cannot set configuration variable " << i.name 
                            << " to " << visitor.new_value << std::endl;
      }
    }
  }

  // these two special variables cannot be environment overidden, 
  // so set them  last
  boost::filesystem::path argvpath(argv0);
  argvpath = boost::filesystem::absolute(argvpath);

  GLOBALS_MAIN_PROCESS_BINARY = argvpath.native();
  GLOBALS_MAIN_PROCESS_PATH = argvpath.parent_path().native();
//   logstream(LOG_INFO) << "Main process binary: " << GLOBALS_MAIN_PROCESS_BINARY << std::endl;
//   logstream(LOG_INFO) << "Main process path: " << GLOBALS_MAIN_PROCESS_PATH << std::endl;
}

} // globals
} // graphlab
