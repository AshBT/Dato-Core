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
#ifndef GRAPHLAB_GLOBALS_GLOBALS_HPP
#define GRAPHLAB_GLOBALS_GLOBALS_HPP
#include <string>
#include <vector>
#include <map>
#include <utility>
#include <functional>
namespace graphlab {

class flexible_type;

namespace globals {

template <typename T>
struct register_global;

/**
 * Registers an integrap point global value. 
 *
 * \param The name of the value. 
 * GRAPHLAB_{name} will be the environment variable which modifies this value.
 * By convention, this should be in all caps
 * \param value A reference to the value. 
 * Value should be initialized the the default values.
 * \param runtime_modifiable Whether this can be set only by environment 
 * variables or at runtime.
 * \param value_check A callback function which checks the value of the value.
 * This should be a simple lambda. (preferable +[](double) {...}) and should not
 * capture anything. If the lambda returns true, the value changes are accepted.
 * Defaults to nullptr in which case all value changes will be accepted.
 */
template <>
struct register_global<int64_t>{
  register_global(std::string name, 
                  int64_t* value, 
                  bool runtime_modifiable,
                  std::function<bool(int64_t)> value_check = nullptr);
};

/**
 * Registers a floating point global value. 
 *
 * \param The name of the value. 
 * GRAPHLAB_{name} will be the environment variable which modifies this value.
 * By convention, this should be in all caps
 * \param value A reference to the value. 
 * Value should be initialized the the default values.
 * \param runtime_modifiable Whether this can be set only by environment 
 * variables or at runtime.
 * \param value_check A callback function which checks the value of the value.
 * This should be a simple lambda. (preferable +[](double) {...}) and should not
 * capture anything. If the lambda returns true, the value changes are accepted.
 * Defaults to nullptr in which case all value changes will be accepted.
 */
template <>
struct register_global<double>{
  register_global(std::string name, 
                  double* value, 
                  bool runtime_modifiable,
                  std::function<bool(double)> value_check = nullptr);
};


/**
 * Registers a string global value. 
 *
 * \param The name of the value. 
 * GRAPHLAB_{name} will be the environment variable which modifies this value.
 * By convention, this should be in all caps
 * \param value A reference to the value. 
 * Value should be initialized the the default values.
 * \param runtime_modifiable Whether this can be set only by environment 
 * variables or at runtime.
 * \param value_check A callback function which checks the value of the value.
 * This should be a simple lambda. (preferable +[](double) {...}) and should not
 * capture anything. If the lambda returns true, the value changes are accepted.
 * Defaults to nullptr in which case all value changes will be accepted.
 */
template <>
struct register_global<std::string>{
  register_global(std::string name, 
                  std::string* value, 
                  bool runtime_modifiable,
                  std::function<bool(std::string)> value_check = nullptr);
};



/**
 * Lists all the global values. If runtime_modifiable == true, lists all 
 * global values which can be modified at runtime. If runtime == false, lists
 * all global values which can only be modified by environment variables.
 */
std::vector<std::pair<std::string, flexible_type> > list_globals(bool runtime_modifiable);

/**
 * Gets a value of a single global value. There are a few builtins.
 * UNITY_SERVER_BINARY: The location of the unity_server executable (for instance ./unity_server)
 * UNITY_SERVER_PATH: The path of the unity_server executable (for instance 
 *
 */
flexible_type get_global(std::string name);

enum class set_global_error_codes {
  SUCCESS = 0,
  NO_NAME = 1,
  NOT_RUNTIME_MODIFIABLE = 2,
  INVALID_VAL = 3
};
/**
 * Sets a modifiable global value. Return set_global_error_codes::SUCCESS (0)
 * on success and otherwise on failure.
 */
set_global_error_codes set_global(std::string name, flexible_type val);

/**
 * Initialize all registered global variables from environment variables.
 * Also initializes the globals GLOBALS_MAIN_PROCESS_BINARY and 
 * GLOBALS_MAIN_PROCESS_PATH from argv[0] which must be passed in from main()
 */
void initialize_globals_from_environment(std::string argv0);

} // namespace globals
} // namespace graphlab

/**
 * Register a global variable.
 * 
 * \param type The type of the variable. Must be integral, string, or double.
 * \param varname The variable name. This variable can then be modified by
 * setting the environment variable with the same name as the variable (
 * prefixxed with GRAPHLAB_)
 * \param runtime_modifiable If false, this variable can only be modified by
 * setting an environment variable. If true, it can be changed at runtime to
 * any value.
 *
 * Example:
 * REGISTER_GLOBAL(size_t, GLOBAL_PIKA_PIKA, false)
 *
 * Then setting the environment variable GRAPHLAB_GLOBAL_PIKA_PIKA will 
 * change the value of the variable.
 *
 * REGISTER_GLOBAL(size_t, GLOBAL_PIKA_PIKA, true)
 * 
 * Will allow the variable to be modified at runtime via Python.
 */
#define REGISTER_GLOBAL(type, varname, runtime_modifiable)          \
  static auto __ ## varname ## __register__ ## instance =           \
      ::graphlab::globals::register_global                          \
                       <type>("GRAPHLAB_" #varname,                 \
                              (type*)(&varname),                    \
                              runtime_modifiable);     


/**
 * Register a global variable with value checking.
 * 
 * \param type The type of the variable. Must be integral, string, or double.
 * \param varname The variable name. This variable can then be modified by
 * setting the environment variable with the same name as the variable (
 * prefixxed with GRAPHLAB_)
 * \param runtime_modifiable If false, this variable can only be modified by
 * setting an environment variable. If true, it can be changed at runtime to
 * any value.
 * \param lambda A function pointer / lambda function which checks the value
 * when a value change is requested. The function should take a single input
 * of the new value, and return a boolean value. (true for good, and false for
 * bad). This lambda should be self contained and should not be dependent on 
 * other global objects since this will cause order of construction/destruction 
 * issues.
 *
 * Example:
 * REGISTER_GLOBAL_WITH_CHECKS(size_t, GLOBAL_PIKA_PIKA, false.
 *                             +[](size_t i){ return i >= 1024})
 *
 * Then setting the environment variable GRAPHLAB_GLOBAL_PIKA_PIKA will 
 * change the value of the variable but it will only succeed if the value
 * is greater than 1024.
 *
 * REGISTER_GLOBAL(size_t, GLOBAL_PIKA_PIKA, true)
 * 
 * Will allow the variable to be modified at runtime via Python
 * But the change will only succeed if the value is greater than 1024.
 */
#define REGISTER_GLOBAL_WITH_CHECKS(type, varname, runtime_modifiable, lambda) \
  static auto __ ## varname ## __register__ ## instance =                      \
      ::graphlab::globals::register_global                                     \
                       <type>("GRAPHLAB_" #varname,                            \
                              (type*)(&varname),                               \
                              runtime_modifiable,                              \
                              lambda);     

#endif
