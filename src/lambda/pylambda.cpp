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
#include <lambda/pylambda.hpp>
#include <lambda/python_api.hpp>
#include <lambda/python_thread_guard.hpp>
#include <lambda/pyflexible_type.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>
#include <util/cityhash_gl.hpp>

namespace graphlab {

namespace lambda {

  namespace python = boost::python;

  extern python::object gc; // defined in python_api.cpp

  size_t pylambda_evaluator::make_lambda(const std::string& pylambda_str) {
    python_thread_guard py_thread_guard;
    try {
      python::object pickle = python::import("pickle");
      PyObject* lambda_bytes = PyByteArray_FromStringAndSize(pylambda_str.c_str(), pylambda_str.size());
      size_t hash_key = hash64(pylambda_str.c_str(), pylambda_str.size());
      m_lambda_hash[hash_key] = new python::object((pickle.attr("loads")(python::object(python::handle<>(lambda_bytes)))));
      logstream(LOG_DEBUG) << "make lambda" << hash_key << std::endl;
      return hash_key;
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    }
  }

  void pylambda_evaluator::release_lambda(size_t lambda_hash) {
    python_thread_guard py_thread_guard;
    logstream(LOG_DEBUG) << "release lambda" << lambda_hash << std::endl;
    auto lambda_obj = m_lambda_hash.find(lambda_hash);

    if (lambda_obj == m_lambda_hash.end()) {
      throw("Cannot find the lambda hash to release " + std::to_string(lambda_hash));
    }

    if (m_current_lambda_hash == lambda_hash) {
      m_current_lambda_hash = (size_t)(-1);
      m_current_lambda = NULL;
    }

    m_lambda_hash.erase(lambda_hash);
    delete m_lambda_hash[lambda_hash];

    // run gc to reclaim heap
    gc.attr("collect")();
  }

  flexible_type pylambda_evaluator::eval(size_t lambda_hash, const flexible_type& arg) {
    set_lambda(lambda_hash);
    try {
        python::object input = PyObject_FromFlex(arg);
        python::object output = m_current_lambda->operator()(input);
        return PyObject_AsFlex(output);
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    } catch (std::exception& e) {
      throw(std::string(e.what()));
    } catch (...) {
      throw("Unknown exception from python lambda evaluation.");
    }
  }

  /**
   * Bulk version of eval.
   */
  std::vector<flexible_type> pylambda_evaluator::bulk_eval(
    size_t lambda_hash,
    const std::vector<flexible_type>& args,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);

    py_set_random_seed(seed);

    std::vector<flexible_type> ret(args.size());
    size_t i = 0;
    for (const auto& x : args) {
      if (skip_undefined && x == FLEX_UNDEFINED) {
        ret[i++] = FLEX_UNDEFINED;
      } else {
        ret[i++] = eval(lambda_hash, x);
      }
    }
    return ret;
  }

  /**
   * Bulk version of eval_dict.
   */
  std::vector<flexible_type> pylambda_evaluator::bulk_eval_dict(
    size_t lambda_hash,
    const std::vector<std::string>& keys,
    const std::vector<std::vector<flexible_type>>& values,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);
    py_set_random_seed(seed);
    std::vector<flexible_type> ret(values.size());
    python::dict input;
    size_t cnt = 0;
    try {
        for (const auto& val: values) {
          PyDict_UpdateFromFlex(input, keys, val);
          python::object output = m_current_lambda->operator()(input);
          ret[cnt++] = PyObject_AsFlex(output);
        }
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    } catch (std::exception& e) {
      throw(std::string(e.what()));
    } catch (...) {
      throw("Unknown exception from python lambda evaluation.");
    }
    return ret;
  }

  void pylambda_evaluator::set_lambda(size_t lambda_hash) {
    if (m_current_lambda_hash == lambda_hash) return;

    auto lambda_obj = m_lambda_hash.find(lambda_hash);
    if (lambda_obj == m_lambda_hash.end()) {
      throw("Cannot find a lambda handle that is value " + std::to_string(lambda_hash));
    }

    m_current_lambda = m_lambda_hash[lambda_hash];
    m_current_lambda_hash = lambda_hash;
  }
} // end of namespace lambda
} // end of namespace graphlab
