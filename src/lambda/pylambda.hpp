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
#ifndef GRAPHLAB_LAMBDA_PYLAMBDA_EVALUATOR_HPP
#define GRAPHLAB_LAMBDA_PYLAMBDA_EVALUATOR_HPP
#include <lambda/lambda_interface.hpp>
#include <flexible_type/flexible_type.hpp>
#include <string>

// Forward delcaration
namespace boost {
  namespace python {
    namespace api {
      class object;
    }
  }
}

namespace graphlab {

namespace lambda {
/**
 * \ingroup lambda
 *
 * A functor class wrapping a pickled python lambda string.
 *
 * The lambda type is assumed to be either: S -> T or or List -> T.
 * where all types should be compatible  with flexible_type.
 *
 * \note: currently only support basic flexible_types: flex_string, flex_int, flex_float
 *
 * \internal
 * All public member functions including the constructors are guarded by the
 * global mutex, preventing simultanious access to the python's GIL.
 *
 * Internally, the class stores a a python lambda object which is created from the
 * pickled lambda string upon construction. The lambda object is equivalent
 * to a python lambda object (with proper reference counting), and therefore, the class is copiable.
 */
class pylambda_evaluator : public lambda_evaluator_interface {

 public:
  /**
   * Construct an empty evaluator.
   */
  pylambda_evaluator() { m_current_lambda_hash = (size_t)(-1); };

  /**
   * Sets the internal lambda from a pickled lambda string.
   *
   * Throws an exception if the construction failed.
   */
  size_t make_lambda(const std::string& pylambda_str);

  /**
   * Release the cached lambda object
   */
  void release_lambda(size_t lambda_hash);

  /**
   * Evaluate the lambda function on each argument separately in the args list.
   */
  std::vector<flexible_type> bulk_eval(size_t lambda_hash, const std::vector<flexible_type>& args, bool skip_undefined, int seed);

  /**
   * Evaluate the lambda function on each element separately in the values.
   * The value element is combined with the keys to form a dictionary argument. 
   */
  std::vector<flexible_type> bulk_eval_dict(size_t lambda_hash,
      const std::vector<std::string>& keys,
      const std::vector<std::vector<flexible_type>>& values,
      bool skip_undefined, int seed);

 private:

  // Set the lambda object for the next evaluation.
  void set_lambda(size_t lambda_hash);

  /**
   * Apply as a function: flexible_type -> flexible_type,
   *
   * \note: this function does not perform type check and exception could be thrown
   * when applying of the function. As a subroutine, this function does not
   * try to acquire GIL and assumes it's already been acquired.
   */
  flexible_type eval(size_t lambda_hash, const flexible_type& arg);

  /**
   * The unpickled python lambda object.
   */
  boost::python::api::object* m_current_lambda = NULL;
  std::map<size_t, boost::python::api::object*> m_lambda_hash;
  size_t m_current_lambda_hash;
};
  } // end of lambda namespace
} // end of graphlab namespace
#endif
