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
#ifndef GRAPHLAB_LAMBDA_PYTHON_THREAD_GUARD_HPP
#define GRAPHLAB_LAMBDA_PYTHON_THREAD_GUARD_HPP

#include <mutex>
#include <Python.h>
#include <boost/python.hpp>

namespace graphlab {

namespace lambda {

/**
 * \ingroup lambda
 *
 * The global mutex provide re-entrant access to the python intepreter.
 */
static std::mutex py_gil;


/**
 * \ingroup lambda
 *
 * An RAII object for guarding multi-thread calls into the python intepreter.
 */
struct python_thread_guard {
  python_thread_guard() : guard(py_gil) {
    thread_state = PyGILState_Ensure();
  }

  ~python_thread_guard() {
    PyGILState_Release(thread_state);
  }

  PyGILState_STATE thread_state;
  std::lock_guard<std::mutex> guard;
};

}
}
#endif
