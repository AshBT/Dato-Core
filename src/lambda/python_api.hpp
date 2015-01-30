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
#ifndef GRAPHLAB_LAMBDA_PYTHON_API_HPP
#define GRAPHLAB_LAMBDA_PYTHON_API_HPP

#include <mutex>

namespace graphlab {

namespace lambda {



/**
 * \ingroup lambda
 *
 * Initialize the python environment, and import global graphlab modules and classes.
 * Must be called before any python functionality is used.
 *
 * Throws string exception on error.
 */
void init_python(int argc, char** argv); 

/**
 * Extract the exception error string from the python interpreter.
 *
 * This function assumes the gil is acquired. See \ref python_thread_guard.
 *
 * Code adapted from http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
 */
std::string parse_python_error();

/**
 * Set the random seed for the interpreter.
 *
 * This function assumes the gil is acquired. See \ref python_thread_guard.
 */
void py_set_random_seed(size_t seed);

} // end lambda
} // end graphlab
#endif
