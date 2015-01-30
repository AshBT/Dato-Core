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
#ifndef GRAPHLAB_LAMBDA_LAMBDA_INTERFACE_HPP
#define GRAPHLAB_LAMBDA_LAMBDA_INTERFACE_HPP
#include <flexible_type/flexible_type.hpp>
#include <cppipc/cppipc.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {

namespace lambda {

GENERATE_INTERFACE_AND_PROXY(lambda_evaluator_interface, lambda_evaluator_proxy,
      (size_t, make_lambda, (const std::string&))
      (void, release_lambda, (size_t))
      (std::vector<flexible_type>, bulk_eval, (size_t)(const std::vector<flexible_type>&)(bool)(int))
      (std::vector<flexible_type>, bulk_eval_dict, (size_t)(const std::vector<std::string>&)(const std::vector<std::vector<flexible_type>>&)(bool)(int))
    )
} // namespace lambda
} // namespace graphlab

#endif
