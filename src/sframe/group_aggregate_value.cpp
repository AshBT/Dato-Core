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
#include <sframe/group_aggregate_value.hpp>
#include <sframe/groupby_aggregate_operators.hpp>
#include <boost/algorithm/string.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>

namespace graphlab {

// defined in group_aggregate_value.hpp
std::shared_ptr<group_aggregate_value> get_builtin_group_aggregator(const std::string& name) {
  if (name == "__builtin__sum__") {
    return std::make_shared<groupby_operators::sum>();
  } else if (name == "__builtin__vector__sum__") {
    return std::make_shared<groupby_operators::vector_sum>();
  } else if (name == "__builtin__max__") {
    return std::make_shared<groupby_operators::max>();
  } else if (name == "__builtin__min__") {
    return std::make_shared<groupby_operators::min>();
  } else if (name == "__builtin__argmin__") {
    return std::make_shared<groupby_operators::argmin>();
  } else if (name == "__builtin__argmax__") {
    return std::make_shared<groupby_operators::argmax>();
  } else if (name == "__builtin__count__") {
    return std::make_shared<groupby_operators::count>();
  } else if (name == "__builtin__avg__") {
    return std::make_shared<groupby_operators::average>();
  } else if (name == "__builtin__vector__avg__"){
    return std::make_shared<groupby_operators::vector_average>();
  } else if (name == "__builtin__var__") {
    return std::make_shared<groupby_operators::variance>();
  } else if (name == "__builtin__stdv__") {
    return std::make_shared<groupby_operators::stdv>();
  } else if (name == "__builtin__select_one__") {
    return std::make_shared<groupby_operators::select_one>();
  } else if (boost::algorithm::starts_with(name, "__builtin__concat__dict__")) {
    return std::make_shared<groupby_operators::zip_dict>();
  } else if (boost::algorithm::starts_with(name, "__builtin__concat__list__")) {
    return std::make_shared<groupby_operators::zip_list>();
  } else if (boost::algorithm::starts_with(name, "__builtin__quantile__")) {
    // parse the quantiles
    // first get everything to the right of "__builtin_quantile__"
    std::string str_quantiles = name.substr(strlen("__builtin__quantile__"));
    std::vector<double> parsed_quantiles;
    bool success = false;
    flexible_type_parser parser;
    const char* c = str_quantiles.c_str();
    std::tie(parsed_quantiles, success) = parser.vector_parse(&c,
                                                              str_quantiles.length());
    if (!success) {
      log_and_throw("Unable to recognize quantiles in " + name);
    }
    for (double& quantile: parsed_quantiles) {
      if (quantile < 0 || quantile > 1) {
        log_and_throw("Quantiles most be between 0 and 1 inclusive");
      }
    }
    // ok. we are good.
    auto quantile_operator = std::make_shared<groupby_operators::quantile>();
    quantile_operator->init(parsed_quantiles);
    return quantile_operator;
  } else {
    log_and_throw("Unknown groupby aggregator " + name);
  }
}

}
