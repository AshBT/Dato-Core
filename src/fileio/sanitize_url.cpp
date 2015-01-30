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
#include <boost/algorithm/string/predicate.hpp>
#include <fileio/sanitize_url.hpp>
#include <fileio/s3_api.hpp>

namespace graphlab {

std::string sanitize_url(std::string url) {
  if (boost::algorithm::starts_with(url, "s3://")) {
    return webstor::sanitize_s3_url(url);
  } else {
    return url;
  }
}

}
