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
#ifndef GRAPHLAB_UNITY_ODBC_CONNECTION_HPP
#define GRAPHLAB_UNITY_ODBC_CONNECTION_HPP

#include <unity/lib/toolkit_class_macros.hpp>
#include <sframe/odbc_connector.hpp>
#include <unity/lib/unity_sframe.hpp>

namespace graphlab {
namespace odbc_connection {

std::vector<graphlab::toolkit_class_specification> get_toolkit_class_registration();

}
}

#endif // GRAPHLAB_UNITY_ODBC_CONNECTION_HPP
