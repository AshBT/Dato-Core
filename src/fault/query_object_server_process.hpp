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
#ifndef FAULT_QUERY_OBJECT_SERVER_PROCESS_HPP
#define FAULT_QUERY_OBJECT_SERVER_PROCESS_HPP
#include <string>
#include <boost/function.hpp>
#include <fault/query_object.hpp>
#include <fault/query_object_create_flags.hpp>
namespace libfault {
typedef boost::function<query_object*(std::string objectkey,
                                      std::vector<std::string> zk_hosts,
                                      std::string zk_prefix,
                                      uint64_t create_flags)>  query_object_factory_type;

int query_main(int argc, char** argv, const query_object_factory_type& factory);

} // namespace;
#endif

