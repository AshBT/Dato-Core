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
#include <zookeeper_util/key_value.hpp>
namespace libfault {

extern std::string get_zk_objectkey_name(std::string objectkey, size_t nrep);
extern std::string get_publish_key(std::string objectkey);

extern bool master_election(graphlab::zookeeper_util::key_value* zk_keyval,
                     std::string objectkey);

extern bool replica_election(graphlab::zookeeper_util::key_value* zk_keyval,
                     std::string objectkey,
                     size_t replicaid);

} // namespace
