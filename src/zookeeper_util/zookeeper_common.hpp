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
/// \ingroup fault
#ifndef GRAPHLAB_ZOOKEEPER_COMMON_HPP
#define GRAPHLAB_ZOOKEEPER_COMMON_HPP
#include <vector>
#include <string>

extern "C" {
#include <zookeeper/zookeeper.h>
}


namespace graphlab {
namespace zookeeper_util {

/// frees a zookeeper String_vector
void free_String_vector(struct String_vector* strings); 

/// convert a zookeeper String_vector to a c++ vector<string>
std::vector<std::string> String_vector_to_vector(
    const struct String_vector* strings); 

/// print a few zookeeper error status
void print_stat(int stat, 
                const std::string& prefix, 
                const std::string& path); 

/// adds a trailing / to the path name if there is not one already
std::string normalize_path(std::string prefix); 

/// Creates a zookeeper directory
int create_dir(zhandle_t* handle, const std::string& path, 
               const std::string& stat_message = "");

/// Deletes a zookeeper directory
int delete_dir(zhandle_t* handle, const std::string& path,
               const std::string& stat_message = "");

/// Creates a zookeeper ephemeral node
int create_ephemeral_node(zhandle_t* handle, 
                 const std::string& path, 
                 const std::string& value,
                 const std::string& stat_message = "");

/// Deletes a zookeeper ephemeral node 
int delete_node(zhandle_t* handle, 
                const std::string& path,
                const std::string& stat_message = "");

/// Deletes a zookeeper sequence node 
int delete_sequence_node(zhandle_t* handle, 
                         const std::string& path,
                         const int version,
                         const std::string& stat_message = "");

/// Gets the effective node name for a sequence node of a particular sequence number
std::string get_sequence_node_path(const std::string& path,
                                   const int version);


/// Creates a zookeeper ephemeral sequence nodea
/// Returns a pair of (status, version)
std::pair<int, int> create_ephemeral_sequence_node(zhandle_t* handle, 
                                                   const std::string& path, 
                                                   const std::string& value,
                                                   const std::string& stat_message = "");

/// Gets the value in a node. output is a pair of (success, value)
std::pair<bool, std::string> get_node_value(zhandle_t* handle,
                                            const std::string& node,
                                            const std::string& stat_message = "");


} // graphlab
} // zookeeper 

#endif
