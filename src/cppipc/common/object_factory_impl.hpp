/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_COMMON_OBJECT_FACTORY_IMPL_HPP
#define CPPIPC_COMMON_OBJECT_FACTORY_IMPL_HPP
#include <cppipc/common/object_factory_base.hpp>
#include <cppipc/server/comm_server.hpp>
#include <map>
#include <string>
#include <functional>
#include <boost/algorithm/string/predicate.hpp>

namespace cppipc {

/**
 * \internal
 * \ingroup cppipc
 * An implementation of the object factory interface.
 * This is a special object created by the comm_server and is used to provide
 * the comm_server with an external interface; for instance to manage the 
 * construction and destruction of objects
 *
 */
class object_factory_impl: public object_factory_base {
 public:
  std::map<std::string, std::function<std::shared_ptr<void>()> > constructors;
  comm_server& srv;

  object_factory_impl(comm_server& comm):srv(comm) { }

  /**
   * creates and registers an object of type object_type_name
   */
  size_t make_object(std::string object_type_name);

  /**
   * Deletes the object of type object_id
   */
  void delete_object(size_t object_id);

  /**
   * Ping test. Replies with the ping value
   */
  std::string ping(std::string pingval);

  /**
   * Get the address on which the server is publishing status updates.
   */
  std::string get_status_publish_address();

  /**
   * Get the address which the server is receiving control messages
   */
  std::string get_control_address();

  void sync_objects(std::vector<size_t> object_ids, bool input_sorted);

  /**
   * \internal
   * Stores a constructor for an object type
   */
  void add_constructor(std::string object_type_name, 
                       std::function<std::shared_ptr<void>()> constructor) {
    constructors[object_type_name] = constructor;
  }
};




} // cppipc
#endif
