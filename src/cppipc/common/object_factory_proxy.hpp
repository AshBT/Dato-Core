/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_COMMON_OBJECT_FACTORY_PROXY_HPP
#define CPPIPC_COMMON_OBJECT_FACTORY_PROXY_HPP
#include <map>
#include <string>
#include <functional>
#include <cppipc/client/object_proxy.hpp>
#include <cppipc/common/object_factory_base.hpp>
namespace cppipc {

/**
 * \internal
 * \ingroup cppipc
 */
class object_factory_proxy: public object_factory_base {
 public:
  object_proxy<object_factory_base> clt;
  object_factory_proxy(comm_client& comm):clt(comm, false, 0) {
    //the factory object 0 is special and the proxy always has it
  }

  inline size_t make_object(std::string objectname) {
    return clt.call(&object_factory_base::make_object, objectname);
  }

  inline std::string ping(std::string pingval) {
    return clt.call(&object_factory_base::ping, pingval);
  }

  inline void delete_object(size_t object_id) {
    clt.call(&object_factory_base::delete_object, object_id);
  }

  inline std::string get_status_publish_address() {
    return clt.call(&object_factory_base::get_status_publish_address);
  }

  inline std::string get_control_address() {
    return clt.call(&object_factory_base::get_control_address);
  }

  inline void sync_objects(std::vector<size_t> object_ids, bool input_sorted) {
    clt.call(&object_factory_base::sync_objects, object_ids, input_sorted);
  }

};




} // cppipc
#endif
