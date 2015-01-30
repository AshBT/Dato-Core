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
#ifndef GRAPHLAB_DHT_INTERNAL_CONTAINER_H_
#define GRAPHLAB_DHT_INTERNAL_CONTAINER_H_

#include <unordered_map>
#include <vector>
#include <algorithm>
#include <type_traits>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <parallel/atomic.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <graphlab/util/token.hpp>
#include <graphlab/util/bitops.hpp>
#include <graphlab/options/options_map.hpp>
#include <graphlab/dht/visitors.hpp>

#include <iostream>

namespace graphlab {
namespace _DHT {

////////////////////////////////////////////////////////////////////////////////
// Several types of internal containers for special use

template <typename _DHT,
          template <typename K, typename V> class _StorageContainer,
          typename _ValueType>
class InternalContainerBase {
 public:
  InternalContainerBase() {}
  InternalContainerBase(const InternalContainerBase&) = delete;
  InternalContainerBase operator=(const InternalContainerBase&) = delete;

  typedef WeakToken KeyType;
  typedef uint64_t LocalKeyType;
  typedef _ValueType ValueType;
  typedef _StorageContainer<LocalKeyType, ValueType> StorageType;
  typedef _DHT DHT;

  static constexpr LocalKeyType getLocalKey(const KeyType& k) {
    return LocalKeyType(k.hash());
  }

  template <typename Visitor>
  typename Visitor::ReturnType
  apply(DHT* local_dht_instance, LocalKeyType key, Visitor getter,
        _ENABLE_IF_RETURNS_VOID(Visitor))
  {
    _lock.lock();
    getter(local_dht_instance, table, key);
    _lock.unlock();
  }

  template <typename Visitor>
  typename Visitor::ReturnType
  apply(DHT* local_dht_instance, LocalKeyType key, Visitor getter,
        _ENABLE_IF_RETURNS_VALUE(Visitor)) {

    _lock.lock();
    typename Visitor::ReturnType r = getter(local_dht_instance, table, key);
    _lock.unlock();

    return r;
  }

private:

  mutable mutex _lock;
  StorageType table;
};

}}

#endif /* _DHT_INTERNAL_H_ */
