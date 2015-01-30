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
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef GRAPHLAB_DHT_HPP
#define GRAPHLAB_DHT_HPP

#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc_dist_object.hpp>

namespace graphlab {

  /**
   * \ingroup rpc
   * Implements a very rudimentary distributed key value store.
   */
  template <typename KeyType, typename ValueType>
  class dht { 

  public:
    typedef boost::unordered_map<size_t, ValueType> storage_type;
  

  private:
    mutable dc_dist_object< dht > rpc;
  
    boost::hash<KeyType> hasher;
    mutex lock;
    storage_type storage;

  public:
    dht(distributed_control &dc) : rpc(dc, this) { }
    
    /**
     * Get the owner of the key
     */    
    procid_t owner(const KeyType& key) const {
      return hasher(key) % rpc.dc().numprocs();
    }
  
    /**
     * gets the value associated with a key.
     * Returns (true, Value) if the entry is available.
     * Returns (false, undefined) otherwise.
     */
    std::pair<bool, ValueType> get(const KeyType &key) const {
      // who owns the data?

      const size_t hashvalue = hasher(key);
      const size_t owningmachine = hashvalue % rpc.numprocs();
      std::pair<bool, ValueType> retval;
      // if it is me, we can return it
      if (owningmachine == rpc.dc().procid()) {

        lock.lock();
        typename storage_type::const_iterator iter = storage.find(hashvalue);
        retval.first = iter != storage.end();
        if (retval.first) retval.second = iter->second;
        lock.unlock();
      } else {
        retval = rpc.RPC_CALL(remote_request, dht<KeyType,ValueType>::get) 
            (owningmachine, key);
      }
      return retval;
    }
 
    /**
     * gets the value associated with a key.
     * Returns (true, Value) if the entry is available.
     * Returns (false, undefined) otherwise.
     */
    request_future<std::pair<bool, ValueType> > get_future(const KeyType &key) const {
      // who owns the data?

      const size_t hashvalue = hasher(key);
      const size_t owningmachine = hashvalue % rpc.numprocs();
      std::pair<bool, ValueType> retval;
      // if it is me, we can return it
      if (owningmachine == rpc.dc().procid()) {

        lock.lock();
        typename storage_type::const_iterator iter = storage.find(hashvalue);
        retval.first = iter != storage.end();
        if (retval.first) retval.second = iter->second;
        lock.unlock();
        return retval;
      } else {
        return rpc.RPC_CALL(future_remote_request,dht<KeyType,ValueType>::get) 
                             (owningmachine, key);
      }
    }
  



    /**
     * Sets the newval to be the value associated with the key
     */
    void set(const KeyType &key, const ValueType &newval) {  
      // who owns the data?
      const size_t hashvalue = hasher(key);
      const size_t owningmachine = hashvalue % rpc.numprocs();
 
      // if it is me, set it
      if (owningmachine == rpc.dc().procid()) {
        lock.lock();
        storage[hashvalue] = newval;
        lock.unlock();
      } else {
        rpc.RPC_CALL(remote_call,dht<KeyType,ValueType>::set) 
                     (owningmachine, key, newval);
      }
    }
  
    void print_stats() const {
      std::cerr << rpc.calls_sent() << " calls sent\n";
      std::cerr << rpc.calls_received() << " calls received\n";
    }
  
    /**
       Must be called by all machines simultaneously
    */
    void clear() {
      rpc.barrier();
      storage.clear();
    }

  };

};
#endif

