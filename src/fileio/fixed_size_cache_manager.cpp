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
#include <fileio/fileio_constants.hpp>
#include <fileio/fixed_size_cache_manager.hpp>
#include <logger/assertions.hpp>
#include <iostream>
#include <iomanip>


namespace graphlab {

namespace fileio {
/*************************************************************************/
/*                                                                       */
/*                         Cache Block implementation                    */
/*                                                                       */
/*************************************************************************/

  // Construct an in-memory cache block
  cache_block::cache_block(cache_id_type cache_id, size_t max_capacity, 
                           fixed_size_cache_manager* owning_cache_manager) :
    cache_id(cache_id),
    owning_cache_manager(owning_cache_manager) { 
      initialize_memory(max_capacity);
    }

  bool cache_block::extend_capacity(size_t new_capacity) {
    if (data && new_capacity <= maximum_capacity) {
      // we already have capacity exceeding new capacity
      if (new_capacity <= capacity) return true;
      size_t queried_capacity = new_capacity;
      // try to double up to maximum capacity
      new_capacity = std::max(new_capacity, capacity * 2);
      new_capacity = std::min(new_capacity, maximum_capacity);
      size_t current_cache_utilization = owning_cache_manager->get_cache_utilization();
      // will we exceed capacity?
      if (current_cache_utilization + (new_capacity - capacity) > 
          FILEIO_MAXIMUM_CACHE_CAPACITY) {

        // resizing will cause us to go over the maximum cache limit
        // try again with the minimal queried size.
        new_capacity = queried_capacity;
        if (current_cache_utilization + (new_capacity - capacity) > 
            FILEIO_MAXIMUM_CACHE_CAPACITY) {
          // yup. we will still exceed capacity. FAIL.
          return false;
        }
      }
      // realloc
      data = (char*)realloc(data, new_capacity);
      owning_cache_manager->increment_utilization((int)new_capacity - (int)capacity);
      capacity = new_capacity;
      return true;
    } else {
      return false;
    }
  }


  std::shared_ptr<fileio_impl::general_fstream_sink> cache_block::write_to_file() {
    ASSERT_TRUE(filename.empty());
    filename = get_temp_name();
    logstream(LOG_DEBUG) << "Flushing to " << filename << std::endl;
    auto fout = std::make_shared<fileio_impl::general_fstream_sink>(filename);
    if (data) fout->write(data, size);
    release_memory();
    return fout;
  }

  void cache_block::initialize_memory(size_t max_capacity) {
    clear();
    maximum_capacity = max_capacity,
    capacity = std::min(FILEIO_INITIAL_CAPACITY_PER_FILE, maximum_capacity);
    size = 0;
    if (capacity > 0) {
      data = (char*)malloc(capacity);
      owning_cache_manager->increment_utilization(capacity);
    } else {
      data = NULL;
    }
  }

  void cache_block::release_memory() {
    if (data) {
      free(data);
      owning_cache_manager->decrement_utilization(capacity);
    }
    data = NULL;
    size = 0;
    capacity = 0;
    maximum_capacity = 0;
  }

  void cache_block::clear() {
    if (data) {
      logstream(LOG_DEBUG) << "Releasing cache ID " << cache_id << std::endl;
      release_memory();
    } else if (!filename.empty()) {
      logstream(LOG_DEBUG) << "Releasing cache ID " << cache_id << std::endl;
      // delete disk temp file
      try {
        logstream(LOG_DEBUG) << "Deleting cached file " << filename << std::endl;
        delete_temp_file(filename);
      } catch (...) {
        logstream(LOG_WARNING) << "Failed to delete temporary file: " 
                               << filename << std::endl;
      }
      filename.clear();
    }
  }

  cache_block::~cache_block() {
    clear();
  }
/*************************************************************************/
/*                                                                       */
/*                       Cache Manager implementation                    */
/*                                                                       */
/*************************************************************************/

  fixed_size_cache_manager& fixed_size_cache_manager::get_instance() {
    static fixed_size_cache_manager instance;
    return instance;
  }

  fixed_size_cache_manager::fixed_size_cache_manager() { }

  fixed_size_cache_manager::~fixed_size_cache_manager() {
    clear();
  }

  void fixed_size_cache_manager::clear() {
    cache_blocks.clear();
  }

  cache_id_type fixed_size_cache_manager::get_temp_cache_id(std::string suffix) {
    std::lock_guard<std::mutex> scoped_lock(mutex);
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(6) << temp_cache_counter;
    ++temp_cache_counter;
    cache_id_type ret(TMP_CACHE_PREFIX + ss.str() + suffix);
    return ret;
  }

  std::shared_ptr<cache_block> fixed_size_cache_manager::new_cache(cache_id_type cache_id) {
    std::lock_guard<std::mutex> lck(mutex);
    logstream_ontick(5, LOG_INFO) << "Cache Utilization:" << get_cache_utilization() << std::endl;
    // if we have exceeded, we try to evict
    if (current_cache_utilization.value >= FILEIO_MAXIMUM_CACHE_CAPACITY) try_cache_evict();
    // read the current cache utilization.
    size_t current_utilization = current_cache_utilization.value;
    // this will the maximum capacity of the new entry
    size_t new_entry_max_capacity = 0;
    if (current_utilization < FILEIO_MAXIMUM_CACHE_CAPACITY) {
      // if we have less than new_max_block_capacity available,
      // give less capacity.
      new_entry_max_capacity = std::min<size_t>(FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE,
                                                FILEIO_MAXIMUM_CACHE_CAPACITY - 
                                                current_utilization);
    } 

    if (cache_blocks.find(cache_id) == cache_blocks.end()) {
      logstream(LOG_DEBUG) << "New cache block " << cache_id 
                           << " Capacity = " << new_entry_max_capacity << std::endl;

      std::shared_ptr<cache_block> block(new cache_block(cache_id, new_entry_max_capacity, this));
      cache_blocks[cache_id] = block;
      return block;
    } else {
      logstream(LOG_DEBUG) << "Overwrite cache block " << cache_id 
                           << " Capacity = " << new_entry_max_capacity << std::endl;
      // we need to clear the content of the block.
      auto iter = cache_blocks.find(cache_id);
      std::shared_ptr<cache_block> block = iter->second;
      // if its a pointer. we just reuse it. 
      if (block->is_pointer()) {
        block->initialize_memory(block->maximum_capacity);
      } else {
        block->initialize_memory(new_entry_max_capacity);
      }
      return block;
    }
  }

  void fixed_size_cache_manager::free(std::shared_ptr<cache_block> block) {
    logstream(LOG_DEBUG) << "Free cache block " << block->cache_id << std::endl;
    std::lock_guard<std::mutex> lck(mutex);
    cache_id_type id = block->cache_id;
    auto iter = cache_blocks.find(id);
    ASSERT_TRUE(iter != cache_blocks.end());
    cache_blocks.erase(iter);
  }

  std::shared_ptr<cache_block> fixed_size_cache_manager::get_cache(cache_id_type cache_id) {
    logstream(LOG_DEBUG) << "Get cache block " << cache_id << std::endl;
    std::lock_guard<std::mutex> lck(mutex);
    if (cache_blocks.find(cache_id) != cache_blocks.end()) {
      return cache_blocks[cache_id];
    }
    throw std::out_of_range("Cannot find cache block with id " + cache_id);
  }

  void fixed_size_cache_manager::increment_utilization(int increment) {
    current_cache_utilization.inc(increment);
  }

  void fixed_size_cache_manager::decrement_utilization(int increment) {
    current_cache_utilization.dec(increment);
  }

  void fixed_size_cache_manager::try_cache_evict() {
    // lock must be acquired outside of this call
    ASSERT_FALSE(mutex.try_lock());
    // we will try to evict the largest
    std::string largest_entry_name;
    std::shared_ptr<cache_block> largest_block;
    size_t current_largest_block_size = 0;
    for (auto& iter: cache_blocks) {
      // we can only evict if we are the only pointers to the cache block
      if (iter.second.unique() && iter.second->is_pointer()) {
        if (iter.second->get_pointer_size() > current_largest_block_size) {
          largest_entry_name = iter.first;
          largest_block = iter.second;
          current_largest_block_size = largest_block->get_pointer_size();
        }
      }
    }
    if (largest_block) {
      logstream_ontick(5, LOG_INFO) << "Evicting " << largest_entry_name 
                          << " with size " << current_largest_block_size << std::endl;
      largest_block->write_to_file();
      logstream_ontick(5, LOG_INFO) << "Cache Utilization:" << get_cache_utilization()
                                    << std::endl;
    }
  }
} // end of fileio

} // end of graphlab
