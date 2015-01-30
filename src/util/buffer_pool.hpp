/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_SFRAME_BUFFER_POOL_HPP
#define GRAPHLAB_SFRAME_BUFFER_POOL_HPP
#include <vector>
#include <memory>
#include <mutex>
#include <stack>
#include <parallel/pthread_tools.hpp>

namespace graphlab {

/**
 * Implements a buffer pool around collections of T.
 * The buffer is lazily allocated; but only up to 2 * buffer_size entries can
 * exist. 
 */
template <typename T>
class buffer_pool {
 public:
  explicit inline buffer_pool(size_t buffer_size = 128) {
    init(buffer_size);
  }

  /**
   * Initializes the buffer pool to a certain capacity. 
   * Can be called in parallel
   */
  inline void init(size_t buffer_size) {
    m_buffer_size = buffer_size;
  }

  /**
   * Returns a new buffer from the buffer pool
   * Can be called in parallel
   */
  inline std::shared_ptr<T> get_new_buffer() {
    if (m_free_buffers.empty()) {
      std::lock_guard<graphlab::mutex> guard(m_buffer_lock);
      // no free buffers. Loop through the buffer pool in search of unique buffer
      for (size_t i = 0;i < m_buffer_pool.size(); ++i) {
        if (m_buffer_pool[i].unique()) m_free_buffers.push(m_buffer_pool[i]);
      }
    }
    if (!m_free_buffers.empty()) {
      std::lock_guard<graphlab::mutex> guard(m_buffer_lock);
      if (!m_free_buffers.empty()) {
        auto ret = m_free_buffers.top();
        m_free_buffers.pop();
        return ret;
      }
    }  
    // allocate a new buffer
    std::shared_ptr<T> new_buffer = std::make_shared<T>();
    std::lock_guard<graphlab::mutex> guard(m_buffer_lock);
    if (m_buffer_pool.size() < m_buffer_size) m_buffer_pool.push_back(new_buffer);
    return new_buffer;
  }

  /**
   * Releases a buffer back to the pool
   * Can be called in parallel
   */
  inline void release_buffer(std::shared_ptr<T>&& buffer) {
    if (buffer) {
      buffer->clear();
      if (m_buffer_pool.size() + m_free_buffers.size() < m_buffer_size) {
        std::lock_guard<graphlab::mutex> guard(m_buffer_lock);
        m_free_buffers.push(std::move(buffer));
      }
      buffer.reset();
    }
  }

 private:
  /// Lock for m_buffer_pool
  graphlab::mutex m_buffer_lock;
  size_t m_buffer_size;
  // 
  /**
   * additional buffers used for returning stuff, decompression, etc.
   * Here we are using a free-list mechanism.
   * When m_free_buffers go empty, we loop through m_buffer_pool 
   * in search of all "unique" pointers which can then be added to the
   * free-list. This allows buffer release to be optional. Though, actively
   * releasing has performance benefits.
   */
  std::vector<std::shared_ptr<T> > m_buffer_pool;
  std::stack<std::shared_ptr<T> > m_free_buffers;
};
}
#endif
