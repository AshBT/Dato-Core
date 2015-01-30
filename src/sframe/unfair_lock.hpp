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
#ifndef GRAPHLAB_SFRAME_UNFAIR_IO_SCHEDULER_HPP
#define GRAPHLAB_SFRAME_UNFAIR_IO_SCHEDULER_HPP
#include <cstdint>
#include <cstddef>
#include <map>
#include <parallel/pthread_tools.hpp>
namespace graphlab {
/**
 * This class implements a completely unfair lock.
 *
 * The basic mechanic of operation is that every thread is assigned a 
 * priority ID (this is via a thread local variable) (rant if apple compiler
 * has proper C++11 support this would just be using the thread_local keyword.
 * But we don't. So it is this annoying boilerplate around pthread_getspecific.).
 *
 * Then if many threads are contending for the lock, the lock will always go to
 * the thread with the lowest priority ID.
 *
 * Furthermore, the lock has a parameterized "stickiness". In other words, when
 * a thread releases the lock, it is granted a certain time window in which
 * if it (or a lower ID thread) returns to acquire the lock, it will be able to
 * get it immediately. This "stickness" essentially parameterizes the
 * CPU-utilization Disk-utilization balance. The more IO bound a task is, the
 * better it is for it to be executed on just one CPU. This threshold attempts 
 * to self-tune by trying to maximize the total throughput of the lock.
 * (i.e.. maximising lock acquisitions per second). This is done by gradually
 * adapting the sleep interval: i.e. if increasing it increases throughput,
 * it gets increases, and vice versa.
 */
class unfair_lock {
 public:
  void lock();
  void unlock();
 private:
  graphlab::mutex m_lock;
  graphlab::mutex m_internal_lock;
  volatile bool m_lock_acquired = false;
  std::map<size_t, graphlab::conditional*> m_cond;
  // autotuning parameters for the lock stickness
  size_t m_previous_owner_priority = 0;
  int m_previous_sleep_interval = 0;
  double m_previous_time_for_epoch = 0;
  int m_current_sleep_interval = 50;
  double m_time_for_epoch = 0;
  size_t m_epoch_counter = 0;
  bool m_initial = true;
  timer m_ti;
};

} // graphlab
#endif
