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



#include <parallel/thread_pool.hpp>
#include <logger/assertions.hpp>

namespace graphlab {

parallel_task_queue::parallel_task_queue(thread_pool& pool):pool(pool) { }

void parallel_task_queue::launch(const boost::function<void (void)> &spawn_function, 
                                 int thread_id) {
  mut.lock();
  tasks_inserted++;
  pool.launch(
      [&,spawn_function]() {
        try {
          spawn_function();
        } catch(const char* ex) {
          // if an exception was raised, put it in the exception queue
          mut.lock();
          exception_queue.push(ex);
          mut.unlock();
        } catch(std::string& ex) {
          mut.lock();
          exception_queue.push(ex);
          mut.unlock();
        } catch(std::exception& ex) {
          mut.lock();
          exception_queue.push(ex.what());
          mut.unlock();
        } catch(...) {
          mut.lock();
          exception_queue.push("Unknown exception");
          mut.unlock();
        }
        mut.lock();
        tasks_completed++;
        if (waiting_on_join && 
            tasks_completed == tasks_inserted) event_condition.signal();
        mut.unlock();
      }, thread_id);
  mut.unlock();
}

void parallel_task_queue::join() {
  std::pair<bool, bool> eventret;
  mut.lock();
  waiting_on_join = true;
  while(1) {
    // nothing to throw, check if all tasks were completed
    if (tasks_completed == tasks_inserted) {
      // yup
      break;
    }
    event_condition.wait(mut);
  }
  waiting_on_join = false;

  mut.unlock();
  if (exception_queue.size() > 0) {
    // check the exception queue.
    std::string first_exception = exception_queue.front();
    exception_queue = std::queue<std::string>();
    throw(first_exception);
  }
}

parallel_task_queue::~parallel_task_queue() {
  // keep joining, and throwing away exceptions
  join();
}



thread_pool::thread_pool(size_t nthreads, bool affinity) {
  cpu_affinity = affinity;
  pool_size = nthreads;
  spawn_thread_group();
} // end of thread_pool


void thread_pool::resize(size_t nthreads) {
  // if the current pool size does not equal the requested number of
  // threads shut the pool down and startup with correct number of
  // threads.  \todo: If the pool size is too small just add
  // additional threads rather than destroying the pool
  if(nthreads != pool_size) {
    pool_size = nthreads;

    // stop the queue from blocking
    spawn_queue.stop_blocking();

    // join the threads in the thread group
    while(true) {
      try {
        threads.join(); break;
      } catch (const char* error_str) {
        // this should not be possible!
        logstream(LOG_FATAL) 
            << "Unexpected exception caught in thread pool destructor: " 
            << error_str << std::endl;
      }
    }
    spawn_queue.start_blocking();
    spawn_thread_group();
  }
} // end of set_nthreads


size_t thread_pool::size() const { return pool_size; }


/**
  Creates the thread group
  */
void thread_pool::spawn_thread_group() {
  size_t ncpus = thread::cpu_count();
  // start all the threads if CPU affinity is set
  for (size_t i = 0;i < pool_size; ++i) {
    if (cpu_affinity) {
      threads.launch(boost::bind(&thread_pool::wait_for_task, this), i % ncpus);
    }
    else {
      threads.launch(boost::bind(&thread_pool::wait_for_task, this));
    }
  }
} // end of spawn_thread_group


void thread_pool::destroy_all_threads() {
  // wait for all execution to complete
  spawn_queue.wait_until_empty();
  // kill the queue
  spawn_queue.stop_blocking();

  // join the threads in the thread group
  while(1) {
    try {
      threads.join();
      break;
    }
    catch (const char* c) {
      // this should not be possible!
      logstream(LOG_FATAL) 
          << "Unexpected exception caught in thread pool destructor: " 
          << c << std::endl;
      ASSERT_TRUE(false);
    }
  }
} // end of destroy_all_threads

void thread_pool::set_cpu_affinity(bool affinity) {
  if (affinity != cpu_affinity) {
    cpu_affinity = affinity;
    // stop the queue from blocking
    spawn_queue.stop_blocking();

    // join the threads in the thread group
    while(1) {
      try {
        threads.join(); break;
      } catch (const char* c) {
        // this should not be possible!
        logstream(LOG_FATAL) 
            << "Unexpected exception caught in thread pool destructor: " 
            << c << std::endl;
        // ASSERT_TRUE(false); // unnecessary
      }
    }
    spawn_queue.start_blocking();
    spawn_thread_group();
  }
} // end of set_cpu_affinity


void thread_pool::launch(const boost::function<void (void)> &spawn_function, 
                         int virtual_threadid) {
  mut.lock();
  ++tasks_inserted;
  spawn_queue.enqueue(std::make_pair(spawn_function, virtual_threadid));
  mut.unlock();
}

void thread_pool::wait_for_task() {
  thread::get_tls_data().set_in_thread_flag(true);
  while(1) {
    std::pair<std::pair<boost::function<void (void)>, int>, bool> queue_entry;
    // pop from the queue
    queue_entry = spawn_queue.dequeue();
    if (queue_entry.second) {
      // try to run the function. remember to put it in a try catch
      int virtual_thread_id = queue_entry.first.second;
      size_t cur_thread_id = thread::thread_id();
      if (virtual_thread_id != -1) {
        thread::set_thread_id(virtual_thread_id);
      }
      queue_entry.first.first();
      thread::set_thread_id(cur_thread_id);
      mut.lock();
      ++tasks_completed;
      if (waiting_on_join && 
          tasks_completed == tasks_inserted) event_condition.signal();
      mut.unlock();

    }
    else {
      // quit if the queue is dead
      break;
    }
  }
} // end of wait_for_task

void thread_pool::join() {
  spawn_queue.wait_until_empty();

  mut.lock();
  waiting_on_join = true;
  while(1) {
    // nothing to throw, check if all tasks were completed
    if (tasks_completed == tasks_inserted) {
      // yup
      break;
    }
    event_condition.wait(mut);
  }
  waiting_on_join = false;
  mut.unlock();
}

thread_pool::~thread_pool() {
  destroy_all_threads();
}


thread_pool& thread_pool::get_instance() {
  static thread_pool ret(thread::cpu_count(), true);
  return ret;
}

}
