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
/*  
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


#include <graphlab/util/inplace_lf_queue.hpp>
namespace graphlab {
void inplace_lf_queue::enqueue(char* c) {
  // clear the next pointer
  (*get_next_ptr(c)) = NULL;
  // atomically,
  // swap(tail, c)
  // tail->next = c;
  char* prev = c;
  atomic_exchange(tail, prev);
  (*get_next_ptr(prev)) = c;
  asm volatile ("" : : : "memory");
}


void inplace_lf_queue::enqueue_unsafe(char* c) {
  // clear the next pointer
  (*get_next_ptr(c)) = NULL;
  // swap(tail, c)
  // tail->next = c;
  char* prev = c;
  std::swap(tail, prev);
  (*get_next_ptr(prev)) = c;
}


char* inplace_lf_queue::dequeue_all() {
  // head is the sentinel
  char* ret_head = get_next(head);
  if (ret_head == NULL) return NULL;
  // now, the sentinel is not actually part of the queue.
  // by the time get_next(sentinel) is non-empty, enqueue must have completely
  // finished at least once, since the next ptr is only connected in line 11.
  // enqueue the sentinel. That will be the new head of the queue.
  // Anything before the sentinel is "returned". And anything after is part
  // of the queue
  enqueue(sentinel);

  // The last element in the returned queue
  // will point to the sentinel.
  return ret_head;
}

char* inplace_lf_queue::dequeue_all_unsafe() {
  // head is the sentinel
  char* ret_head = get_next(head);
  if (ret_head == NULL) return NULL;
  // now, the sentinel is not actually part of the queue.
  // by the time get_next(sentinel) is non-empty, enqueue must have completely
  // finished at least once, since the next ptr is only connected in line 11.
  // enqueue the sentinel. That will be the new head of the queue.
  // Anything before the sentinel is "returned". And anything after is part
  // of the queue
  enqueue_unsafe(sentinel);

  // The last element in the returned queue
  // will point to the sentinel.
  return ret_head;
}


} // namespace graphlab
