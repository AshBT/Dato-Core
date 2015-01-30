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
#include<vector>
#include<mutex>
#include<sframe/sframe_constants.hpp>

namespace graphlab {
/**
 * Provide buffered write abstraction.
 * The class manages buffered concurrent write to an output iterator.
 *
 * Example:
 *
 * Suppose there are M data sources randomly flow to N sinks. We can use
 * buffered_writer to achieve efficient concurrent write.
 *
 * \code
 *
 * std::vector<input_iterator> sources; // size M
 * std::vector<output_iterator>  sinks; // size N
 * std::vector<std::mutex>  sink_mutex; // size N
 *
 * parallel_for_each(s : sources) {
 *   std::vector<buffered_writer> writers;
 *   for (i = 1...N) {
 *    writers.push_back(buffered_writer(sinks[i], sink_mutex[i]));
 *   }
 *   while (s.has_next()) {
 *     size_t destination = random.randint(N);
 *     writers[destination].write(s.next());
 *   }
 *   for (i = 1...N) {
 *     writers[i].flush();
 *   }
 * }
 * \endcode
 *
 * Two parameters "soft_limit" and "hard_limit" are used to control the buffer
 * size. When soft_limit is met, the writer will try to flush the buffer
 * content to the sink. When hard_limit is met, the writer will force the flush.
 */
template<typename ValueType, typename OutIterator>
class buffered_writer {
public:
  buffered_writer(OutIterator& out, std::mutex& out_lock,
                  size_t soft_limit = SFRAME_WRITER_BUFFER_SOFT_LIMIT,
                  size_t hard_limit = SFRAME_WRITER_BUFFER_HARD_LIMIT) :
    out(out), out_lock(out_lock),
    soft_limit(soft_limit),
    hard_limit(hard_limit) {
    ASSERT_GT(hard_limit, soft_limit);
  }

  /**
   * Write the value to the buffer.
   * Try flush when buffer exceeds soft limit and force
   * flush when buffer exceeds hard limit.
   */
  void write(const ValueType& val) {
    buffer.push_back(val);
    if (buffer.size() >= soft_limit) {
      bool locked = out_lock.try_lock();
      if (locked || buffer.size() >= hard_limit) {
        flush(locked);
      }
    }
  }

  /**
   * Flush the buffer to the output sink. Clear the buffer when finished.
   */
  void flush(bool is_locked = false) {
    if (!is_locked) {
      out_lock.lock();
    }
    std::lock_guard<std::mutex> guard(out_lock, std::adopt_lock);
    std::copy(buffer.begin(), buffer.end(), out);
    buffer.clear();
  }

private:
  OutIterator& out;
  std::mutex& out_lock;
  size_t soft_limit;
  size_t hard_limit;
  std::vector<ValueType> buffer;
};
}
