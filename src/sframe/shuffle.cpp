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
#include<sframe/shuffle.hpp>
#include<fileio/buffered_writer.hpp>
#include<memory>

namespace graphlab {

// number of rows to fetch for read sframe read
const size_t READER_BUFFER_SIZE = 1024 * 4;

// number of rows to buffer before trying to write out to the output sframe.
const size_t WRITER_BUFFER_SOFT_LIMIT = 1024 * 4;

// number of rows to buffer before forcing it to write out to the output sframe.
const size_t WRITER_BUFFER_HARD_LIMIT = 1024 * 10;

std::vector<sframe> shuffle(
    sframe sframe_in,
    size_t n,
    std::function<size_t(const std::vector<flexible_type>&)> hash_fn) {

    ASSERT_GT(n, 0);

    // split the work to threads
    // for n bins let's assign n / log(n) workers, assuming rows are evenly distributed.
    size_t num_rows = sframe_in.num_rows();
    size_t num_workers = graphlab::thread::cpu_count();
    size_t rows_per_worker = num_rows / num_workers;

    // prepare the out sframe
    std::vector<sframe> sframe_out;
    std::vector<sframe::iterator> sframe_out_iter;
    sframe_out.resize(n);
    for (auto& sf: sframe_out) {
      sf.open_for_write(sframe_in.column_names(), sframe_in.column_types(), "",  1);
      sframe_out_iter.push_back(sf.get_output_iterator(0));
    }
    std::vector<std::unique_ptr<std::mutex>> sframe_out_locks;
    for (size_t i = 0; i < n; ++i) {
      sframe_out_locks.push_back(std::unique_ptr<std::mutex>(new std::mutex));
    }

    auto reader = sframe_in.get_reader();
    parallel_for(0, num_workers, [&](size_t worker_id) {
        size_t start_row = worker_id * rows_per_worker;
        size_t end_row = (worker_id == (num_workers-1)) ? num_rows
                                                        : (worker_id + 1) * rows_per_worker;

        // prepare thread local output buffer for each sframe
        std::vector<buffered_writer<std::vector<flexible_type>, sframe::iterator>> writers;
        for (size_t i = 0; i < n; ++i) {
          writers.push_back(
            buffered_writer<std::vector<flexible_type>, sframe::iterator>
            (sframe_out_iter[i], *sframe_out_locks[i],
             WRITER_BUFFER_SOFT_LIMIT, WRITER_BUFFER_HARD_LIMIT)
          );
        }

        std::vector<std::vector<flexible_type>> in_buffer(READER_BUFFER_SIZE);
        while (start_row < end_row) {
          // read a chunk of rows to shuffle
          size_t rows_to_read = std::min<size_t>((end_row - start_row), READER_BUFFER_SIZE);
          size_t rows_read = reader->read_rows(start_row, start_row + rows_to_read, in_buffer);
          DASSERT_EQ(rows_read, rows_to_read);
          start_row += rows_read;

          for (auto& row : in_buffer) {
            size_t out_index = hash_fn(row) % n;
            writers[out_index].write(row);
          }
        } // end of while

        // flush the rest of the buffer
        for (size_t i = 0; i < n; ++i) {
          writers[i].flush();
        }
    });

    // close all sframe writers
    for (auto& sf: sframe_out) {
      sf.close();
    }
    return sframe_out;
}
}
