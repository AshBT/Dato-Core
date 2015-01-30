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
#include <table_printer/table_printer.hpp>
#include <timer/timer.hpp>
#include <random/random.hpp>
#include <parallel/lambda_omp.hpp>

using namespace graphlab;

int main(int argc, char **argv) {

  {
    table_printer table( { {"Iteration", 0}, {"Elapsed Time", 10}, {"RMSE", 8}, {"Top String", 16} } );

    table.print_header();

    table.print_row(0, progress_time(),           1e6,   "Alphabetical.");
    table.print_row(1, progress_time(),           10,    "Alphabet soup.");
    table.print_row(2, progress_time(0.1),        1,     "Mine!!!!");
    table.print_row(4, progress_time(100),        0.1,   "Now it's a really long string.");
    table.print_row(5, progress_time(1000),       0.01,  "Yours!!!!");
    table.print_row(6, progress_time(1000.0001),  0.001, "");
    table.print_row(7, progress_time(5e5),        1e-6,  "Graphlab");

    table.print_row("FINAL", progress_time(5e6), 1e-6, "Graphlab");

    table.print_footer();
  }


  {
    table_printer table( { {"Iteration", 0}, {"Elapsed Time", 10}, {"RMSE", 8} } );

    table.print_header();

    for(size_t i = 0; i < 2000; ++i) {
      table.print_progress_row(i, i, progress_time(), std::exp(-double(i) / 5000));
      usleep(8000); // Sleep for 8 milliseconds
    }

    table.print_row("FINAL", progress_time(), 1e-6);

    table.print_footer();
  }


  {

    random::seed(0);

    table_printer table( { {"samples_processed", 0}, {"Elapsed Time", 10}, {"A value", 8} } );

    table.print_header();

    size_t proc = 0;

    for(size_t i = 0; i < 50000; ++i) {
      table.print_progress_row(proc, proc, progress_time(), i);
      proc += random::fast_uniform<size_t>(0, 100);
      usleep(100);  // sleep for 200 microseconds
    }

    table.print_row("FINAL", progress_time(), 1e-6);

    table.print_footer();
  }


  {
    table_printer table( { {"Iteration", 0}, {"Elapsed Time", 10}, {"My Value", 8} } );

    table.print_header();

    atomic<size_t> num_processed = 0;

    parallel_for(size_t(0), size_t(20000), [&](size_t i) {
        size_t idx = ++num_processed;

        table.print_progress_row(idx, idx, progress_time(), std::exp(-double(i) / 5000));
      });

    table.print_row("FINAL", progress_time(), 1e-6);

    table.print_footer();
  }


  {
    table_printer table( { {"Iteration", 0}, {"Kitten Now Being Shaved", 0}, {"Percent Complete", 8} } );

    table.set_output_stream(std::cout); 
    
    table.print_header();

    for(size_t i = 0; i < 20; ++i) {
      std::vector<flexible_type> v{i, std::string("K-") + std::to_string(i), double(i) / 20};
          
      table.print_row(v);
      usleep(8000); // Sleep for 8 milliseconds
    }

    table.print_footer();
  }


  return 0;
}
