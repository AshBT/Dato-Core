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
#include <sframe/sframe_constants.hpp>
#include <globals/globals.hpp>
#include <limits>
namespace graphlab {

// will be modified at startup to match the number of CPUs
size_t SFRAME_DEFAULT_NUM_SEGMENTS = 16;
const size_t DEFAULT_SARRAY_READER_BUFFER_SIZE = 1024;
const size_t SARRAY_FROM_FILE_BATCH_SIZE = 32768;
const size_t MIN_SEGMENT_LENGTH = 1024;
const size_t SFRAME_WRITER_BUFFER_SOFT_LIMIT = 1024 * 4;
const size_t SFRAME_WRITER_BUFFER_HARD_LIMIT = 1024 * 10;
size_t SFRAME_FILE_HANDLE_POOL_SIZE = 128;
const size_t SFRAME_BLOCK_MANAGER_BLOCK_BUFFER_COUNT = 128;
const float COMPRESSION_DISABLE_THRESHOLD = 0.9;
size_t SFRAME_DEFAULT_BLOCK_SIZE =  64 * 1024;
const size_t SARRAY_WRITER_MIN_ELEMENTS_PER_BLOCK = 8;
const size_t SARRAY_WRITER_INITAL_ELEMENTS_PER_BLOCK = 16;
size_t SFRAME_WRITER_MAX_BUFFERED_CELLS = 32*1024*1024; // 64M elements
size_t SFRAME_WRITER_MAX_BUFFERED_CELLS_PER_BLOCK = 256*1024; // 1M elements.
// will be modified at startup to be 4x nCPUS
size_t SFRAME_MAX_BLOCKS_IN_CACHE = 32;
size_t SFRAME_CSV_PARSER_READ_SIZE = 50 * 1024 * 1024; // 50MB
size_t SFRAME_GROUPBY_BUFFER_NUM_ROWS = 1024 * 1024;
size_t SFRAME_JOIN_BUFFER_NUM_CELLS = 50*1024*1024;
size_t SFRAME_IO_READ_LOCK = false;
size_t SFRAME_SORT_PIVOT_ESTIMATION_SAMPLE_SIZE = 2000000;
size_t SFRAME_SORT_MAX_SEGMENTS = 128;
const size_t SFRAME_IO_LOCK_FILE_SIZE_THRESHOLD = 4 * 1024 * 1024;
std::string LIBODBC_PREFIX("");
size_t ODBC_BUFFER_SIZE = size_t(3 * 1024 * 1024) * size_t(1024); // 3 GB (to allow for a blob or two)
size_t ODBC_BUFFER_MAX_ROWS = 2000;

REGISTER_GLOBAL(std::string,
                LIBODBC_PREFIX,
                true);


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_DEFAULT_NUM_SEGMENTS, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_FILE_HANDLE_POOL_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 64; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_DEFAULT_BLOCK_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1024; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_MAX_BLOCKS_IN_CACHE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_CSV_PARSER_READ_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1024; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_GROUPBY_BUFFER_NUM_ROWS,
                            true, 
                            +[](int64_t val){ return val >= 64; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_JOIN_BUFFER_NUM_CELLS,
                            true, 
                            +[](int64_t val){ return val >= 1024; });



REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_WRITER_MAX_BUFFERED_CELLS,
                            true, 
                            +[](int64_t val){ return val >= 1024; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_WRITER_MAX_BUFFERED_CELLS_PER_BLOCK,
                            true, 
                            +[](int64_t val){ return val >= 1024; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_IO_READ_LOCK,
                            true, 
                            +[](int64_t val){ return val == 0 || val == 1 ; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_SORT_PIVOT_ESTIMATION_SAMPLE_SIZE,
                            true, 
                            +[](int64_t val){ return val > 128 ; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_SORT_MAX_SEGMENTS,
                            true,
                            +[](int64_t val){ return val > 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            ODBC_BUFFER_SIZE,
                            true,
                            +[](int64_t val){ return val >= 1024; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            ODBC_BUFFER_MAX_ROWS,
                            true,
                            +[](int64_t val){ return (val >= 1) && (val <= 1000000); });
} // namespace graphlab
