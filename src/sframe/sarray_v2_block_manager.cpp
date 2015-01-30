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
extern "C" {
#include <lz4/lz4.h>
}
#include <algorithm>
#include <mutex>
#include <boost/algorithm/string.hpp>
#include <sframe/sarray_v2_block_manager.hpp>
#include <sframe/sarray_index_file.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/unfair_lock.hpp>

namespace graphlab {
namespace v2_block_impl {

static constexpr size_t NUM_IO_LOCKS = 16;
static unfair_lock* get_io_locks() { 
  static unfair_lock iolocks[NUM_IO_LOCKS];
  return iolocks;
}

block_manager& block_manager::get_instance() {
  static block_manager manager;
  return manager;
}

block_manager::block_manager() {
  m_buffer_pool.init(SFRAME_BLOCK_MANAGER_BLOCK_BUFFER_COUNT);
}

column_address block_manager::open_column(std::string column_file) {
  std::lock_guard<graphlab::mutex> guard(m_global_lock);
  std::pair<std::string, size_t> parsed_fname = parse_v2_segment_filename(column_file); 
  if (parsed_fname.second == (size_t)(-1)) parsed_fname.second = 0;
  size_t segment_id = 0;
  if (m_file_to_segments.count(parsed_fname.first)) {
    // the segment has already been opened
    segment_id = m_file_to_segments[parsed_fname.first];
  } else {
    // create a new segment 
    segment_id = (segment_id_counter);
    ++segment_id_counter;

    std::shared_ptr<segment> seg_ptr = std::make_shared<segment>();
    m_segments[segment_id] = seg_ptr;
    
    m_file_to_segments[parsed_fname.first] = segment_id;
    
    seg_ptr->segment_file = parsed_fname.first;
    size_t io_lock_id = fileio::get_io_parallelism_id(parsed_fname.first);
    if (io_lock_id == (size_t)(-1)) {
      seg_ptr->io_parallelism_id = io_lock_id;
    } else {
      seg_ptr->io_parallelism_id = io_lock_id % NUM_IO_LOCKS;
    }

    init_segment(seg_ptr);
  }

  ASSERT_TRUE(m_segments[segment_id] != NULL);
  m_segments[segment_id]->reference_count.inc();
  column_address ret{segment_id, parsed_fname.second};
  return ret;
}

void block_manager::close_column(column_address addr) {
  std::lock_guard<graphlab::mutex> guard(m_global_lock);
  size_t segment_id = std::get<0>(addr);
  ASSERT_TRUE(m_segments[segment_id] != NULL); 

  std::shared_ptr<segment> seg_ptr = m_segments[segment_id];
  
  bool segment_destroyed = false;
  {
    std::lock_guard<graphlab::mutex> guard(seg_ptr->lock);
    // decrement the reference count of the internal segment
    if (seg_ptr->reference_count.dec() == 0) {
      logstream(LOG_DEBUG) << "Closing " << seg_ptr->segment_file 
                           << std::endl;
      segment_destroyed = true;
      // we can close the segment entirely
      m_file_to_segments.erase(seg_ptr->segment_file);
      // try to close the segment files
      std::lock_guard<graphlab::mutex> guard(m_file_handles_lock);
      std::shared_ptr<general_ifstream> handle = 
          seg_ptr->segment_file_handle.lock();
      if (handle) handle->close();
    }
  } 
  if (segment_destroyed) {
    m_segments.erase(segment_id); 
  }
}

size_t block_manager::num_blocks_in_column(column_address addr) {
  // get the segment 
  size_t segment_id = std::get<0>(addr);
  size_t column_id = std::get<1>(addr);
  std::shared_ptr<segment> seg = get_segment(segment_id);
  ASSERT_GT(seg->blocks.size(), column_id);
  return seg->blocks[column_id].size();
}

const block_info& block_manager::get_block_info(block_address addr) {

  size_t segment_id, column_id, block_id;
  std::tie(segment_id, column_id, block_id) = addr;
  // get the segment 
  std::shared_ptr<segment> seg = get_segment(segment_id);
  return seg->blocks[column_id][block_id];
}

std::shared_ptr<std::vector<char> > 
block_manager::read_block(block_address addr, block_info** ret_info) {

  size_t segment_id, column_id, block_id;
  std::tie(segment_id, column_id, block_id) = addr;
  // get the segment 
  std::shared_ptr<segment> seg = get_segment(segment_id);
  // get the block info
  block_info& info = seg->blocks[column_id][block_id];

  if(ret_info) (*ret_info) = &info;

  // get the return buffer
  // resize ret to the block length on disk
  std::shared_ptr<std::vector<char> > ret = m_buffer_pool.get_new_buffer();
  ret->resize(info.length);

  // acquire lock on get the file handle and perform the read
  std::unique_lock<graphlab::mutex> guard(seg->lock);
  std::shared_ptr<general_ifstream> fin = get_segment_file_handle(seg);
  fin->seekg(info.offset, std::ios_base::beg);
  size_t iolockid = seg->io_parallelism_id;
  bool use_io_lock = SFRAME_IO_READ_LOCK > 0 && 
      (seg->file_size > SFRAME_IO_LOCK_FILE_SIZE_THRESHOLD);
  if (use_io_lock && iolockid != (size_t)(-1)) get_io_locks()[iolockid].lock();
  fin->read(ret->data(), info.length);
  if (use_io_lock && iolockid != (size_t)(-1)) get_io_locks()[iolockid].unlock();
  if (fin->fail()) {
    m_buffer_pool.release_buffer(std::move(ret));
    ret.reset();
    return ret;
  }
  guard.unlock();


  if (info.flags & LZ4_COMPRESSION) {
    /*
     * Decompress into another buffer.
     */
    std::shared_ptr<std::vector<char> > decompression_buffer = 
        m_buffer_pool.get_new_buffer();
    decompression_buffer->resize(info.block_size);
    LZ4_decompress_safe(ret->data(),                   // src
                        decompression_buffer->data(),  // target
                        info.length,                   // src length
                        info.block_size);              // target length
    std::swap(ret, decompression_buffer);
    m_buffer_pool.release_buffer(std::move(decompression_buffer));
  } 
  return ret;
}



bool block_manager::read_typed_block(block_address addr, 
                                     std::vector<flexible_type>& ret,
                                     block_info** ret_info) {
  block_info* info;
  std::shared_ptr<std::vector<char> > read_buffer = read_block(addr, &info);
  if (ret_info) (*ret_info) = info;
  if (!read_buffer) return false;
  // check that the block flags match
  bool success = typed_decode(*info, read_buffer->data(), read_buffer->size(), ret);
  m_buffer_pool.release_buffer(std::move(read_buffer));
  // check its the correct number of elements read
  return success;
}



/**************************************************************************/
/*                                                                        */
/*                           Private Functions                            */
/*                                                                        */
/**************************************************************************/
std::shared_ptr<general_ifstream> block_manager::get_new_file_handle(std::string s) {
  std::lock_guard<graphlab::mutex> guard(m_file_handles_lock);
  logstream(LOG_DEBUG) << "Opening " << s << std::endl;
  std::shared_ptr<general_ifstream> fin(new general_ifstream(s, false));
  if (m_file_handle_pool.size() > SFRAME_FILE_HANDLE_POOL_SIZE) {
    // we have exceeded the pool size. release the oldest handle
    m_file_handle_pool.pop_front();
  }
  m_file_handle_pool.push_back(fin);
  return fin;
}

std::shared_ptr<block_manager::segment> block_manager::get_segment(size_t segid) {
  std::lock_guard<graphlab::mutex> guard(m_global_lock);
  DASSERT_TRUE(m_segments[segid] != NULL);
  return m_segments[segid];
}

std::shared_ptr<general_ifstream> 
block_manager::get_segment_file_handle(std::shared_ptr<segment>& group) {
  std::shared_ptr<general_ifstream> fin = group->segment_file_handle.lock();
  if (!fin) {
    fin = get_new_file_handle(group->segment_file);
    group->segment_file_handle = fin;
  }
  fin->clear();
  return fin;
}

void block_manager::init_segment(std::shared_ptr<block_manager::segment>& seg) {
  // fast exit
  if (seg->inited) return;
  std::lock_guard<graphlab::mutex> guard(seg->lock);
  // check and exit again while within the lock
  if (seg->inited) return;
  // for each segment, read the block footer
  std::shared_ptr<general_ifstream> fin = get_segment_file_handle(seg);
  // jump to the footer
  uint64_t filesize = fin->file_size();
  uint64_t footer_size = -1;
  fin->seekg(filesize - sizeof(footer_size), std::ios_base::beg);

  // read 8 bytes
  fin->read(reinterpret_cast<char*>(&footer_size), sizeof(footer_size));

  // deserialize the block information
  fin->clear();
  fin->seekg(filesize - footer_size - sizeof(footer_size), std::ios_base::beg);
  iarchive iarc(*fin);
  iarc >> seg->blocks;

  seg->inited = true;
  seg->file_size = filesize;
}


} // namespace v2_block_impl
} // namespace graphlab
