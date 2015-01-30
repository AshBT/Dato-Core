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

#ifndef GRAPHLAB_UNITY_SFRAME_SORT_IMPL
#define GRAPHLAB_UNITY_SFRAME_SORT_IMPL

#include <functional>
#include <algorithm>
#include <parallel/thread_pool.hpp>
#include <parallel/lambda_omp.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe_config.hpp>
#include <sketches/quantile_sketch.hpp>
#include <sketches/streaming_quantile_sketch.hpp>
#include <unity/query_process/lazy_sframe.hpp>

namespace graphlab {

namespace sframe_sort_impl {

/**
* Comparator that compares two flex_list value with given ascending/descending
* order. Order value "true" means ascending, "false" means descending
* it compares all values in the two flex_list types
**/
struct less_than_full_function
{
  less_than_full_function() {};
  less_than_full_function(const std::vector<bool>& sort_orders):
    m_sort_orders(sort_orders)
  {}

  inline bool operator() (const flexible_type& v1, const flexible_type& v2) const
  {
    DASSERT_TRUE(v1.get_type() == flex_type_enum::LIST);
    DASSERT_TRUE(v2.get_type() == flex_type_enum::LIST);

    const flex_list& v1_v = v1.get<flex_list>();
    const flex_list& v2_v = v2.get<flex_list>();
    return compare(v1_v, v2_v);
  }


  inline bool operator() (const std::vector<flexible_type>& v1, const std::vector<flexible_type>& v2) const {
    return compare(v1, v2);
  }

  inline bool compare(const std::vector<flexible_type>& v1, const std::vector<flexible_type>& v2) const {
    DASSERT_TRUE(v1.size() == v2.size());
    DASSERT_TRUE(v1.size() == m_sort_orders.size());

    for(size_t i = 0; i < m_sort_orders.size(); i++) {
      bool ascending = m_sort_orders[i];

      if (v1[i] == FLEX_UNDEFINED) {
        if (v2[i] == FLEX_UNDEFINED) {
          continue;  // equal
        } else {
          return ascending;
        }
      }

      if (v2[i] == FLEX_UNDEFINED) {
        return !ascending;
      }

      if (v1[i] < v2[i]) return ascending;
      if (v1[i] > v2[i]) return !ascending;
    }

    return false;
  }

  std::vector<bool> m_sort_orders;
};

/**
* Comparator that compares two flex_list value with given ascending/descending
* order and given sort columns.
* This is different from above in that it is only comparing part of the columns
* in the value, not all the values
* Order value "true" means ascending, "false" means descending
**/
struct less_than_partial_function
{
  less_than_partial_function(const std::vector<size_t>& sort_columns, const std::vector<bool>& sort_orders)
    : m_sort_columns(sort_columns), m_sort_orders(sort_orders)
  {
    DASSERT_TRUE(sort_orders.size() == sort_columns.size());
  }

  inline bool operator() (const std::vector<flexible_type>& v1, const std::vector<flexible_type>& v2)
  {
    DASSERT_TRUE(v1.size() == v2.size());

    size_t i = 0;
    for(auto column_idx: m_sort_columns) {
      DASSERT_TRUE(v1.size() > column_idx);
      bool ascending = m_sort_orders[i++];

      if (v1[column_idx] == FLEX_UNDEFINED) {
        if (v2[column_idx] == FLEX_UNDEFINED) continue;
        return ascending;
      }

      if (v2[column_idx] == FLEX_UNDEFINED) {
        return !ascending;
      }

      if (v1[column_idx] < v2[column_idx]) return ascending;
      if (v1[column_idx] > v2[column_idx]) return !ascending;
    }

    return false;
  }

  std::vector<size_t> m_sort_columns;
  std::vector<bool> m_sort_orders;
};

/**
** Create a quantile sketch for the key columns so that we can decide how to partition
   the sframe
**/
std::shared_ptr<sketches::streaming_quantile_sketch<flexible_type, less_than_full_function>>
create_quantile_sketch(
  std::shared_ptr<lazy_sframe>&   sframe_ptr,
  const std::vector<bool>&        sort_orders ) {


  auto comparator =  less_than_full_function(sort_orders);
  auto global_quantiles = std::make_shared<sketches::streaming_quantile_sketch<flexible_type, less_than_full_function>>(0.005, comparator);

  graphlab::mutex lock;
  size_t dop = thread::cpu_count();
  float proportion_to_sample =
    float(SFRAME_SORT_PIVOT_ESTIMATION_SAMPLE_SIZE) / float(sframe_ptr->size());

  boost::posix_time::ptime epoch_time(boost::gregorian::date(1970,1,1));
  auto now_time = boost::posix_time::second_clock::local_time();
  auto time_dur = now_time - epoch_time;
  int random_seed = int(time_dur.fractional_seconds());


  // construct return
  if(proportion_to_sample < 1.0f) {
    auto logical_filter_op = std::make_shared<le_logical_filter<std::vector<flexible_type>>>(
        sframe_ptr->get_query_tree(),
        std::make_shared<le_random>(proportion_to_sample, random_seed, sframe_ptr->size()),
        flex_type_enum::VECTOR);

    sframe_ptr = std::make_shared<lazy_sframe>(logical_filter_op,
        sframe_ptr->column_names(),
        sframe_ptr->column_types());
  }
  auto input_iterator = sframe_ptr->get_iterator(dop);
  logstream(LOG_INFO) << "Sampling pivot proportion: " << proportion_to_sample << std::endl;

  parallel_for(0, dop,
     [&](size_t idx) {
        size_t elements_sampled = 0;
        std::shared_ptr<sketches::streaming_quantile_sketch<flexible_type, less_than_full_function>> quantiles;
        quantiles.reset(new sketches::streaming_quantile_sketch<flexible_type, less_than_full_function>(0.005, comparator));

        while(true) {
          std::vector<std::vector<flexible_type> > items
              = input_iterator->get_next(idx, graphlab::sframe_config::SFRAME_READ_BATCH_SIZE);

          if (items.size() == 0) break;

          elements_sampled += items.size();
          for(auto& val: items) {
            quantiles->add(std::move(val));
          }
        }

        quantiles->substream_finalize();
        lock.lock();
        global_quantiles->combine(std::move(*quantiles));
        lock.unlock();
  });

  global_quantiles->combine_finalize();
  return global_quantiles;
}

/**
* Find the "spliting points" that can partition the sframe into roughly similar
* size chunks so that elements between chunks are relatively ordered.

* The way to do this is to do a sketch summary over the sorted columns, find the
* quantile keys for each incremental quantile and use that key as "spliting point".

* \param sframe_ptr The lazy sframe that needs to be sorted
* \param sort_orders The sort order for the each sorted columns, true means ascending
* \param num_partitions The number of partitions to partition the result to
* \param[out] partition_keys The "pivot point". There will be num_partitions-1 of these.
* \param[out] partition_sorted Indicates whether or not a given partition contains
*   all the same key hence no need to sort later
* \return true if all key values are the same(hence no need to sort), false otherwise
**/
bool get_partition_keys(
  std::shared_ptr<lazy_sframe>    sframe_ptr,
  const std::vector<bool>&        sort_orders,
  size_t                          num_partitions,
  std::vector<flexible_type>&     partition_keys) {

  auto quantiles = create_quantile_sketch(sframe_ptr, sort_orders);

  // figure out all the cutting place we need for the each partion by calculating
  // quantiles
  double quantile_unit = 1.0 / num_partitions;
  flexible_type quantile_val;

  for (size_t i = 0;i < num_partitions - 1; ++i) {
    quantile_val = quantiles->query_quantile((i + 1) * quantile_unit);
    partition_keys.push_back(quantile_val);
  }
  return false;
}

/**
 * Partition given sframe into multiple partitions according to given partition key.
 * This results to multiple partitions and partitions are relatively ordered.

 * This function writes the resulting partitions into a sarray<string> type, where
 * each segment in the sarray is one partition that are relatively ordered.

 * We store a serialized version of original sframe sorting key columns and values

 * \param sframe_ptr The lazy sframe to be scatter partitioned
 * \param sort_columns The column indexes for all sorted columns
 * \param sort_orders The ascending/descending order for each sorting column
 * \param partition_keys The "spliting" point to partition the sframe

 * \return a pointer to a persisted sarray object, the sarray stores serialized
 *   values of partitioned sframe, with values between segments relatively ordered
**/
std::shared_ptr<sarray<std::string>> scatter_partition(
  const std::shared_ptr<lazy_sframe> sframe_ptr,
  const std::vector<size_t>& sort_columns,
  const std::vector<bool>& sort_orders,
  const std::vector<flexible_type>& partition_keys,
  std::vector<size_t>& partition_sizes,
  std::vector<bool>& partition_sorted) {

  log_func_entry();

  size_t num_partitions_keys = partition_keys.size() + 1;
  logstream(LOG_INFO) << "Scatter partition for sort, scatter to " +
        std::to_string(num_partitions_keys) + " partitions" << std::endl;

  // Preparing resulting sarray for writing
  auto parted_array = std::make_shared<sarray<std::string>>();
  parted_array->open_for_write(num_partitions_keys);

  std::vector<sarray<std::string>::iterator> outiter_vector(num_partitions_keys);
  for(size_t i = 0; i < num_partitions_keys; ++i) {
    outiter_vector[i] = parted_array->get_output_iterator(i);
  }

  // Create a mutex for each partition
  std::vector<mutex> outiter_mutexes(num_partitions_keys);
  std::vector<mutex> sorted_mutexes(num_partitions_keys);
  std::vector<flex_list> first_sort_key(num_partitions_keys);
  std::vector<size_t> partition_size_in_bytes(num_partitions_keys, 0);
  std::vector<size_t> partition_size_in_rows(num_partitions_keys, 0);

  // Iterate over each row of the given SFrame, compare against the partition key,
  // and write that row to the appropriate segment of the partitioned sframe_ptr
  size_t num_keys = sort_orders.size();
  size_t dop = thread::cpu_count();
  less_than_full_function less_than(sort_orders);
  auto parallel_iterator = sframe_ptr->get_iterator(dop);

  parallel_for(0, dop, [&](size_t segment_id) {
    oarchive oarc;
    std::vector<flexible_type> sort_keys(sort_columns.size());
    while(true) {
      auto items = parallel_iterator->get_next(segment_id, graphlab::DEFAULT_SARRAY_READER_BUFFER_SIZE);
      if (items.size() == 0) break;

      for(auto& item: items) {
        // extract sort key
        for(size_t i = 0; i < num_keys; i++) {
          sort_keys[i] = item[sort_columns[i]];
        }

        // find which partition the value belongs to
        size_t partition_id = num_partitions_keys - 1;
        for(size_t i = 0; i < partition_keys.size() ; i++) {
          const flex_list& partition_key = partition_keys[i].get<flex_list>();
          if((sort_keys == partition_key) || less_than(sort_keys, partition_key)) {
            partition_id = i;
            break;
          }
        }
        DASSERT_TRUE(partition_id < num_partitions_keys);

        sorted_mutexes[partition_id].lock();
        if(partition_sorted[partition_id]) {
          if(first_sort_key[partition_id].size() == 0) {
            first_sort_key[partition_id] = sort_keys;
          } else {
            if(first_sort_key[partition_id] != sort_keys) {
              partition_sorted[partition_id] = false;
            }
          }
        }
        sorted_mutexes[partition_id].unlock();

        // stream the key and value to output segment
        for(auto& k : item) {  oarc << k; }
        std::string s = std::string(oarc.buf, oarc.off);

        // write to coresponding output segment
        outiter_mutexes[partition_id].lock();

        // Calculate roughly how much memory each partition will take up when
        // loaded to be sorted
        partition_size_in_bytes[partition_id] += s.size();
        ++partition_size_in_rows[partition_id];

        *(outiter_vector[partition_id]) = std::move(s);
        ++outiter_vector[partition_id];

        outiter_mutexes[partition_id].unlock();
        oarc.off = 0;
      }
    }
    free(oarc.buf);
  });

  parted_array->close();

  // Calculate how large we think each row is.  This is somewhat ad-hoc, but
  // gives a decent proxy on how many bytes each segment will take up when
  // loaded into memory.  The fudge factor here is accounting for overhead due
  // to flexible_type, and oddities when representing numbers as strings.
  size_t fudge_factor_per_row = 0;
  for(const auto &type: sframe_ptr->column_types()) {
    switch(type) {
      case flex_type_enum::STRING:
        fudge_factor_per_row += 32;
        break;
      case flex_type_enum::INTEGER:
        fudge_factor_per_row += 4;
        break;
      case flex_type_enum::FLOAT:
        fudge_factor_per_row += 4;
        break;
      default:
        break;
    }
  }

  // In case we never find a type that has a fudge factor
  if(fudge_factor_per_row < 1)
    fudge_factor_per_row = 1;



  for(size_t i = 0; i < num_partitions_keys; ++i) {
    partition_size_in_bytes[i] += (partition_size_in_rows[i] * fudge_factor_per_row);
    logstream(LOG_INFO) << "Size of partition " << i << ": " <<
      partition_size_in_bytes[i] << std::endl;
  }

  partition_sizes = partition_size_in_bytes;

  return parted_array;
}

/**
** Sort the whole sframe in memory.
** This is used in case the sframe is small and we can sort in memory
**/
std::shared_ptr<sframe> sort_sframe_in_memory(
  std::shared_ptr<lazy_sframe> sframe_ptr,
  std::vector<size_t> sort_columns,
  std::vector<bool> sort_orders) {

  size_t num_rows = sframe_ptr->size();
  auto iter = sframe_ptr->get_iterator(1);
  auto rows = iter->get_next(0, num_rows);
  DASSERT_TRUE(rows.size() == num_rows);

  std::sort(rows.begin(), rows.end(), less_than_partial_function(sort_columns, sort_orders));

  // persist to disk
  // Note: we could potentially keep this in meory but it may take too much
  // memory in case of big enough sframe, it is better we dump it to disk here
  auto ret = std::make_shared<sframe>();
  ret->open_for_write(sframe_ptr->column_names(), sframe_ptr->column_types(), "", 1);
  std::move(rows.begin(), rows.end(), ret->get_output_iterator(0));
  ret->close();
  return ret;
}

} // namespace sframe_sort_impl

} // namespace graphlab

#endif
