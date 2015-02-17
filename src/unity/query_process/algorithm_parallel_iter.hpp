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
#ifndef GRAPHLAB_UNITY_SITERABLE_ALGORITHMS_PARALLEL_ITER_HPP
#define GRAPHLAB_UNITY_SITERABLE_ALGORITHMS_PARALLEL_ITER_HPP
#include <set>
#include <functional>
#include <algorithm>
#include <cmath>
#include <iterator>
#include <system_error>
#include <fileio/sanitize_url.hpp>
#include <sframe/groupby.hpp>
#include <sframe/csv_writer.hpp>
#include <sframe/sframe_config.hpp>
#include <util/cityhash_gl.hpp>
#include <unity/query_process/lazy_sarray.hpp>
#include <unity/query_process/lazy_sframe.hpp>
#include <unity/lib/flex_dict_view.hpp>

namespace graphlab {

/**
 * This file wraps all functions that support parallel iterations. It is a sister file of
 * algorithm.hpp, provides similar functionality but different input interface. Later this
 * file may be merged with the other file.
**/

/**
 * Performs a reduction on input in parallel, this function decides the
 * degree of parallelism, usually depend on number of CPUs.
 *
 * \param input The iterator supports parallel batch reading of items
 * \param reduce_fn The reduction function to use. This must be of the form
 *                 bool f(const array_value_type&, reduction_type&).
 * \param init The initial value to use in the reduction
 *
 * \tparam ReduceFunctionType The result type of each reduction.
 * \tparam FunctionType The type of the reduce function
 * \tparam AggregateFunctionType The type of the reduce function
 *
 */
template <typename ResultType, typename T, typename ReduceFunctionType, typename AggregateFunctionType>
ResultType reduce(
  std::shared_ptr<lazy_sarray<T>> input,
  ReduceFunctionType reduce_fn,
  AggregateFunctionType aggregate_fn,
  ResultType init = ResultType()) {

  log_func_entry();

  size_t dop = thread::cpu_count();

  std::vector<ResultType> ret;
  ret.resize(dop, init);

  auto input_iterator = input->get_iterator( dop, false);

  // perform a parallel for through the list of segments
  parallel_for(0, dop,
     [&](size_t idx) {

      std::vector<T> items;
      ResultType reduce_result = init;

      while(true) {
        items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) {
          break;
        }

        for(auto i = items.begin(); i != items.end(); ++i) {
          if (!reduce_fn(*i, reduce_result)) break;
        }
      }

      ret[idx] = reduce_result;
     });


  // final reduce
  ResultType result = init;
  for(auto i = ret.begin(); i != ret.end(); ++i) {
    aggregate_fn(*i, result);
  }

  return result;
}

/**
 * Copies the first "limit" number of items to the output indicated by "output".
 * The maximum number of items to copy is regulated by parameter "limit"

 * LazyInput can be either lazy_sframe or lazy_sarray<T>
 *
 * \param input The parallel iterator to read information from
 * \param output The output iterator to write to.
 * \param limit The number of items to copy
 */
template <typename T, typename LazyInput, typename Iterator>
void copy(
  std::shared_ptr<LazyInput> input,
  Iterator output,
  size_t limit=(size_t)(-1)) {

  log_func_entry();

  auto input_iterator = input->get_iterator(1, false /* do not materialize  */);

  // for each segment
  // copy into the output
  size_t items_left = limit;
  size_t items_to_read;
  std::vector<T> items;
  while(items_left > 0) {
    items_to_read = std::min(items_left, sframe_config::SFRAME_READ_BATCH_SIZE);
    items = input_iterator->get_next(0, items_to_read);

    if (items.size() == 0) break;

    for(auto i = items.begin(); i != items.end(); i++) {
      *output = *i;
      output++;
    }

    items_left -= items.size();
  }
}

/**
 * Persist the input to a sarray using the default number of segments
 *
 * \param input The parallel iterator to read information from
 * \param type The type of sarray item
 * \param file_name Optional parameter, the file name for the sarray, must end with ".sidx".
 *        If the file name is not provided, then the sarray is a temporary file and the name will
 *        be generated automatically
 *
 *  Returns a shared pointer to the newly created sarray object
 */
template<typename T>
inline std::shared_ptr<sarray<T>> save_sarray(
  lazy_sarray<T>& input,
  flex_type_enum type,
  std::string file_name = std::string()) {

  log_func_entry();

  if (!file_name.empty()) {
    std::string expected_ext(".sidx");
    if (!boost::algorithm::ends_with(file_name, expected_ext)) {
      log_and_throw("Index file must end with " + expected_ext);
    }
  }

  auto output_sarray_ptr = std::make_shared<sarray<T>>();
  if (!file_name.empty()) {
    output_sarray_ptr->open_for_write(file_name);
  } else {
    output_sarray_ptr->open_for_write();
  }

  output_sarray_ptr->set_type(type);

  size_t dop = output_sarray_ptr->num_segments();
  auto input_iterator = input.get_iterator(dop, false);

  // get writer iterators
  std::vector<decltype(output_sarray_ptr->get_output_iterator(0))> writer_iters(dop);
  for (size_t i = 0; i < dop; ++i) {
    writer_iters[i] = output_sarray_ptr->get_output_iterator(i);
  }

  // write items to output in parallel
  parallel_for(0, dop,
   [&](size_t idx) {

    std::vector<T> items;
    while(true) {
      items = input_iterator->get_next(idx, graphlab::sframe_config::SFRAME_READ_BATCH_SIZE);
      if (items.size() == 0) {
        break;
      }

      for(auto i = items.begin(); i != items.end(); ++i) {
        *(writer_iters[idx]) = *i;
        writer_iters[idx]++;
      }
    }
   });

   output_sarray_ptr->close();
   return output_sarray_ptr;
}

/**
 * Persist the input to a sframe using the default number of segments
 *
 * \tparam S either a lazy_sframe or a lazy_sarray object

 * \param input lazy sframe or lazy sarray that returns vector of values
 * \param type The type of sarray item
 * \param column_types The types for all columns in the sframe
 * \param column_names The names for all columns in the sframe
 * \param file_name Optional parameter, the file name for the sarray, must end with ".sidx".
 *        If the file name is not provided, then the sarray is a temporary file and the name will
 *        be generated automatically
 *
 *  Returns a shared pointer to the newly created sframe object
 */
template<typename S>
inline std::shared_ptr<sframe> save_sframe(
  std::shared_ptr<S> input,
  std::vector<std::string> column_names,
  std::vector<flex_type_enum> column_types,
  std::string file_name = std::string()) {

  log_func_entry();

  if (!file_name.empty()) {
   std::string expected_ext(".frame_idx");
   if(!boost::algorithm::ends_with(file_name, expected_ext)) {
     log_and_throw("SFrame index file must end with " + expected_ext);
   }
  }

  auto output_sframe_ptr = std::make_shared<sframe>();
  if (!file_name.empty()) {
    output_sframe_ptr->open_for_write(column_names, column_types, file_name);
  } else {
    output_sframe_ptr->open_for_write(column_names, column_types);
  }

  //Make sure all writers are healthy before writing.
  size_t dop = output_sframe_ptr->num_segments();
  for (size_t i = 0; i < dop; ++i) {
    output_sframe_ptr->get_output_iterator(i);
  }

  // write items to output in parallel, cannot materialize here because it may be a
  // le_sframe that emits vector of data and cannot persist to a sarray
  auto vector_iterator = input->get_iterator(dop, false /* do not materialize */);

  parallel_for(0, dop,
   [&](size_t idx) {

    auto output_iter = output_sframe_ptr->get_output_iterator(idx);

    while(true) {
      // Read a batch of values

      std::vector<std::vector<flexible_type>> items
          = vector_iterator->get_next(idx, graphlab::sframe_config::SFRAME_READ_BATCH_SIZE);
      if (items.size() == 0) break;

      std::move(items.begin(), items.end(), output_iter);
    }
   });

  output_sframe_ptr->close();
  return output_sframe_ptr;
}

/**
 *  Save a given sframe as csv file format
 *
 * \param url location of the output file
 * \param input The pointer to lazy sframe object
 * \param column_names The names for columns in output sframe
 **/
inline void save_sframe_to_csv(
  std::string url,
  std::shared_ptr<lazy_sframe> input,
  std::vector<std::string> column_names,
  csv_writer& writer) {

  general_ofstream fout(url);
  if (!fout.good()) {
    log_and_throw(std::string("Unable to open " + sanitize_url(url) + " for write"));
  }


  // write the header
  size_t num_cols = column_names.size();
  if (num_cols == 0) return;

  if (writer.header) writer.write_verbatim(fout, column_names);

  auto iterator = input->get_iterator(1, false);
  std::vector<std::vector<flexible_type>> items(num_cols);

  // size_t buflen = 512 * 1024;
  // char* buf = new char[buflen];
  // size_t bytes_written = 0;

  while(true) {

    // Read a batch of values
    auto items = iterator->get_next(0, sframe_config::SFRAME_READ_BATCH_SIZE);
    if (items.size() == 0) break;

    // go through the rows and write to file
    for(auto& item : items) {
      writer.write(fout, item);
      // Switch to the code below for faster performance.
      // #include <sframe/sframe_io.hpp>
      // bytes_written = sframe_row_to_csv(item, buf, buflen);
      // if (bytes_written == buflen) {
      //   delete buf;
      //   fout.close();
      //   throw(std::ios_base::failure("Row size exceeds max buf size."));
      // }
      // fout.write(buf, bytes_written);
    }

  }
  // delete buf[];

  if (!fout.good()) {
    log_and_throw_io_failure("Fail to write.");
  }
}

template<typename T>
inline std::shared_ptr<sarray<T>> group(
  std::shared_ptr<lazy_sarray<T>> input) {

  size_t out_nsegments = std::max<size_t>(16, thread::cpu_count() * std::max<size_t>(1, log2(thread::cpu_count())));

  auto comparator = [=](const T& a, const T& b) {
    auto atype = a.get_type();
    auto btype = b.get_type();
    if (atype < btype) {
      return true;
    } else if(atype > btype) {
      return false;
    } else if (atype == flex_type_enum::UNDEFINED && btype == flex_type_enum::UNDEFINED) {
      return false;
    } else {
      return a < b;
    }
  };

  hash_bucket_container<T> hash_container(out_nsegments, comparator);

  auto input_iterator = input->get_iterator(out_nsegments);

  logstream(LOG_DEBUG) << "Group: shuffling" << std::endl;
  parallel_for(0, out_nsegments, [&](size_t segmentid) {
    bool done = false;
    while (!done) {
      // Hash input elements to worker's bucket
      std::vector<T> input_chunk = input_iterator->get_next(segmentid, graphlab::sframe_config::SFRAME_READ_BATCH_SIZE);
      done = input_chunk.size() != graphlab::sframe_config::SFRAME_READ_BATCH_SIZE;
      for (const T& val : input_chunk) {
        size_t hash = hash64(val.hash()) % out_nsegments;
        hash_container.add(val, hash);
      }
    }
  });

  logstream(LOG_DEBUG) << "Group: sorting and writing out" << std::endl;
  std::shared_ptr<sarray<T>> out_sarray = std::make_shared<sarray<T>>();
  out_sarray->open_for_write(out_nsegments);
  hash_container.sort_and_write(*out_sarray);
  return out_sarray;
}


/**
 * Combines the contents of multiple SArrays into one
 * The result will be ordered concatenantion of all rows
 * of the arrays
 *
 * This class accomplishes the abstract equivalent of:
 * \code
 * for each SArray in lazy_array_vector
 *   for each x in sarray
 *     write x to output
 * \endcode
 *
 * \param lazy_array_vector The list of SArrays to read from
 * \param output The output iterator to write to.
 */
template <typename T, typename SWriter>
void combine(std::vector<std::shared_ptr<lazy_sarray<T>>> lazy_array_vector, SWriter&& writer) {

  log_func_entry();

  // compute total items
  size_t total_items = 0;
  for(size_t i = 0; i < lazy_array_vector.size(); i++) {
    total_items += lazy_array_vector[i]->size();
  }

  // compute output segment size
  size_t segment_length = total_items / writer.num_segments();
  segment_length += segment_length * writer.num_segments() < total_items ? 1 : 0;
  logstream(LOG_DEBUG) << "Total items " << total_items << ", segment length: " << segment_length << std::endl;

  // write to output, we iterator through all elements for each sarray and write
  // to output sarray, the elements are written in output in order
  size_t output_segment_idx = 0;
  size_t items_in_current_segment = 0;
  auto output = writer.get_output_iterator(output_segment_idx);

  for(size_t array_index = 0; array_index < lazy_array_vector.size(); array_index++) {

    logstream(LOG_DEBUG) << "writing array " << array_index << " to output" << std::endl;
    auto iterator = lazy_array_vector[array_index]->get_iterator(1);

    auto items = iterator->get_next(0, sframe_config::SFRAME_READ_BATCH_SIZE);
    size_t begin_index = 0;
    size_t total_items_written = 0;
    while(true) {
      size_t items_to_move = std::min(segment_length - items_in_current_segment, items.size() - begin_index);
      for (size_t i = begin_index; i < begin_index + items_to_move; i++) {
        *output = items[i];
      }

      begin_index += items_to_move;
      items_in_current_segment += items_to_move;
      total_items_written += items_to_move;

      // refill items if needed
      if (begin_index == items.size()) {
        logstream(LOG_DEBUG) << "reading next batch, items written: " << items_in_current_segment << ", output segment: " << output_segment_idx << std::endl;
        items = iterator->get_next(0, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) break;
        begin_index = 0;
      }

      // switch output segment if needed
      if (items_in_current_segment == segment_length) {
        logstream(LOG_DEBUG) << "switching output segment, items written: " << items_in_current_segment << ", output segment: " << output_segment_idx << std::endl;
        items_in_current_segment = 0;
        output_segment_idx++;

        if (output_segment_idx < writer.num_segments()) {
          output = writer.get_output_iterator(output_segment_idx);
        } else {
          DASSERT_MSG(total_items_written == total_items, "All items should have been written when reaching last segment");
        }
      }
    }
  }
}

/**
 * Filters input to output calling the filterfn on each input and emitting
 * the input to output only if the filter function evaluates to true.
 *
 * This class accomplishes the abstract equivalent of
 * \code
 * for each x in input:
 *    if (filterfn(x)) write x to output
 * \endcode
 *
 * The input object must be a descendent of \ref siterable, and the output
 * object must be a descendent of \ref swriter_base. \ref sarray and
 * \ref swriter are two common instances of either.
 *
 * The output object should be of the same number of segments as the input
 * object. If they are of different number of segments, this function will
 * attempt to change the number of segments of the output object. Changing the
 * number of segments is generally a successful operation unless writes have
 * already occured on the output. If the number of segments do not match,
 * and if the number of output segments cannot be set, this function
 * will throw a string exception and fail.
 *
 * \param input The input to read from. Must be a descendent of siterable
 * \param output The output writer to write to. Must be a descendent of swriter_base
 * \param filterfn The filter operation to perform on the input. If the filterfn
 *                 evaluates to true, the input is copied to the output.
 */
template <typename S, typename T, typename FilterFn,
typename = typename std::enable_if<sframe_impl::is_sarray_like<T>::value>::type>
void copy_if(std::shared_ptr<S> input, T&& output,
             FilterFn filterfn,
             int random_seed=std::time(NULL)) {
  log_func_entry();
  ASSERT_TRUE(output.is_opened_for_write());

  size_t dop = output.num_segments();
  auto input_iterator = input->get_iterator(dop);

  // perform a parallel for through the list of segments
  parallel_for(0, dop,
              [&](size_t idx) {
              if (random_seed != -1){
               random::get_source().seed(random_seed + idx);
              }

              auto output_iter = output.get_output_iterator(idx);
              while(true) {
                auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
                if (items.size() == 0) break;
                std::copy_if(items.begin(), items.end(), output_iter, filterfn);
              }
            });
}

/**
 * Split input to output1 and output2 calling the filterfn on each input and emitting
 * the input to output1 if the filter function evaluates to true, output2 otherwise.
 *
 * This class accomplishes the abstract equivalent of
 * \code
 * for each x in input:
 *    if (filterfn(x)) write x to output1
 *    else write x to output2
 * \endcode
 *
 * The input object must be a descendent of \ref siterable, and the output
 * object must be a descendent of \ref swriter_base. \ref sarray and
 * \ref swriter are two common instances of either.
 *
 * The output object should be of the same number of segments as the input
 * object. If they are of different number of segments, this function will
 * attempt to change the number of segments of the output object. Changing the
 * number of segments is generally a successful operation unless writes have
 * already occured on the output. If the number of segments do not match,
 * and if the number of output segments cannot be set, this function
 * will throw a string exception and fail.
 *
 * \param input The input to read from. Must be a descendent of siterable
 * \param output1 The output writer to write to. Must be a descendent of swriter_base
 * \param output2 The output writer to write to. Must be a descendent of swriter_base
 * \param filterfn The filter operation to perform on the input. If the filterfn
 *                 evaluates to true, the input is copied to the output1, else output2.
 */
template <typename S, typename T, typename FilterFn,
typename = typename std::enable_if<sframe_impl::is_sarray_like<T>::value>::type>
void split(std::shared_ptr<S> input, T&& output1, T&& output2,
           FilterFn filterfn,
           int random_seed=std::time(NULL)) {
  log_func_entry();
  ASSERT_TRUE(output1.is_opened_for_write());
  ASSERT_TRUE(output2.is_opened_for_write());
  if (output1.num_segments() != output2.num_segments()) {
    log_and_throw("Expects two outputs to have the same number of segments");
  }

  size_t dop = output1.num_segments();
  auto input_iterator = input->get_iterator(dop);

  // perform a parallel for through the list of segments
  parallel_for(0, dop,
               [&](size_t idx) {
                 if (random_seed != -1) {
                    random::get_source().seed(random_seed + idx);
                 }
                 auto output_iter1 = output1.get_output_iterator(idx);
                 auto output_iter2 = output2.get_output_iterator(idx);

                 while(true) {
                   auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
                   if (items.size() == 0) break;

                   auto iter = items.begin();
                   while(iter != items.end()) {
                     auto& val = *iter;
                     if (filterfn(val)) {
                       *output_iter1 = val;
                       ++output_iter1;
                     } else {
                       *output_iter2 = val;
                       ++output_iter2;
                     }
                     ++iter;
                   }
                 }
               });
}

/**
 * Transform the input SArray using the given transform function and write result
 * to output
 * \param input Input sarray to transform from
 * \param output Ouptut SArray like object that supports get_iterator and write to it
 * \param transformfn The transform function
 *
 * \tparam T output type
 * \tparam TransformFn the transform function type
**/
template <typename T, typename TransformFn,
typename = typename std::enable_if<sframe_impl::is_sarray_like<T>::value>::type>
void transform(
  std::shared_ptr<lazy_sarray<flexible_type>> input, T&& output, TransformFn transformfn) {

  log_func_entry();
  ASSERT_TRUE(output.is_opened_for_write());

  size_t dop = output.num_segments();
  auto input_iterator = input->get_iterator(dop);
  parallel_for(0, dop,
    [&](size_t idx) {
      auto output_iter = output.get_output_iterator(idx);

      while(true) {
        auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) break;

        for(auto& item : items) {
          *output_iter = transformfn(item);

          ++output_iter;

        } // for(auto& item)
      } // while(true)
  });
}

/**
 * Transform the input SFrame using the give transform function and write result
 * to output SFrame, each input row may generate multiple output rows
 * \param input sframe to transform from
 * \param output sframe/sarray like object that support writing
 * \param multi_transformfn The transform function that given one row, produces multiple rows
 *
 * \tparam T output type
 * \tparam TransformFn the transform function type
**/
template <typename T, typename TransformFn,
typename = typename std::enable_if<sframe_impl::is_sarray_like<T>::value>::type>
void multi_transform(
  std::shared_ptr<lazy_sframe> input,
  T&& output, TransformFn multi_transformfn) {

  size_t dop = output.num_segments();
  auto input_iterator = input->get_iterator(dop);

  parallel_for(0, dop,
    [&](size_t idx) {
      auto output_iter = output.get_output_iterator(idx);

      while(true) {
        auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) break;

        for(auto& item : items) {
          auto new_items = multi_transformfn(item);
          for(auto& new_item : new_items) {
            *output_iter = new_item;
            ++output_iter;
          }
        } // for(auto& item)
      } // while(true)
  });
}

/**
 * Pack the input columns into one dictionary column. The key in the dictionary
 * value comes from coresponding position in input "keys", and value comes from
 * each column
 *
 * \param lazy_array_vector The list of lazy_sarray to read value from
 * \param keys The key for dictionary value
 * \param fill_na: value to fill when missing value is encountered
 * \param dtype: resulting sarray type
 * \param output The output SArray
 *
**/
inline void pack(
  std::vector<std::shared_ptr<lazy_sarray<flexible_type>>>& lazy_array_vector,
  const std::vector<std::string>& keys,
  const flexible_type fill_na,
  flex_type_enum dtype,
  std::shared_ptr<sarray<flexible_type>> output) {

  log_func_entry();
  ASSERT_TRUE(output->is_opened_for_write());
  ASSERT_TRUE(keys.size() == lazy_array_vector.size());
  ASSERT_TRUE(keys.size() > 0);

  size_t dop = output->num_segments();
  size_t num_cols = keys.size();

  // get input iterators
  std::vector<std::unique_ptr<parallel_iterator<flexible_type>>> input_iterators;
  for(auto lazy_array : lazy_array_vector) {
    input_iterators.push_back(lazy_array->get_iterator(dop));
  }

  // pack values in parallel
  parallel_for(0, dop,
              [&](size_t idx) {

      std::vector<std::vector<flexible_type>> items(num_cols);
      auto output_iter = output->get_output_iterator(idx);

      while(true) {
        for (size_t i = 0; i < num_cols; i++) {
          items[i] = input_iterators[i]->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
          ASSERT_TRUE(i == 0 || items[i].size() == items[0].size());
        }

        if (items[0].size() == 0) break;;

        // emit one value at a time
        for (size_t row = 0; row < items[0].size(); row++) {
          if (dtype == flex_type_enum::DICT) {
            flex_dict out_val;
            for (size_t col = 0; col < num_cols; col++) {
              if (items[col][row] != FLEX_UNDEFINED) {
                out_val.push_back(std::make_pair(keys[col], std::move(items[col][row])));
              } else if (fill_na != FLEX_UNDEFINED) {
                out_val.push_back(std::make_pair(keys[col], fill_na));
              }
            }
            *output_iter = out_val;
          } else if (dtype == flex_type_enum::VECTOR) {
            flex_vec out_val;
            for (size_t col = 0; col < num_cols; col++) {
              if (!items[col][row].is_na()) {
                out_val.push_back((double)items[col][row]);
              } else {
                if (fill_na == FLEX_UNDEFINED) {
                  out_val.push_back(NAN);
                } else {
                  out_val.push_back((double)fill_na);
                }
              }
            }
            *output_iter = out_val;
          } else {
            flex_list out_val;
            for (size_t col = 0; col < num_cols; col++) {
              if (items[col][row] != FLEX_UNDEFINED) {
                out_val.push_back(std::move(items[col][row]));
              } else {
                out_val.push_back(fill_na);
              }
            }
            *output_iter = out_val;
          }

          output_iter++;
        }
      }
  });
}
/**
 * given an input SArray of datetime, expand it to multiple output
 * columns based on elements. Each element maps to one column in output
 *
 * \param input The input SArray to read from
 * \param elements Specify the elements from the datetime to expand
 * \param output The sframe to write to
 */
inline void expand(
  const std::shared_ptr<lazy_sarray<flexible_type>>& input,
  const std::vector<flexible_type>& elements,
  sframe& output) {

  log_func_entry();
  ASSERT_TRUE(elements.size() > 0);

  size_t num_cols = elements.size();
  size_t dop = output.num_segments();
  ASSERT_TRUE(output.is_opened_for_write());

  flex_type_enum dtype = input->get_type();
  ASSERT_TRUE(dtype == flex_type_enum::DATETIME);
  // get input iterators
  auto input_iterator = input->get_iterator(dop);

  parallel_for(0, dop,
              [&](size_t idx) {

      auto out_iterator = output.get_output_iterator(idx);

      while(true) {
        auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) break;

        for (auto& val : items) {
          std::vector<flexible_type> row(num_cols);

          if (val.get_type() == flex_type_enum::UNDEFINED) {
            for(size_t i = 0; i < num_cols; i++) {
              row[i] = FLEX_UNDEFINED;
            }
          } else {

              for(size_t i = 0; i < num_cols; i++) {
                const flex_date_time & dt = val.get<flex_date_time>();
                boost::posix_time::ptime ptime_val = flexible_type_impl::my_from_time_t(dt.first + (dt.second * 1800));
                tm _tm = boost::posix_time::to_tm(ptime_val);
                if(elements[i] == "year")
                    row[i] = _tm.tm_year + 1900;
                else if(elements[i] == "month")
                    row[i] = _tm.tm_mon + 1; // +1 is for adjustment with datetime object in python,tm is from 0-11 while datetime is from 1-12
                else if(elements[i] == "day")
                    row[i] = _tm.tm_mday;
                else if(elements[i] == "hour")
                    row[i] = _tm.tm_hour;
                else if(elements[i] == "minute")
                    row[i] = _tm.tm_min;
                else if(elements[i] == "second")
                    row[i] = _tm.tm_sec;
                else if(elements[i] == "tzone")
                    row[i] = dt.second/2.0; // we store half an hour in the datetime timezone offset.
              }
          }

          *out_iterator = std::move(row);
          out_iterator++;
        }
      }
  });
}

/**
 * given an input SArray of dict type, unpack key/value pair to multple output
 * columns, each unique key map to one column in output
 *
 * \param input The input SArray to read from
 * \param keys The set of keys to extract dict value from or the index into the
 *   list or array to extract value from
 * \param output The sframe to write to
 * \param na_value: Convert all na_value to missing value if given
 */
inline void unpack(
  const std::shared_ptr<lazy_sarray<flexible_type>>& input,
  const std::vector<flexible_type>& keys,
  sframe& output,
  const flexible_type& na_value) {

  log_func_entry();
  ASSERT_TRUE(keys.size() > 0);

  size_t num_cols = keys.size();
  size_t dop = output.num_segments();
  ASSERT_TRUE(output.is_opened_for_write());

  flex_type_enum dtype = input->get_type();

  // get input iterators
  auto input_iterator = input->get_iterator(dop);

  parallel_for(0, dop,
              [&](size_t idx) {

      auto out_iterator = output.get_output_iterator(idx);

      while(true) {
        auto items = input_iterator->get_next(idx, sframe_config::SFRAME_READ_BATCH_SIZE);
        if (items.size() == 0) break;

        for (auto& val : items) {
          std::vector<flexible_type> row(num_cols);

          if (val.get_type() == flex_type_enum::UNDEFINED) {
            for(size_t i = 0; i < num_cols; i++) {
              row[i] = FLEX_UNDEFINED;
            }
          } else {

            if (dtype == flex_type_enum::DICT) {
              const flex_dict_view& dict_val(val);
              for(size_t i = 0; i < num_cols; i++) {
                if (dict_val.has_key(keys[i]) && dict_val[keys[i]] != na_value) {
                  row[i] = dict_val[keys[i]];
                } else {
                  row[i] = FLEX_UNDEFINED;
                }
              }
            } else if(dtype == flex_type_enum::LIST) {
              for(size_t i = 0; i < num_cols; i++) {
                size_t index = keys[i].get<flex_int>();
                if (val.size() <= index || val.array_at(index) == na_value) {
                  row[i] = FLEX_UNDEFINED;
                } else {
                  row[i] = val.array_at(index);
                }
              }
            } else {
              DASSERT_MSG(dtype == flex_type_enum::VECTOR, "dtype for unpack is not expected!");
              for(size_t i = 0; i < num_cols; i++) {
                size_t index = keys[i].get<flex_int>();
                if (val.size() <= index || val[index] == na_value || std::isnan(val[index])) {
                  row[i] = FLEX_UNDEFINED;
                } else {
                  row[i] = val[index];
                }
              }
            }
          }

          *out_iterator = std::move(row);
          out_iterator++;
        }
      }
  });
}


} // namespace graphlab
#endif
