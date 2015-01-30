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
#include <cmath>
#include <boost/heap/priority_queue.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/flex_dict_view.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/variant.hpp>
#include <unity/query_process/lazy_sarray.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/curl_downloader.hpp>
#include <fileio/sanitize_url.hpp>
#include <fileio/fs_utils.hpp>
#include <graphlab/util/file_line_count_estimator.hpp>
#include <util/cityhash_gl.hpp>
#include <util/hash_value.hpp>
#include <parallel/atomic.hpp>
#include <parallel/lambda_omp.hpp>
#include <unity/lib/unity_sarray_binary_operations.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/parallel_csv_parser.hpp>
#include <sframe/generic_avro_reader.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>
#include <sframe/sframe_constants.hpp>
#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>
#include <unity/lib/auto_close_sarray.hpp>
#include <unity/lib/unity_global.hpp>
#include <image/image_util.hpp>

namespace graphlab {


unity_sarray::unity_sarray() { }

unity_sarray::~unity_sarray() {
  clear();
}

unity_sarray::unity_sarray(const unity_sarray& other) {
  construct_from_unity_sarray(other);
};

unity_sarray& unity_sarray::operator=(const unity_sarray& other) {
  construct_from_unity_sarray(other);
  return *this;
};

void unity_sarray::construct_from_vector(const std::vector<flexible_type>& vec, flex_type_enum type) {
  log_func_entry();
  clear();

  auto sarray_ptr = std::make_shared<sarray<flexible_type>>();

  sarray_ptr->open_for_write();
  sarray_ptr->set_type(type);

  // ok. copy into the writer.
  graphlab::copy(vec.begin(), vec.end(), *sarray_ptr);
  sarray_ptr->close();

  construct_from_sarray(sarray_ptr);
}

void unity_sarray::construct_from_const(const flexible_type& value, size_t size) {
  log_func_entry();
  clear();
  auto type = value.get_type();
  // if I get a None, lets make a constant column of float, all None
  if (type == flex_type_enum::UNDEFINED) {
    type = flex_type_enum::FLOAT;
  }
  auto le_constant_ptr = std::make_shared<le_constant>(value, size);
  construct_from_lazy_operator(le_constant_ptr, false, type);
}

void unity_sarray::construct_from_sarray(std::shared_ptr<sarray<flexible_type>> s_ptr) {
  clear();

  auto le_sarray_ptr = std::make_shared<le_sarray<flexible_type>>(s_ptr);
  construct_from_lazy_operator(le_sarray_ptr, true, s_ptr->get_type());
}

void unity_sarray::construct_from_lazy_operator(std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> input, bool materialized, flex_type_enum type) {
  clear();
  m_lazy_sarray = std::make_shared<lazy_sarray<flexible_type>>(input, materialized, type);
}

void unity_sarray::construct_from_lazy_sarray(std::shared_ptr<lazy_sarray<flexible_type>> lazy_sarray) {
  clear();
  m_lazy_sarray = lazy_sarray;
}

void unity_sarray::construct_from_sarray_index(std::string index) {
  logstream(LOG_INFO) << "Construct sarray from location: " << sanitize_url(index) << std::endl;
  clear();
  auto status = fileio::get_file_status(index);


  if (fileio::is_web_protocol(index)) {
    // if it is a web protocol, we cannot be certain what type of file it is.
    // HEURISTIC: 
    //   assume it is a "directory" and try to load dir_archive.ini 
    if (fileio::try_to_open_file(index + "/dir_archive.ini")) {
      status = fileio::file_status::DIRECTORY;
    } else {
      status = fileio::file_status::REGULAR_FILE;
    }
  } 

  if (status == fileio::file_status::MISSING) {
    // missing file. fail quick
    log_and_throw_io_failure(sanitize_url(index) + " not found.");
  } if (status == fileio::file_status::REGULAR_FILE) {
    // its a regular file, load it normally
    auto sarray_ptr = std::make_shared<sarray<flexible_type>>(index);
    m_lazy_sarray = std::make_shared<lazy_sarray<flexible_type>>(
        std::make_shared<le_sarray<flexible_type>>(sarray_ptr),
        true,  // materialized
        sarray_ptr->get_type());
  } else if (status == fileio::file_status::DIRECTORY) {
    // its a directory, open the directory and verify that it contains an
    // sarray and then load it if it does
    dir_archive dirarc;
    dirarc.open_directory_for_read(index);
    std::string content_value;
    if (dirarc.get_metadata("contents", content_value) == false ||
        content_value != "sarray") {
      log_and_throw("Archive does not contain an SArray");
    }
    std::string prefix = dirarc.get_next_read_prefix();
    auto sarray_ptr = std::make_shared<sarray<flexible_type>>(prefix + ".sidx");
    m_lazy_sarray = std::make_shared<lazy_sarray<flexible_type>>(
        std::make_shared<le_sarray<flexible_type>>(sarray_ptr),
        true,  // materialized
        sarray_ptr->get_type());
    dirarc.close();
  }
}

void unity_sarray::append_file_to_sarray(
    std::string url,
    std::shared_ptr<sarray<flexible_type>> sarray_ptr,
    flex_type_enum type,
    size_t cumulative_file_read_sizes,
    size_t total_input_file_sizes,
    size_t& current_output_segment) {
  log_func_entry();

  general_ifstream fin(url);
  if (!fin.good()) {
    log_and_throw_io_failure(std::string("Cannot open ") + sanitize_url(url));
  }
  size_t num_output_segments = sarray_ptr->num_segments();

  csv_line_tokenizer tokenizer;
  tokenizer.init();

  while(fin.good()) {
    flexible_type out(type);
    std::string line;

    // compute the current output segment
    // It really is simply.
    // current_output_segment =
    //         (fin.get_bytes_read() + cumulative_file_read_sizes)
    //          * num_output_segments / total_input_file_sizes;
    // But a lot of sanity checking is required because
    //  - fin.get_bytes_read() may fail.
    //  - files on disk may change after I last computed the file sizes, so
    //    there is no guarantee that cumulatively, they will all add up.
    //  - So, a lot of bounds checking is required.
    //  - Also, Once I advance to a next segment, I must not go back.
    size_t next_output_segment = current_output_segment;
    size_t read_pos = fin.get_bytes_read();
    if (read_pos == (size_t)(-1)) {
      // I don't know where I am in the file. Use what I last know
      read_pos = cumulative_file_read_sizes;
    } else {
      read_pos += cumulative_file_read_sizes;
    }
    next_output_segment = read_pos * num_output_segments / total_input_file_sizes;
    // sanity boundary check
    if (next_output_segment >= num_output_segments) next_output_segment = num_output_segments - 1;
    // we never go back
    current_output_segment = std::max(current_output_segment, next_output_segment);

    auto output = sarray_ptr->get_output_iterator(current_output_segment); // always output to segment 0
    for (size_t ctr = 0; ctr < SARRAY_FROM_FILE_BATCH_SIZE; ++ctr) {
      eol_safe_getline(fin, line);
      if (fin.bad()) {
        log_and_throw_io_failure("Read failed.");
      }
      if (line.empty() && fin.eof()) {
        break;
      }
      const char* buf = line.c_str();
      size_t len = line.size();
      out.reset(type);
      if (!tokenizer.parse_as(&buf, len, out)) {
        std::stringstream ss;
        ss << "Cannot parse \"" << line << "\" as type " << flex_type_enum_to_name(type);
        log_and_throw(ss.str());
      }
      (*output) = std::move(out);
      ++output;
    }
  }
  fin.close();
}


/**
 * Constructs an SArray from a url. Each line of the file will be a row in the
 * resultant SArray, and each row will be of string type. If URL is a directory,
 * or a glob, each matching file will be appended.
 */
void unity_sarray::construct_from_files(std::string url,
                                        flex_type_enum type) {
  std::vector<std::pair<std::string, fileio::file_status>> file_and_status =
      fileio::get_glob_files(url);

  log_func_entry();
  logstream(LOG_INFO) << "Construct sarray from url: " << sanitize_url(url) <<  " type: "
                      << flex_type_enum_to_name(type) << std::endl;
  clear();

  auto sarray_ptr = std::make_shared<sarray<flexible_type>>();
  sarray_ptr->open_for_write(num_temp_directories());
  sarray_ptr->set_type(type);



  // error propagation for bad files
  for (auto p : file_and_status) {
    if (p.second == fileio::file_status::MISSING) {
      log_and_throw_io_failure(std::string("Cannot open ") + sanitize_url(p.first));
    }
  }

  // get the total input size
  size_t cumulative_file_read_sizes = 0;
  size_t total_input_file_sizes = 0;
  std::vector<size_t> file_sizes;
  for (auto file : file_and_status) {
    general_ifstream fin(file.first);
    size_t file_size = fin.file_size();
    file_sizes.push_back(file_size);
    total_input_file_sizes += file_size;
  }
  size_t current_output_segment = 0;
  for (size_t i = 0;i < file_and_status.size(); ++i) {
    if (file_and_status[i].second == fileio::file_status::REGULAR_FILE) {
      logprogress_stream << "Adding file "
                         << sanitize_url(file_and_status[i].first)
                         << " to the array"
                         << std::endl;
      append_file_to_sarray(file_and_status[i].first,
                            sarray_ptr, type, cumulative_file_read_sizes,
                            total_input_file_sizes, current_output_segment);
      cumulative_file_read_sizes += file_sizes[i];
    }
  }

  sarray_ptr->close();
  construct_from_sarray(sarray_ptr);
}

void unity_sarray::construct_from_autodetect(std::string url, flex_type_enum type) {
  auto status = fileio::get_file_status(url);

  if (fileio::is_web_protocol(url)) {
    // if it is a web protocol, we cannot be certain what type of file it is.
    // HEURISTIC: 
    //   assume it is a "directory" and try to load dir_archive.ini 
    if (fileio::try_to_open_file(url + "/dir_archive.ini")) {
      status = fileio::file_status::DIRECTORY;
    } else {
      status = fileio::file_status::REGULAR_FILE;
    }
  }

  if (status == fileio::file_status::MISSING) {
    // missing file. might be a glob. try again using construct_from_file
    construct_from_files(url, type);
  } else if (status == fileio::file_status::DIRECTORY) {
    // it is a directory. first see if it is a directory holding an sarray
    bool is_directory_archive = fileio::try_to_open_file(url + "/dir_archive.ini");
    if (is_directory_archive) {
      construct_from_sarray_index(url);
    } else {
      construct_from_files(url, type);
    }
  } else {
    // its a regular file. This is the tricky case
    if (boost::ends_with(url, ".sidx")) {
      construct_from_sarray_index(url);
    } else {
      construct_from_files(url, type);
    }
  }
}

void unity_sarray::construct_from_avro(std::string url) {
  auto status = fileio::get_file_status(url);
  if (status == fileio::file_status::MISSING) {
    log_and_throw_io_failure(std::string("Cannot open ") + sanitize_url(url));
  } else {
    log_func_entry();

    generic_avro_reader reader(url);
    auto type = reader.get_flex_type();

    if (type == flex_type_enum::UNDEFINED)
      log_and_throw("Avro schema is undefined");

    logstream(LOG_INFO) << "Construct sarray from AVRO url: " << sanitize_url(url) <<  " type: "
                        << flex_type_enum_to_name(type) << std::endl;

    auto sarray_ptr = std::make_shared<sarray<flexible_type>>();
    sarray_ptr->open_for_write();
    sarray_ptr->set_type(type);

    
    size_t current_output_segment = 0; // TO DO: should we write to different segments?

    auto output = sarray_ptr->get_output_iterator(current_output_segment);
    bool has_more = true;
    size_t num_read = 0;
    size_t progress_interval = 10000;

    while (has_more) {
      if ((num_read >= progress_interval) && (num_read % progress_interval == 0)) {
        logprogress_stream << "Added " << num_read << " records to SArray"
                           << std::endl;
      }

      flexible_type record;
      std::tie(has_more, record) = reader.read_one_flexible_type();

      if (record.get_type() != flex_type_enum::UNDEFINED) {
        (*output) = std::move(record);
        ++output;
        ++num_read;
      } else {
        logstream(LOG_WARNING) << "ignoring undefined record" << std::endl;
      }
    }

    sarray_ptr->close();
    construct_from_sarray(sarray_ptr);
  }
}

void unity_sarray::save_array(std::string target_directory) {
  if (!m_lazy_sarray) {
   log_and_throw("Invalid Sarray");
  }

  dir_archive dirarc;
  dirarc.open_directory_for_write(target_directory);
  dirarc.set_metadata("contents", "sarray");
  std::string prefix = dirarc.get_next_write_prefix();
  save_array_by_index_file(prefix + ".sidx");
  dirarc.close();
}

void unity_sarray::save_array_by_index_file(std::string index_file) {
  if (!m_lazy_sarray) {
   log_and_throw("Invalid Sarray");
  }
  graphlab::save_sarray(*(m_lazy_sarray.get()), dtype(), index_file);
}

void unity_sarray::clear() {
  m_lazy_sarray.reset();
}

size_t unity_sarray::size() {
  Dlog_func_entry();
  if (m_lazy_sarray) {
    return m_lazy_sarray->size();
  } else {
    return 0;
  }
}

std::shared_ptr<sarray<flexible_type> > unity_sarray::get_underlying_sarray() {
  Dlog_func_entry();
  if (m_lazy_sarray) {
    return m_lazy_sarray->get_sarray_ptr();
  } else {
    return std::shared_ptr<sarray<flexible_type>>();
  }
}

std::shared_ptr<lazy_sarray<flexible_type> > unity_sarray::get_lazy_sarray() {
  return m_lazy_sarray;
}

flex_type_enum unity_sarray::dtype() {
  Dlog_func_entry();
  if (m_lazy_sarray) {
    return m_lazy_sarray->get_type();
  } else {
    return flex_type_enum::UNDEFINED;
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::head(size_t nrows) {
  log_func_entry();
  std::shared_ptr<unity_sarray> ret(new unity_sarray());
  sarray<flexible_type> out_sarray;
  out_sarray.open_for_write(1);
  out_sarray.set_type(dtype());
  nrows = std::min<size_t>(nrows, size());
  if (m_lazy_sarray) {
    graphlab::copy<flexible_type>(m_lazy_sarray, out_sarray.get_output_iterator(0), nrows);
  }
  out_sarray.close();
  ret->construct_from_sarray(std::make_shared<sarray<flexible_type>>(out_sarray));
  return ret;
}

std::shared_ptr<unity_sarray_base> unity_sarray::transform(const std::string& lambda,
                                                           flex_type_enum type,
                                                           bool skip_undefined,
                                                           int seed) {
  log_func_entry();

  if (m_lazy_sarray) {

    // create a le_transform operator to lazily evaluate this
    auto transform_operator = std::make_shared<le_transform<flexible_type>>(
      this->get_query_tree(),
      lambda,
      skip_undefined,
      seed,
      type
    );

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, type);

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), type);
    return ret;
  }
}


std::shared_ptr<unity_sarray_base> unity_sarray::transform_native(
    const function_closure_info& toolkit_fn_closure,
    flex_type_enum type,
    bool skip_undefined,
    int seed) {
  auto native_execute_function = 
                  get_unity_global_singleton()
                  ->get_toolkit_function_registry()
                  ->get_native_function(toolkit_fn_closure);

  auto fn = [native_execute_function](const flexible_type& f)->flexible_type {
        variant_type var = f;
        return variant_get_value<flexible_type>(native_execute_function({f})); 
      };
  return transform_lambda(fn, type, skip_undefined, seed);
}

std::shared_ptr<unity_sarray_base> unity_sarray::transform_lambda(
    std::function<flexible_type(const flexible_type&)> function,
    flex_type_enum type,
    bool skip_undefined,
    int seed) {

  // build up the lambda required for the transformer
  if (m_lazy_sarray) {
    // create a le_transform operator to lazily evaluate this
    auto transform_operator = std::make_shared<le_transform<flexible_type>>(
      this->get_query_tree(),
      [function, skip_undefined](const flexible_type& f)->flexible_type {
        if (skip_undefined && f.get_type() == flex_type_enum::UNDEFINED) return f;
        return function(f);
      },
      skip_undefined,
      seed,
      type
    );

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, type);

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), type);
    return ret;
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::group() {
  log_func_entry();

  std::shared_ptr<unity_sarray> ret(new unity_sarray);
  if (m_lazy_sarray) {
    std::shared_ptr<sarray<flexible_type>> out_sarray = graphlab::group(m_lazy_sarray);
    ret->construct_from_sarray(out_sarray);
  } else {
    ret->construct_from_vector(std::vector<flexible_type>(), this->dtype());
  }
  return ret;
}

std::shared_ptr<unity_sarray_base> unity_sarray::append(
    std::shared_ptr<unity_sarray_base> other) {
  std::shared_ptr<unity_sarray> other_unity_sarray = 
      std::static_pointer_cast<unity_sarray>(other);

  if (!m_lazy_sarray || !other_unity_sarray->m_lazy_sarray) {
    log_and_throw("SArray is not initialized");
  }

  if (this->dtype() != other->dtype()) {
    log_and_throw("Both SArrays have to have the same value type");
  }

  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->m_lazy_sarray = m_lazy_sarray->append(other_unity_sarray->m_lazy_sarray);
  return ret_unity_sarray;
}

std::shared_ptr<unity_sarray_base> unity_sarray::vector_slice(size_t start, size_t end) {
  log_func_entry();
  auto this_dtype = dtype();
  if (this_dtype != flex_type_enum::LIST && this_dtype != flex_type_enum::VECTOR) {
    log_and_throw("Cannot slice a non-vector array.");
  }
  if (end <= start) {
    log_and_throw("end of slice must be greater than start of slice.");
  }

  if (m_lazy_sarray) {
    flex_type_enum output_dtype =
        (end == start + 1 && this_dtype == flex_type_enum::VECTOR) ? flex_type_enum::FLOAT : this_dtype;

    auto transform_operator = std::make_shared<le_transform<flexible_type>>(
        this->get_query_tree(),
        [=](const flexible_type& f)->flexible_type {
          if (f.get_type() == flex_type_enum::UNDEFINED) {
            return f;
          } else {
            // if we can slice from the array
            if (end <= f.size()) {
              // yup we have enough room to slice the array.
              flexible_type ret;
              if (output_dtype == flex_type_enum::FLOAT) {
                // length 1
                ret.reset(flex_type_enum::FLOAT);
                ret.soft_assign(f[start]);
              } else {
                // length many
                ret.reset(output_dtype);
                for (size_t i = start; i < end; ++i) {
                  if (this_dtype == flex_type_enum::VECTOR) {
                    ret.push_back(f[i]);
                  } else {
                    ret.push_back(f.array_at(i));
                  }
                }
              }
              return ret;
            } else {
              // no room to slice the array. fail.
              return FLEX_UNDEFINED;
            }
          }
        },
        output_dtype);

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    // this *always* makes the array smaller. So we should probably make this
    // eager
    ret_unity_sarray->construct_from_lazy_operator(transform_operator,
                                                       false,
                                                       output_dtype);

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(),
                               flex_type_enum::FLOAT);
    return ret;
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::filter(const std::string& lambda,
                                                        bool skip_undefined, int seed) {
  log_func_entry();
  if (m_lazy_sarray) {
    // create a filter operator to lazily evaluate this
    auto filter_operator = std::make_shared<le_lambda_filter>(
      this->get_query_tree(),
      lambda,
      skip_undefined,
      seed,
      this->dtype()
    );

    std::shared_ptr<unity_sarray > ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_lazy_operator(filter_operator, false, this->dtype());

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), dtype());
    return ret;
  }
}


std::shared_ptr<unity_sarray_base> 
unity_sarray::logical_filter(std::shared_ptr<unity_sarray_base> index) {
  log_func_entry();

  if(!m_lazy_sarray) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    ret->construct_from_const(0.0, 0);
    return ret;
  }

  ASSERT_TRUE(index != nullptr);

  // empty arrays all around. quick exit
  if (this->size() == 0 && index->size() == 0) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
  // we are both non-empty. check that size matches
  if (this->size() != index->size()) {
    log_and_throw(std::string("Array size mismatch"));
  }

  // create a new le_logical_filter operator to lazily evaluate
  std::shared_ptr<unity_sarray> other_array = std::static_pointer_cast<unity_sarray>(index);

  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> me;
  auto vector_filter_op = std::make_shared<le_logical_filter<flexible_type>>(
    this->get_query_tree(),
    other_array->get_query_tree(),
    this->dtype()
  );

  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(vector_filter_op, false, dtype());

  return ret_unity_sarray;
}


std::shared_ptr<unity_sarray_base> unity_sarray::topk_index(size_t k, bool reverse) {
  log_func_entry();

  auto sarray_ptr = get_underlying_sarray();
  if (sarray_ptr) {
    // check that I have less than comparable of this type
    unity_sarray_binary_operations::
        check_operation_feasibility(dtype(), dtype(), "<");

    struct pqueue_value_type {
      flexible_type val;
      size_t segment_id;
      size_t segment_offset;
      bool operator<(const pqueue_value_type& other) const {
        return val < other.val;
      }
    };
    typedef boost::heap::priority_queue<pqueue_value_type,
            //  darn this is ugly this defines the comparator type
            //  as a generic std::function
            boost::heap::compare<
                std::function<bool(const pqueue_value_type&,
                                   const pqueue_value_type&)> > > pqueue_type;

    // ok done. now we need to merge the values from all of the queues
    pqueue_type::value_compare comparator;
    if (reverse) {
      comparator = std::less<pqueue_value_type>();
    } else {
      comparator =
          [](const pqueue_value_type& a, const pqueue_value_type& b)->bool {
            return !(a < b);
          };
    }

    auto sarray_reader = sarray_ptr->get_reader(thread::cpu_count());
    std::vector<pqueue_type> queues(sarray_reader->num_segments(),
                                    pqueue_type(comparator));

    // parallel insert into num-segments of priority queues
    parallel_for(0, sarray_reader->num_segments(),
                 [&](size_t idx) {
                   auto begin = sarray_reader->begin(idx);
                   auto end = sarray_reader->end(idx);
                   size_t ctr = 0;
                   while(begin != end) {
                     if (!((*begin).is_na())) {
                       queues[idx].push(pqueue_value_type{*begin, idx, ctr});
                       if (queues[idx].size() > k) queues[idx].pop();
                     }
                     ++ctr;
                     ++begin;
                   }
                 });
    pqueue_type master_queue(comparator);

    for(auto& subqueue : queues) {
      for (auto& pqueue_value: subqueue) {
        master_queue.push(pqueue_value);
        if (master_queue.size() > k) master_queue.pop();
      }
    }
    // good. now... split this into the collection of segments as the
    // values to flag as true
    std::vector<std::vector<size_t> > values_to_flag(sarray_reader->num_segments());
    for (auto& pqueue_value: master_queue) {
      values_to_flag[pqueue_value.segment_id].push_back(pqueue_value.segment_offset);
    }
    // sort the subsequences
    for (auto& subvec: values_to_flag) {
      std::sort(subvec.begin(), subvec.end());
    }

    // now we need to write out the segments
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(sarray_reader->num_segments());
    out_sarray->set_type(flex_type_enum::INTEGER);

    parallel_for(0, sarray_reader->num_segments(),
                 [&](size_t idx) {
                   auto output = out_sarray->get_output_iterator(idx);
                   size_t ctr = 0;
                   size_t subvecidx = 0;
                   size_t target_elements = sarray_reader->segment_length(idx);
                   // write some mix of 0 and 1s. outputing 1s
                   // for each time the ctr an entry in values_to_flag[idx]
                   while(ctr < target_elements) {
                     // when we run out of elements in subvec we break
                     if (subvecidx >= values_to_flag[idx].size()) break;
                     if (values_to_flag[idx][subvecidx] == ctr) {
                         (*output) = 1;
                         ++subvecidx;
                      } else {
                         (*output) = 0;
                      }
                      ++output;
                      ++ctr;
                   }
                   // here we have ran out of elements in the sub vector
                   // and we just output all zeros
                   while(ctr < target_elements) {
                     (*output) = 0;
                      ++output;
                      ++ctr;
                   }
                 });

    out_sarray->close();
    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);

    return ret_unity_sarray;
  } else {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}


size_t unity_sarray::num_missing() {
  log_func_entry();

  if (m_lazy_sarray) {
    auto reductionfn = [](const flexible_type& f, size_t& n_missing)->bool {
      if (f.get_type() == flex_type_enum::UNDEFINED) ++n_missing;
      return true;
    };
    auto combinefn = [](const size_t& left, size_t& right)->bool {
      right += left;
      return true;
    };

    size_t ret_val = graphlab::reduce<size_t>(m_lazy_sarray, reductionfn, combinefn, 0);
    return ret_val;
  } else {
    return 0;
  }
}

bool unity_sarray::all() {
  log_func_entry();

  if (m_lazy_sarray) {
    std::function<bool(const flexible_type&, int&)> reductionfn;
    reductionfn = [](const flexible_type& f, int& segment_all)->bool {
      segment_all &= !f.is_zero();
      return true;
    };

    int ret_val = graphlab::reduce<int>(m_lazy_sarray, reductionfn, reductionfn, 1);
    return ret_val > 0;
  } else {
    return true;
  }
}

bool unity_sarray::any() {
  log_func_entry();

  if (m_lazy_sarray) {
    std::function<bool(const flexible_type&, int&)> reductionfn;
    reductionfn = [](const flexible_type& f, int& segment_all)->bool {
      segment_all |= !f.is_zero();
      return !segment_all; // keep continuing if we have not
      // hit a non-empty case.
    };

    int ret_val = graphlab::reduce<int>(m_lazy_sarray, reductionfn, reductionfn, 0);
    return ret_val > 0;
  } else {
    return false;
  }
}

flexible_type unity_sarray::max() {
  log_func_entry();
  if (this->size() == 0) {
    return flex_undefined();
  }

  std::function<bool(const flexible_type&, flexible_type&)> reductionfn;
  flex_type_enum cur_type = dtype();
  if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::DATETIME||
     cur_type == flex_type_enum::FLOAT) {

    flexible_type max_val;
    if(cur_type == flex_type_enum::INTEGER) {
      max_val = std::numeric_limits<flex_int>::lowest();
    } else if(cur_type == flex_type_enum::DATETIME) {
      max_val = flex_date_time(
          flexible_type_impl::my_to_time_t(boost::posix_time::min_date_time), 0);
    } else if(cur_type == flex_type_enum::FLOAT) {
      max_val = std::numeric_limits<flex_float>::lowest();
    }

    reductionfn = [&](const flexible_type& f, flexible_type& maxv)->bool {
                    if (f.get_type() != flex_type_enum::UNDEFINED) {
                      if(f > maxv) maxv = f;
                    }
                    return true;
                  };

    max_val =
      graphlab::reduce<flexible_type>(m_lazy_sarray, reductionfn, reductionfn, max_val);

    return max_val;
  } else {
    log_and_throw("Cannot perform on non-numeric types!");
  }
}

flexible_type unity_sarray::min() {
  log_func_entry();

  if(this->size() > 0) {
    std::function<bool(const flexible_type&, flexible_type&)> reductionfn;
    flex_type_enum cur_type = dtype();
    if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::DATETIME||
       cur_type == flex_type_enum::FLOAT) {

      flexible_type min_val;
      if(cur_type == flex_type_enum::INTEGER) {
        min_val = std::numeric_limits<flex_int>::max();
      } else if(cur_type == flex_type_enum::DATETIME) {
        min_val = flex_date_time(
            flexible_type_impl::my_to_time_t(boost::posix_time::max_date_time), 0);
      } else if(cur_type == flex_type_enum::FLOAT) {
        min_val = std::numeric_limits<flex_float>::max();
      }

      reductionfn = [&](const flexible_type& f, flexible_type& minv)->bool {
                      if (f.get_type() != flex_type_enum::UNDEFINED) {
                        if(f < minv) minv = f;
                      }
                      return true;
                    };

      min_val =
        graphlab::reduce<flexible_type>(m_lazy_sarray, reductionfn, reductionfn, min_val);


      return min_val;
    } else {
      log_and_throw("Cannot perform on non-numeric types!");
    }
  }

  return flex_undefined();
}

flexible_type unity_sarray::sum() {
  log_func_entry();

  if(this->size() > 0) {
    flex_type_enum cur_type = dtype();
    if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::FLOAT) {

      //TODO: Do I need to do this?
      flexible_type start_val;
      if(cur_type == flex_type_enum::INTEGER) {
        start_val = flex_int(0);
      } else {
        start_val = flex_float(0);
      }

      auto reductionfn =
          [](const flexible_type& f, flexible_type& sum)->bool {
            if (f.get_type() != flex_type_enum::UNDEFINED) {
              sum += f;
            }
            return true;
          };

      flexible_type sum_val =
        graphlab::reduce<flexible_type>(m_lazy_sarray, reductionfn, reductionfn, start_val);

      return sum_val;
    } else if (cur_type == flex_type_enum::VECTOR) {

      bool failure = false;
      auto reductionfn =
          [&failure](const flexible_type& f, std::pair<bool, flexible_type>& sum)->bool {
                      if (f.get_type() != flex_type_enum::UNDEFINED) {
                        if (sum.first == false) {
                          // initial val
                          sum.first = true;
                          sum.second = f;
                        } else if (sum.second.size() == f.size()) {
                          // accumulation
                          sum.second += f;
                        } else {
                          // length mismatch. fail
                          failure = true;
                          return false;
                        }
                      }
                      return true;
                    };

      auto combinefn =
          [&failure](const std::pair<bool, flexible_type>& f, std::pair<bool, flexible_type>& sum)->bool {
            if (sum.first == false) {
              // initial state
              sum = f;
            } else if (f.first == false) {
              // there is no f to add.
              return true;
            } else if (sum.second.size() == f.second.size()) {
              // accumulation
              sum.second += f.second;
            } else {
              // length mismatch
              failure = true;
              return false;
            }
            return true;
          };

      std::pair<bool, flexible_type> start_val{false, flex_vec()};
      std::pair<bool, flexible_type> sum_val =
        graphlab::reduce<std::pair<bool, flexible_type> >(m_lazy_sarray, reductionfn, combinefn , start_val);

      // failure indicates there is a missing value, or there is vector length
      // mismatch
      if (failure) {
        log_and_throw("Cannot perform sum over vectors of variable length.");
      }

      return sum_val.second;

    } else {
      log_and_throw("Cannot perform on non-numeric types!");
    }
  }

  return flex_undefined();
}

//TODO: Would be nice to cache this result
flexible_type unity_sarray::mean() {
  log_func_entry();

  if(this->size() > 0) {

    flex_type_enum cur_type = dtype();
    if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::FLOAT ) {

      std::pair<double, size_t> start_val{0.0, 0.0}; // mean, and size
      auto reductionfn =
          [](const flexible_type& f,
                      std::pair<double, size_t>& mean)->bool {
            if (f.get_type() != flex_type_enum::UNDEFINED) {
              // Divide done each time to keep from overflowing
              ++mean.second;
              mean.first += (flex_float(f) - mean.first) / double(mean.second);
            }
            return true;
          };

      // second reduction function to aggregate result
      auto aggregatefn = [](const std::pair<double, size_t>& f,
                            std::pair<double, size_t> &mean)->bool {
        // weighted sum of the two
        if (mean.second + f.second > 0) {
          mean.first =
              mean.first * ((double)mean.second / (double)(mean.second + f.second)) +
              f.first * ((double)f.second / (double)(mean.second + f.second));
          mean.second += f.second;
        }  
        return true;
      };

      std::pair<double, size_t> mean_val =
        graphlab::reduce<std::pair<double, size_t> >(m_lazy_sarray, reductionfn, aggregatefn, start_val);

      return mean_val.first;
    
    
    } else if(cur_type == flex_type_enum::VECTOR) {

      std::pair<flexible_type, size_t> start_val{flexible_type(), 0}; // mean, and size
      auto reductionfn =
          [](const flexible_type& f,
                      std::pair<flexible_type, size_t>& mean)->bool {
              // In the first operation in case of vector, initialzed vector will be size 0
              // so we cannot simply add. Copy instead. 
              if (mean.second == 0){
                ++mean.second;
                mean.first = f; 
              } else {
                if (f.get_type() == flex_type_enum::VECTOR && f.size() != mean.first.size()){
                  log_and_throw("Cannot perform mean on SArray with vectors of different lengths.");
                }
              // Divide done each time to keep from overflowing
                ++mean.second;
                mean.first += (f - mean.first) / double(mean.second);
              }
            return true;
          };

      // second reduction function to aggregate result
      auto aggregatefn = [](const std::pair<flexible_type, size_t>& f,
                            std::pair<flexible_type, size_t> &mean)->bool {
        // weighted sum of the two
        if (mean.second > 0 &&  f.second > 0) {
          if (mean.first.get_type() == flex_type_enum::VECTOR && f.first.size() != mean.first.size()){
                log_and_throw("Cannot perform mean on SArray with vectors of different lengths.");
            }
          mean.first =
              mean.first * ((double)mean.second / (double)(mean.second + f.second)) +
              f.first * ((double)f.second / (double)(mean.second + f.second));
          mean.second += f.second;
        // If count of mean is 0, simply copy the other over since we cannot add
        // vectors of different lengths. 
        } else if (f.second > 0){
          mean.first = f.first;
          mean.second = f.second;
        }  
        return true;
      };

      std::pair<flexible_type, size_t> mean_val =
        graphlab::reduce<std::pair<flexible_type, size_t> >(m_lazy_sarray, reductionfn, aggregatefn, start_val);

      return mean_val.first;
    } else {
      log_and_throw("Cannot perform on types that are not numeric or vector!");
    }
  }

  return flex_undefined();
}

flexible_type unity_sarray::std(size_t ddof) {
  log_func_entry();
  flexible_type variance = this->var(ddof);
  // Return whatever error happens
  if(variance.get_type() == flex_type_enum::UNDEFINED) {
    return variance;
  }

  return std::sqrt((flex_float)variance);
}

flexible_type unity_sarray::var(size_t ddof) {
  log_func_entry();

  if(this->size() > 0) {
    size_t size = this->size();

    flex_type_enum cur_type = dtype();
    if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::FLOAT) {

      if(ddof >= size) {
        log_and_throw("Cannot calculate with degrees of freedom <= 0");
      }

      // formula from
      // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Incremental_Algorithm
      struct incremental_var {
        size_t n = 0;
        double mean = 0;
        double m2 = 0;
      };
      auto reductionfn = [](const flexible_type& f, incremental_var& var)->bool {
                      if (f.get_type() != flex_type_enum::UNDEFINED) {
                        ++var.n;
                        double delta = flex_float(f) - var.mean;
                        var.mean += delta / var.n;
                        var.m2 += delta * (flex_float(f) - var.mean);
                      }
                      return true;
                    };

      auto aggregatefn = [](const incremental_var& f, incremental_var& var)->bool {
        double delta = f.mean - var.mean;
        if (var.n + f.n > 0) {
          var.mean =
              var.mean * ((double)var.n / (double)(var.n + f.n)) +
              f.mean * ((double)f.n / (double)(var.n + f.n));
          var.m2 += f.m2 + delta * var.n * delta * f.n /(double)(var.n + f.n);
          var.n += f.n;
        }
        return true;
      };

      incremental_var var =
        graphlab::reduce<incremental_var>(m_lazy_sarray, reductionfn, aggregatefn, incremental_var());

      // Divide by degrees of freedom and return
      return var.m2 / flex_float(var.n - ddof);
    } else {
      log_and_throw("Cannot perform on non-numeric types!");
    }
  }
  return flex_undefined();
}

std::shared_ptr<unity_sarray_base> unity_sarray::str_to_datetime(std::string format) {
  
  log_func_entry();
  auto sarray_ptr = get_underlying_sarray();
  if(sarray_ptr) {
    flex_type_enum current_type = sarray_ptr->get_type();
    if (current_type != flex_type_enum::STRING)
      log_and_throw("input SArray must be string type.");

    // Get a new swriter
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(flex_type_enum::DATETIME);
   
    const size_t max_n_threads = thread::cpu_count(); 
    std::vector<std::shared_ptr<std::istringstream> > streams(max_n_threads);
    for (size_t index = 0; index < max_n_threads;index++) { 
        std::shared_ptr<std::istringstream> ss(new std::istringstream);
        ss->exceptions(std::ios_base::failbit);
        ss->imbue(std::locale(ss->getloc(),new boost::local_time::local_time_input_facet(format))); 
        streams[index] = ss;
    }
    
    auto transformfn = [format,streams](const flexible_type& f)->flexible_type {
      flexible_type ret;
      if(f.is_na()) {
        ret = FLEX_UNDEFINED;
        return ret;
      }  

      try {
        
        size_t thread_idx = thread::thread_id();
        std::shared_ptr<std::istringstream> ss = streams[thread_idx];
        boost::local_time::local_date_time ldt(boost::posix_time::not_a_date_time); 
        ss->str(f.get<flex_string>());
        (*ss) >> ldt; // do the parse
        
        boost::posix_time::ptime p = ldt.utc_time();
        std::time_t _time = flexible_type_impl::my_to_time_t(p);
        int32_t timezone_offset = 0;
        if(ldt.zone())
          timezone_offset = (int32_t)ldt.zone()->base_utc_offset().total_seconds()/1800;
        ret = flex_date_time(_time,timezone_offset);
      } catch(std::exception& ex) {
        log_and_throw("Unable to interpret " + f.get<flex_string>() + " as string with " + format + " format");
      }
      return ret;
    };
    graphlab::transform(*sarray_ptr, *out_sarray, transformfn);
    // Make a unity_sarray from the sarray we wrote
    out_sarray->close();
    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);

    return ret_unity_sarray;
  } else {
    // empty undefined SArray
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::datetime_to_str(const std::string format) {
  
  
  log_func_entry();
  auto sarray_ptr = get_underlying_sarray();
  if(sarray_ptr) {
    flex_type_enum current_type = sarray_ptr->get_type();
    if (current_type != flex_type_enum::DATETIME)
      log_and_throw("input SArray must be datetime type.");

    // Get a new swriter
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(flex_type_enum::STRING);

    const size_t max_n_threads = thread::cpu_count(); 
    std::vector<std::shared_ptr<std::ostringstream> > streams(max_n_threads);
    for (size_t index = 0; index < max_n_threads;index++) { 
        std::shared_ptr<std::ostringstream> ss(new std::ostringstream);
        ss->exceptions(std::ios_base::failbit);
        ss->imbue(std::locale(ss->getloc(),new boost::local_time::local_time_facet(format.c_str()))); 
        streams[index] = ss;
    }

    auto transformfn = [format,streams](const flexible_type& f)->flexible_type {
      flexible_type ret;

      if(f.is_na()) {
        ret = FLEX_UNDEFINED;
        return ret;
      }

      try {
          size_t thread_idx = thread::thread_id();
          
          const flex_date_time & dt = f.get<flex_date_time>();
          std::string prefix = "0.";
          int sign_adjuster = 1;
          if(dt.second < 0) { 
            sign_adjuster = -1; 
            prefix = "-0.";
          }
   
          std::shared_ptr<std::ostringstream> date_osstr = streams[thread_idx];
          boost::local_time::time_zone_ptr zone(new boost::local_time::posix_time_zone("GMT"+prefix+std::to_string(sign_adjuster * dt.second*30)));
          boost::local_time::local_date_time az(flexible_type_impl::my_from_time_t(dt.first),zone);
          (*date_osstr) << az;
          ret = date_osstr->str();
          date_osstr->str(""); // need to clear stringstream buffer
          //date_osstr->clear(); // need to clear the error flags ... we don't care in this scenario. 
      } catch(std::exception& ex) {
        log_and_throw("Unable to interpret " + f.get<flex_string>() + " as string with " + format + " format");
      }
      return ret;
    };
    graphlab::transform(*sarray_ptr, *out_sarray, transformfn);
    // Make a unity_sarray from the sarray we wrote
    out_sarray->close();
    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);

    return ret_unity_sarray;
  } else {
    // empty undefined SArray
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

}

std::shared_ptr<unity_sarray_base> unity_sarray::astype(flex_type_enum dtype, bool undefined_on_failure) {
  // TODO: make this functino lazy
  log_func_entry();

  // Special path for converting image sarray to vector type.
  // Because we do not want to materialize the image vector.
  if (this->dtype() == flex_type_enum::IMAGE) {
    if (dtype == flex_type_enum::VECTOR) {
      return image_util::image_sarray_to_vector_sarray(std::make_shared<unity_sarray>(*this), undefined_on_failure);
    }
  }
  auto sarray_ptr = get_underlying_sarray();
  if(sarray_ptr) {
    flex_type_enum current_type = sarray_ptr->get_type();
    // if no changes. just keep the identity function
    if (dtype == current_type) return std::static_pointer_cast<unity_sarray>(shared_from_this());

    if(! (flex_type_is_convertible(current_type, dtype) ||
          (current_type == flex_type_enum::STRING && dtype == flex_type_enum::INTEGER) ||
          (current_type == flex_type_enum::STRING && dtype == flex_type_enum::FLOAT) ||
          (current_type == flex_type_enum::STRING && dtype == flex_type_enum::VECTOR) ||
          (current_type == flex_type_enum::STRING && dtype == flex_type_enum::LIST) ||
          (current_type == flex_type_enum::STRING && dtype == flex_type_enum::DICT)
         )) {
      log_and_throw("Not able to cast to given type");
    }

    // Get a new swriter
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(dtype);

    flexible_type_parser parser;

    // The assigment operator takes care of casting
    if (current_type == flex_type_enum::STRING) {
      // we need to treat strings with special care
      // we need to perform a lexical cast
      auto transformfn = [dtype,undefined_on_failure,&parser](const flexible_type& f)->flexible_type {
        if (f.get_type() == flex_type_enum::UNDEFINED) return f;
        flexible_type ret;
        try {
          if (dtype == flex_type_enum::INTEGER) {
            ret = std::stol(f.get<flex_string>());
          } else if (dtype == flex_type_enum::FLOAT) {
            ret = std::stod(f.get<flex_string>());
          } else if (dtype == flex_type_enum::VECTOR) {
            bool success;
            const std::string& val = f.get<flex_string>();
            const char* c = val.c_str();
            std::tie(ret, success) = parser.vector_parse(&c, val.length());
            if (!success) {
              ret = FLEX_UNDEFINED;
              log_and_throw("Cannot convert to array");
            }
          } else if (dtype == flex_type_enum::LIST) {
            bool success;
            const std::string& val = f.get<flex_string>();
            const char* c = val.c_str();
            std::tie(ret, success) = parser.recursive_parse(&c, val.length());
            if (!success) {
              ret = FLEX_UNDEFINED;
              log_and_throw("Cannot convert to list");
            }
          } else if (dtype == flex_type_enum::DICT) {
            bool success;
            const std::string& val = f.get<flex_string>();
            const char* c = val.c_str();
            std::tie(ret, success) = parser.dict_parse(&c, val.length());
            if (!success) {
              ret = FLEX_UNDEFINED;
              log_and_throw("Cannot convert to dict");
            }
          }
        } catch(std::exception& ex) {
          if (undefined_on_failure) ret = FLEX_UNDEFINED;
          else log_and_throw("Unable to interpret " + f.get<flex_string>() + " as the target type.");
        }
        return ret;
      };
      graphlab::transform(*sarray_ptr, *out_sarray, transformfn);
    } else {
      graphlab::copy_if(*sarray_ptr, *out_sarray,
                        [](const flexible_type &f){return true;});
    }

    // Make a unity_sarray from the sarray we wrote
    out_sarray->close();
    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);

    return ret_unity_sarray;
  } else {
    // empty undefined SArray
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::clip(flexible_type lower,
                                                      flexible_type upper) {
  log_func_entry();
  auto sarray_ptr = get_underlying_sarray();
  if(sarray_ptr) {
    flex_type_enum cur_type = dtype();
    if(cur_type == flex_type_enum::INTEGER ||
       cur_type == flex_type_enum::FLOAT ||
       cur_type == flex_type_enum::VECTOR) {
      // Check types of lower and upper for numeric/undefined
      if((lower.get_type() != flex_type_enum::INTEGER &&
          lower.get_type() != flex_type_enum::FLOAT &&
          lower.get_type() != flex_type_enum::UNDEFINED) ||
         (upper.get_type() != flex_type_enum::INTEGER &&
          upper.get_type() != flex_type_enum::FLOAT &&
          upper.get_type() != flex_type_enum::UNDEFINED)) {
        log_and_throw("Must give numeric thresholds!");
      }

      // If undefined, that threshold doesn't exist
      bool clip_lower = !(lower.get_type() == flex_type_enum::UNDEFINED);
      bool clip_upper = !(upper.get_type() == flex_type_enum::UNDEFINED);

      if(clip_lower && clip_upper) {
        if(lower > upper) {
          log_and_throw("Upper clip value must be less than lower value.");
        }
      } else if(!clip_lower && !clip_upper) {
        // No change to the SArray, just return the same one
        return std::static_pointer_cast<unity_sarray>(shared_from_this());
      }

      bool threshold_is_float = (lower.get_type() == flex_type_enum::FLOAT) ||
                                (upper.get_type() == flex_type_enum::FLOAT);
      bool change_made = false;

      flex_type_enum new_type = cur_type;
      if(cur_type == flex_type_enum::INTEGER && threshold_is_float) {
        // If the threshold is float, the result sarray is always float
        new_type = flex_type_enum::FLOAT;
      } else if(cur_type == flex_type_enum::FLOAT && !threshold_is_float) {
        // Threshold must be a float to compare against a list of floats
        if(clip_lower) lower = flex_float(lower);
        if(clip_upper) upper = flex_float(upper);
      }

      auto transformfn = [&](const flexible_type& f)->flexible_type {
        if (f.get_type() == flex_type_enum::UNDEFINED) return f;
        else if (f.get_type() == flex_type_enum::VECTOR) {
          flexible_type newf = f;
          for (size_t i = 0;i < newf.size(); ++i) {
            if(clip_lower && (newf[i] < lower)) {
              change_made = true;
              newf[i] = lower;
            } else if(clip_upper && (newf[i] > upper)) {
              change_made = true;
              newf[i] = upper;
            }
          }
          return newf;
        } else {
          // float or integer
          if(clip_lower && (f < lower)) {
            change_made = true;
            return lower;
          } else if(clip_upper && (f > upper)) {
            change_made = true;
            return upper;
          }
        }
        return f;
      };

      auto out_sarray = std::make_shared<sarray<flexible_type>>();
      out_sarray->open_for_write(thread::cpu_count());
      out_sarray->set_type(new_type);
      graphlab::transform(*sarray_ptr, *out_sarray, transformfn);

      out_sarray->close();

      if(!change_made) {
        // If not change was made, the newly written SArray just goes
        // out of scope and its temp files get deleted.  The original
        // SArray is returned.
        return std::static_pointer_cast<unity_sarray>(shared_from_this());
      }

      std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
      ret_unity_sarray->construct_from_sarray(out_sarray);

      return ret_unity_sarray;
    } else {
      log_and_throw("Cannot perform on non-numeric types");
    }
  } else {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

std::shared_ptr<unity_sarray_base> unity_sarray::nonzero() {
  log_func_entry();

  auto sarray_ptr = get_underlying_sarray();
  if (sarray_ptr) {
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(flex_type_enum::INTEGER);
    auto reader = sarray_ptr->get_reader(thread::cpu_count());
    std::vector<size_t> segment_begin_offset{0};
    size_t current_length = 0;
    for (size_t i = 0; i < reader->num_segments(); ++i) {
      current_length += reader->segment_length(i);
      segment_begin_offset.push_back(current_length);
    }
    parallel_for(0, reader->num_segments(), [&](size_t i) {
      size_t offset = segment_begin_offset[i];
      auto begin = reader->begin(i);
      auto end = reader->end(i);
      auto out = out_sarray->get_output_iterator(i);
      while (begin != end) {
        if (!(*begin).is_zero())
          *out = offset;
        ++begin;
        ++offset;
      }
    });
    out_sarray->close();
    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);
    return ret_unity_sarray;
  } else {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

size_t unity_sarray::nnz() {
  log_func_entry();

  auto sarray_ptr = get_underlying_sarray();
  // the atomic counter
  atomic<size_t> ctr = 0;
  if(this->size() > 0) {
    auto reader = sarray_ptr->get_reader(thread::cpu_count());
    parallel_for(0, reader->num_segments(),
                 [&](size_t i) {
                   auto iter = reader->begin(i);
                   auto iterend = reader->end(i);
                   for(; iter != iterend; ++iter) {
                     // If nonzero, add the index
                     if(!(*iter).is_zero()) ctr.inc();
                   }
                 });
  }
  return ctr.value;
}

std::shared_ptr<unity_sarray_base> unity_sarray::scalar_operator(flexible_type other,
                                                                 std::string op,
                                                                 bool right_operator) {
  log_func_entry();
  flex_type_enum left_type, right_type;

  if (!right_operator) {
    // this is a left operator. we are doing array [op] other
    left_type = dtype();
    right_type = other.get_type();
  } else {
    // this is a right operator. we are doing other [op] array
    left_type = other.get_type();
    right_type = dtype();
  }

  // check for correctness and figure out the output type,
  // and get the operation we need to perform

  unity_sarray_binary_operations::
      check_operation_feasibility(left_type, right_type, op);

  flex_type_enum output_type = unity_sarray_binary_operations::
      get_output_type(left_type, right_type, op);

  auto transformfn = unity_sarray_binary_operations::
      get_binary_operator(left_type, right_type, op);

  // quick exit for empty array
  if (!m_lazy_sarray || size() == 0) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), output_type);
    return ret;
  }

  // create the lazy evalation transform operator from the source
  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
  if (other.get_type() != flex_type_enum::UNDEFINED) {
    auto transform_operator = std::make_shared<le_transform<flexible_type>>(
        m_lazy_sarray->get_query_tree(),
        [=](const flexible_type& f)->flexible_type {
          if (f.get_type() == flex_type_enum::UNDEFINED) {
            return f;
          } else {
            return right_operator ? transformfn(other, f) :transformfn(f, other);
          }
        }, output_type);

    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, output_type);
  } else {
    auto transform_operator = std::make_shared<le_transform<flexible_type>>(
        m_lazy_sarray->get_query_tree(),
        [=](const flexible_type& f)->flexible_type {
          return right_operator ? transformfn(other, f) :transformfn(f, other);
        }, output_type);

    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, output_type);
  }

  return ret_unity_sarray;
}


void unity_sarray::construct_from_unity_sarray(const unity_sarray& other) {
  m_lazy_sarray = other.m_lazy_sarray;
}

std::shared_ptr<unity_sarray_base> unity_sarray::left_scalar_operator(
    flexible_type other, std::string op) {
  log_func_entry();
  return scalar_operator(other, op, false);
}

std::shared_ptr<unity_sarray_base> unity_sarray::right_scalar_operator(
    flexible_type other, std::string op) {
  log_func_entry();
  return scalar_operator(other, op, true);
}

std::shared_ptr<unity_sarray_base> unity_sarray::vector_operator(
    std::shared_ptr<unity_sarray_base> other, std::string op) {
  log_func_entry();
  unity_sarray_binary_operations::check_operation_feasibility(dtype(), other->dtype(), op);

  flex_type_enum output_type =
      unity_sarray_binary_operations::get_output_type(dtype(), other->dtype(), op);
  // empty arrays all around. quick exit
  if (this->size() == 0 && other->size() == 0) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), output_type);
    return ret;
  }
  // array mismatch
  if (this->size() != other->size()) {
    log_and_throw(std::string("Array size mismatch"));
  }

  // we are ready to perform the transform. Build the transform operation
  auto transformfn =
      unity_sarray_binary_operations::get_binary_operator(dtype(), other->dtype(), op);

  bool op_is_not_equality_compare = (op != "==" && op != "!=");
  bool op_is_equality = (op == "==");
  auto transform_fn_with_undefined_checking =
      [=](const flexible_type& f, const flexible_type& g)->flexible_type {
        if (f.get_type() == flex_type_enum::UNDEFINED ||
            g.get_type() == flex_type_enum::UNDEFINED) {
          if (op_is_not_equality_compare) {
            // op is not == or !=
            return FLEX_UNDEFINED;
          } else if (op_is_equality) {
            // op is ==
            return f.get_type() == g.get_type();
          } else {
            // op is !=
            return f.get_type() != g.get_type();
          }
        }
        else return transformfn(f, g);
      };

  // create a new le_vector operator to lazily evaluate
  std::shared_ptr<unity_sarray> other_array = 
      std::static_pointer_cast<unity_sarray>(other);

  auto vector_op = std::make_shared<le_vector>(
    this->get_query_tree(),
    other_array->get_query_tree(),
    transform_fn_with_undefined_checking,
    output_type
    );

  auto le_generator_ptr = std::make_shared<lazy_sarray<flexible_type>>(
    vector_op,
    false,
    output_type);

  std::shared_ptr<unity_sarray >ret_unity_sarray(new unity_sarray());
  ret_unity_sarray->construct_from_lazy_operator(vector_op, false, output_type);

  return ret_unity_sarray;
}

std::shared_ptr<unity_sarray_base> unity_sarray::drop_missing_values() {
  log_func_entry();

  auto sarray_ptr = get_underlying_sarray();

  if(sarray_ptr) {
    auto filterfn = [&](const flexible_type& f)->flexible_type {
      return !f.is_na();
    };

    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(sarray_ptr->get_type());
    graphlab::copy_if(*sarray_ptr, *out_sarray, filterfn);

    out_sarray->close();

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_sarray(out_sarray);
    return ret_unity_sarray;
  } else {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::fill_missing_values(flexible_type default_value) {
  log_func_entry();

  std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());

  if(m_lazy_sarray) {
    if(!flex_type_is_convertible(default_value.get_type(), this->dtype())) {
      log_and_throw("Default value must be convertible to column type");
    }

    auto transform_fn = [default_value](const flexible_type &f)->flexible_type {
      if(f.is_na()) {
        return default_value;
      }
      return f;
    };

    auto fill_values_op =
      std::make_shared<le_transform<flexible_type>>(
          m_lazy_sarray->get_query_tree(),
          transform_fn,
          this->dtype());

    ret_unity_sarray->construct_from_lazy_operator(fill_values_op, false, this->dtype());
  }

  return ret_unity_sarray;
}

std::shared_ptr<unity_sarray_base> unity_sarray::tail(size_t nrows) {
  log_func_entry();
  size_t maxrows = std::min<size_t>(size(), nrows);
  size_t end = size();
  size_t start = end - maxrows;
  return copy_range(start, 1, end);
}

std::shared_ptr<unity_sarray_base> unity_sarray::sample(float percent, int random_seed) {
  log_func_entry();
  auto sarray_ptr = get_underlying_sarray();
  if(sarray_ptr) {
    auto out_sarray = std::make_shared<sarray<flexible_type>>();
    out_sarray->open_for_write(thread::cpu_count());
    out_sarray->set_type(sarray_ptr->get_type());
    copy_if(*sarray_ptr, *out_sarray,
            [percent](const flexible_type& x) {return random::rand01() <= percent;},
            std::set<size_t>(),
            random_seed);
    out_sarray->close();
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    ret->construct_from_sarray(out_sarray);
    return ret;
  } else {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::count_bag_of_words(std::map<std::string, flexible_type> options) {
  log_func_entry();
  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::STRING) {
    log_and_throw("Only string type is supported for word counting.");
  }

  bool to_lower = true;
  if (options.find("to_lower") != options.end()) {
    to_lower = options["to_lower"];
  }

  auto transformfn = [to_lower](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    flex_dict ret;
    const std::string& str = f.get<flex_string>();

    // Tokenizing the string by space, and add to mape
    // Here we optimize for speed to reduce the string malloc
    size_t word_begin = 0;
    // skip leading spaces
    while (word_begin < str.size() && (std::ispunct(str[word_begin]) || std::isspace(str[word_begin])))
      ++word_begin;

    std::string word;
    flexible_type word_flex;

    // count bag of words
    std::unordered_map<flexible_type, size_t> ret_count;

    for (size_t i = word_begin; i < str.size(); ++i) {
      if (std::ispunct(str[i]) || std::isspace(str[i])) {
        // find the end of thw word, make a substring, and transform to lower case
        word = std::string(str, word_begin, i - word_begin);
        if  (to_lower)
          std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        word_flex = std::move(word);

        // add the word to map
        ret_count[word_flex]++;

        // keep skipping space, and reset word_begin
        while (i < str.size() && (std::ispunct(str[i]) || std::isspace(str[i])))
          ++i;
        word_begin = i;
      }
    }

    // add the last word
    if (word_begin < str.size()) {
      word = std::string(str, word_begin, str.size() - word_begin);
      if  (to_lower)
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
      word_flex = std::move(word);
      ret_count[word_flex]++;
    }

    // convert to dictionary
    for(auto& val : ret_count) {
      ret.push_back({val.first, flexible_type(val.second)});
    }
    return ret;
  };

  return transform_to_sarray(transformfn, flex_type_enum::DICT);
}


std::shared_ptr<unity_sarray_base> 
unity_sarray::count_ngrams(size_t n, std::map<std::string, flexible_type> options) {

  log_func_entry();
  if (!m_lazy_sarray) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    return ret;
  }

  if (this->dtype() != flex_type_enum::STRING) {
    log_and_throw("Only string type is supported for n-gram counting.");
  }

  bool to_lower = true;
  if (options.find("to_lower") != options.end()) {
    to_lower = options["to_lower"];
  }


  auto transformfn = [to_lower, n](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    //Maps that hold id's, n-grams, and counts. 


    typedef std::pair<std::deque<size_t>, std::deque<size_t> > deque_pair;

    std::unordered_map<hash_value, deque_pair > ngram_id_map;
    std::unordered_map<hash_value, size_t > id_count_map;

    std::string lower;

    //Do a string copy if need to convert to lowercase 

    if (to_lower){
      lower = boost::algorithm::to_lower_copy(f.get<flex_string>());
    }

    const std::string& str =  (to_lower) ? lower : f.get<flex_string>();



    // Tokenizing the string by space, and add to map
    // Here we optimize for speed to reduce the string malloc
    size_t word_begin = 0;
    size_t word_end = 0; 

    flex_dict ret;
    std::deque<size_t> begin_deque;
    std::deque<size_t> end_deque;
    hash_value ngram_id; 
    bool end_of_doc = false;

    while (1){

      // Getting the next word until we have n of them 
      while (begin_deque.size() < n){
      // skip leading spaces 
        while (word_begin < str.size() 
          && (std::ispunct(str[word_begin]) 
            || std::isspace(str[word_begin])))
          ++word_begin;
      //If you reach the end, break out of loops.
        if (word_begin >= str.size()) {
          end_of_doc = true;
          break;
        }
      // Find end of word
        word_end = word_begin;  
        while (!std::ispunct(str[word_end]) 
          && !std::isspace(str[word_end]) 
          && word_end < str.size())
          ++word_end;
      //Add to n-gram deque
        begin_deque.push_back(word_begin);
        end_deque.push_back(word_end);
        word_begin = word_end + 1;
      }

    // If not end of doc, add n-grams to maps 
      if (end_of_doc){
        break;
      }else{
    //Combines hashes of all the words in n-gram in order dependent way, 
    //producing a new hash. 

        ngram_id = 0;

        assert(begin_deque.size() == n);
        assert(end_deque.size() == n);

        for(size_t i = 0; i < n; ++i){
          size_t word_length = end_deque[i]- begin_deque[i];
          uint128_t ngram_hash = hash128(&str[begin_deque[i]],word_length);
          ngram_id = hash128_combine(ngram_id.hash(), ngram_hash);
        }

    // Add deques to a map. These point to an instance of the n-gram in the 
    // document string, to avoid unnecessary copies. 
        if (ngram_id_map.count(ngram_id) == 0){
          ngram_id_map[ngram_id] = std::make_pair(begin_deque,end_deque);
        } 

        id_count_map[ngram_id]++;


    // Slide along 1 word.
        begin_deque.pop_front();
        end_deque.pop_front();

      }
    }



  //Convert to dictionary

    std::string to_copy;

    for(auto& val : id_count_map) {
      size_t word_length;
      std::deque<size_t> ngram_begin_deque = ngram_id_map[val.first].first;
      std::deque<size_t> ngram_end_deque = ngram_id_map[val.first].second; 
      to_copy.resize (0 , ' ');

      for (size_t i = 0; i <  n - 1; ++i){
        word_length = ngram_end_deque[i]- ngram_begin_deque[i];
        to_copy.append(&str[ngram_begin_deque[i]], word_length);
        to_copy.append( " ", 1);
      }

      word_length = ngram_end_deque[n -1]- ngram_begin_deque[n-1];
      to_copy.append(&str[ngram_begin_deque[n-1]], word_length);

      ret.push_back({move(to_copy), flexible_type(val.second)});
    }
    return ret;
  };

  return transform_to_sarray(transformfn, flex_type_enum::DICT);
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::count_character_ngrams(size_t n, std::map<std::string, flexible_type> options) {
  log_func_entry();
  if (!m_lazy_sarray) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    return ret;
  }

  if (this->dtype() != flex_type_enum::STRING) {
    log_and_throw("Only string type is supported for word counting.");
  }

  bool to_lower = true;
  bool ignore_space = true;
  if (options.find("to_lower") != options.end()) {
    to_lower = options["to_lower"];
  }

  if (options.find("ignore_space") != options.end()) {
    ignore_space = options["ignore_space"];
  }


  auto transformfn = [to_lower, ignore_space, n](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    //Maps that hold id's, n-grams, and counts. 

    typedef std::pair<std::deque<size_t>, size_t > deque_count_pair;

    std::unordered_map<hash_value, deque_count_pair > ngram_id_map;
    

    std::string lower;

    //Do a string copy if need to convert to lowercase 

    if (to_lower){
      lower = boost::algorithm::to_lower_copy(f.get<flex_string>());
    }

    const std::string& str =  (to_lower) ? lower : f.get<flex_string>();



    // Tokenizing the string by space, and add to map
    // Here we optimize for speed to reduce the string malloc
    size_t character_location = 0; 

    flex_dict ret;
    std::deque<size_t> character_deque;
    hash_value ngram_id; 
    bool end_of_doc = false;

    while (1){

      // Getting the next word until we have n of them 
      while (character_deque.size() < n){
      // skip leading spaces 
        while (character_location < str.size() 
          && (std::ispunct(str[character_location]) 
          || (std::isspace(str[character_location]) && ignore_space)))
          ++character_location;
      //If you reach the end, break out of loops.
        if (character_location >= str.size()) {
          end_of_doc = true;
          break;
        }
      //Add to n-gram deque
        character_deque.push_back(character_location);
        ++character_location;
      }

    // If not end of doc, add n-grams to maps 
      if (end_of_doc){
        break;
      }else{
    //Combines hashes of all the words in n-gram in order dependent way, 
    //producing a new hash. 

        ngram_id = 0;

        assert(character_deque.size() == n);

        for(size_t i = 0; i < n; ++i){
          uint128_t ngram_hash = hash128(&str[character_deque[i]],1);
          ngram_id = hash128_combine(ngram_id.hash(),ngram_hash);
        }

    // Add deques to a map. These point to an instance of the n-gram 
    //in the document string, to avoid unnecessary copies. 
        if (ngram_id_map.count(ngram_id) == 0){
          ngram_id_map[ngram_id] = std::make_pair(character_deque,1);
        } else {
          ngram_id_map[ngram_id].second++;
        }

    // Slide along 1 character.
        character_deque.pop_front();

      }
    }

  //Convert to dictionary

    std::string to_copy;

    for(auto& val : ngram_id_map) {
      std::deque<size_t> ngram_character_deque = val.second.first;
      to_copy.resize (0 , ' ');

      for (size_t i = 0; i <  n; ++i){
        to_copy.append(&str[ngram_character_deque[i]], 1);
      }

      ret.push_back({move(to_copy), flexible_type(val.second.second)});
    }
    return ret;
  };

  return transform_to_sarray(transformfn, flex_type_enum::DICT);
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::dict_trim_by_keys(const std::vector<flexible_type>& keys, bool exclude) {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

  std::set<flexible_type> keyset(keys.begin(), keys.end());

  auto transformfn = [exclude, keyset](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    flex_dict ret;
    const flex_dict& input = f.get<flex_dict>();
    for(auto& val : input) {
      bool is_in_key = val.first.get_type() == flex_type_enum::UNDEFINED ? false : keyset.count(val.first);
      if (exclude != is_in_key) {
        ret.push_back(std::make_pair<flexible_type, flexible_type>(
          flexible_type(val.first), flexible_type(val.second)));
      }
    }

    return ret;
  };

  return transform_to_sarray(transformfn, flex_type_enum::DICT);
}

std::shared_ptr<unity_sarray_base>
unity_sarray::dict_trim_by_values(const flexible_type& lower, const flexible_type& upper) {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

  bool has_lower_bound = lower.get_type() != flex_type_enum::UNDEFINED;
  bool has_upper_bound = upper.get_type() != flex_type_enum::UNDEFINED;

  // invalid parameter
  if (has_upper_bound && has_lower_bound && lower > upper) {
    log_and_throw("Low bound must be higher than upper bound.");
  }

  // nothing to trim
  if (!has_upper_bound && !has_lower_bound) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  auto transformfn = [&](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    flex_dict ret;
    const flex_dict& input = f.get<flex_dict>();
    for(auto& val : input) {
      bool lower_bound_match =
        !has_lower_bound ||
        !flex_type_has_binary_op(val.second.get_type(), lower.get_type(), '<') ||
        val.second >= lower;

      bool upper_bound_match =
        !has_upper_bound ||
        !flex_type_has_binary_op(val.second.get_type(), upper.get_type(), '<') ||
        val.second <= upper;

      if (lower_bound_match && upper_bound_match) {
        ret.push_back(std::make_pair<flexible_type, flexible_type>(
          flexible_type(val.first), flexible_type(val.second)));
      }
    }

    return ret;
  };

  return transform_to_sarray(transformfn, flex_type_enum::DICT);
}

std::shared_ptr<unity_sarray_base> unity_sarray::dict_keys() {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

  auto transformfn = [](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;
    return flex_dict_view(f).keys();
  };

  return transform_to_sarray(transformfn, flex_type_enum::LIST);
}

std::shared_ptr<unity_sarray_base> unity_sarray::dict_values() {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

 auto transformfn = [](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;
    return flex_dict_view(f).values();
  };

  return transform_to_sarray(transformfn, flex_type_enum::LIST);
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::dict_has_any_keys(const std::vector<flexible_type>& keys) {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

  std::set<flexible_type> keyset(keys.begin(), keys.end());

  auto transformfn = [keyset](const flexible_type& f)->int {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    for(auto& val : f.get<flex_dict>()) {
      bool is_in_key = val.first.get_type() == flex_type_enum::UNDEFINED ? false : keyset.count(val.first);
      if (is_in_key) return 1;
    }

    return 0;
  };

  return transform_to_sarray(transformfn, flex_type_enum::INTEGER);
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::dict_has_all_keys(const std::vector<flexible_type>& keys) {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  if (this->dtype() != flex_type_enum::DICT) {
    log_and_throw("Only dictionary type is supported for trim by keys.");
  }

  auto transformfn = [keys](const flexible_type& f)->int {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    // make sure each key exists in the dictionary
    const flex_dict_view& v(f);
    for(auto& key: keys) {
      if (!v.has_key(key)) return 0;
    }

    return 1;
  };

  return transform_to_sarray(transformfn, flex_type_enum::INTEGER);
}


std::shared_ptr<unity_sarray_base> unity_sarray::item_length() {
  log_func_entry();

  if (!m_lazy_sarray) {
    return std::static_pointer_cast<unity_sarray>(shared_from_this());
  }

  std::set<flex_type_enum> supported_types(
    {flex_type_enum::DICT, flex_type_enum::VECTOR, flex_type_enum::LIST});

  if (!supported_types.count(this->dtype())) {
    log_and_throw("item_length() is only applicable for SArray of type list, dict and array.");
  }

  auto transformfn = [](const flexible_type& f)->flexible_type {
    if (f.get_type() == flex_type_enum::UNDEFINED) return f;

    return f.size();
  };

  return transform_to_sarray(transformfn, flex_type_enum::INTEGER);
}

std::shared_ptr<unity_sframe_base >unity_sarray::unpack_dict(
  const std::string& column_name_prefix,
  const std::vector<flexible_type>& limit,
  const flexible_type& na_value) {

  log_func_entry();

  if (this->dtype() != flex_type_enum::DICT) {
    throw "unpack_dict is only applicable to SArray of dictionary type.";
  }

  bool has_key_limits = limit.size() > 0;

  std::map<flexible_type, flex_type_enum> key_valuetype_map;

  // prefill the key_value_type map if limit is given
  if (has_key_limits) {
    for(auto v : limit) {
      key_valuetype_map[v] = flex_type_enum::UNDEFINED;
    }
  }

  // extract dictionary keys and value types from all rows
  auto reductionfn = [has_key_limits](const flexible_type& f, std::map<flexible_type, flex_type_enum>& key_valuetype_map)->bool {
    if (f != FLEX_UNDEFINED) {
      const flex_dict& input = f.get<flex_dict>();
      for(auto& v : input) {
        flex_type_enum type = v.second.get_type();
        auto position = key_valuetype_map.find(v.first);
        if (position == key_valuetype_map.end()) {

          // skip the key if limited
          if (has_key_limits) continue;

          key_valuetype_map[v.first] = type;
        } else if (position->second == flex_type_enum::UNDEFINED) {
          position->second = type;
        } else if (position->second != type && type != flex_type_enum::UNDEFINED) {
          // use string if types don't agree
          position->second = flex_type_enum::STRING;
        }
      }
    }
    return true;
  };

  auto combinefn = [](std::map<flexible_type, flex_type_enum>& mapping, std::map<flexible_type, flex_type_enum>& aggregate)->bool {
    for(auto& val : mapping) {
      auto position = aggregate.find(val.first);
      if (position == aggregate.end()) {
        aggregate[val.first] = val.second;
      } else if (position->second == flex_type_enum::UNDEFINED) {
        position->second = val.second;
      } else if (val.second != flex_type_enum::UNDEFINED && position->second != val.second) {
          // use string if types don't agree
          position->second = flex_type_enum::STRING;
      }
    }
    return true;
  };

  key_valuetype_map =
    graphlab::reduce<std::map<flexible_type, flex_type_enum>>(m_lazy_sarray, reductionfn, combinefn, key_valuetype_map);

  if (key_valuetype_map.size() == 0) {
    throw "Nothing to unpack, SArray is empty";
  }

  // generate column types
  std::vector<flex_type_enum> column_types(key_valuetype_map.size());
  std::vector<flexible_type> unpacked_keys(key_valuetype_map.size());
  size_t i = 0;
  for(auto val : key_valuetype_map) {
    unpacked_keys[i] = val.first;
    column_types[i] = val.second == flex_type_enum::UNDEFINED ? flex_type_enum::FLOAT : val.second;
    i++;
  }

  return unpack(column_name_prefix, unpacked_keys, column_types, na_value);
}

std::shared_ptr<unity_sframe_base> unity_sarray::expand(
  const std::string& column_name_prefix,
  const std::vector<flexible_type>& expanded_column_elements,
  const std::vector<flex_type_enum>& expanded_columns_types) {

  log_func_entry();

  if (!m_lazy_sarray) {
    throw ("SFrame is not initialized yet");
  }

  auto mytype = this->dtype();
  if (mytype != flex_type_enum::DATETIME) {
    throw("Cannot expand an SArray of type that is not datetime type");
  }

  if (expanded_column_elements.size() != expanded_columns_types.size()) {
    throw "Expanded column names and types length do not match";
  }

  if (expanded_column_elements.size() == 0) {
    throw "Please provide at least one column to expand datetime to";
  }

  // generate column names 
  std::vector<std::string> column_names;
  column_names.reserve(expanded_column_elements.size());
  for (auto& key : expanded_column_elements) {
    if (column_name_prefix.empty()) {
      column_names.push_back((flex_string)key);
    } else {
      column_names.push_back(column_name_prefix + "." + (flex_string)key);
    }
  }

  std::shared_ptr<unity_sframe> ret (new unity_sframe());
  sframe sf;
  sf.open_for_write(column_names, expanded_columns_types, "", SFRAME_DEFAULT_NUM_SEGMENTS);
  graphlab::expand(m_lazy_sarray,expanded_column_elements, sf);
  sf.close();
  ret->construct_from_sframe(sf);
  return ret;
}

std::shared_ptr<unity_sframe_base> unity_sarray::unpack(
  const std::string& column_name_prefix,
  const std::vector<flexible_type>& unpacked_keys,
  const std::vector<flex_type_enum>& column_types,
  const flexible_type& na_value) {

  log_func_entry();

  if (!m_lazy_sarray) {
    throw ("SFrame is not initialized yet");
  }

  auto mytype = this->dtype();
  if (mytype != flex_type_enum::DICT &&
    mytype != flex_type_enum::LIST &&
    mytype != flex_type_enum::VECTOR) {
    throw("Cannot unpack an SArray of type that is not list/array/dict type");
  }

  if (unpacked_keys.size() != column_types.size()) {
    throw "unpacked column names and types length do not match";
  }

  if (unpacked_keys.size() == 0) {
    throw "Please provide at least one column to unpack to";
  }

  // generate column names
  std::vector<std::string> column_names;
  column_names.reserve(unpacked_keys.size());
  for (auto& key : unpacked_keys) {
    if (column_name_prefix.empty()) {
      column_names.push_back((flex_string)key);
    } else {
      column_names.push_back(column_name_prefix + "." + (flex_string)key);
    }
  }

  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  sframe sf;
  sf.open_for_write(column_names, column_types, "", SFRAME_DEFAULT_NUM_SEGMENTS);
  graphlab::unpack(m_lazy_sarray, unpacked_keys, sf, na_value);
  sf.close();
  ret->construct_from_sframe(sf);
  return ret;
}

template<typename Transformfn>
std::shared_ptr<unity_sarray_base> 
unity_sarray::transform_to_sarray(Transformfn transformfn, flex_type_enum return_type) {
  // calculate in parallel
  auto out_sarray = std::make_shared<sarray<flexible_type>>();
  out_sarray->open_for_write();
  out_sarray->set_type(return_type);

  graphlab::transform(m_lazy_sarray, *out_sarray, transformfn);

  out_sarray->close();

  std::shared_ptr<unity_sarray> ret(new unity_sarray());
  ret->construct_from_sarray(out_sarray);
  return ret;
}

void unity_sarray::begin_iterator() {
  log_func_entry();
  auto sarray_ptr = get_underlying_sarray();

  // nothing to iterate over. quit
  if (!sarray_ptr || size() == 0) return;
  iterator_sarray_ptr = sarray_ptr->get_reader();
  // init the iterators
  iterator_current_segment_iter.reset(new sarray_iterator<flexible_type>(iterator_sarray_ptr->begin(0)));
  iterator_current_segment_enditer.reset(new sarray_iterator<flexible_type>(iterator_sarray_ptr->end(0)));
  iterator_next_segment_id = 1;
}


std::vector<flexible_type> unity_sarray::iterator_get_next(size_t len) {
  log_func_entry();
  std::vector<flexible_type> ret;
  // nothing to iterate over. quit
  if (!iterator_sarray_ptr || size() == 0) return ret;
  // try to extract len elements
  ret.reserve(len);
  // loop across segments
  while(1) {
    // loop through current segment
    while(*iterator_current_segment_iter != *iterator_current_segment_enditer) {
      ret.push_back(**iterator_current_segment_iter);
      ++(*iterator_current_segment_iter);
      if (ret.size() >= len) break;
    }
    if (ret.size() >= len) break;
    // if we run out of data in the current segment, advance to the next segment
    // if we run out of segments, quit.
    if (iterator_next_segment_id >= iterator_sarray_ptr->num_segments()) break;
    iterator_current_segment_iter.reset(new sarray_iterator<flexible_type>(
        iterator_sarray_ptr->begin(iterator_next_segment_id)));
    iterator_current_segment_enditer.reset(new sarray_iterator<flexible_type>(
        iterator_sarray_ptr->end(iterator_next_segment_id)));
    ++iterator_next_segment_id;
  }

  return ret;
}


void unity_sarray::materialize() {
  if (m_lazy_sarray) {
    m_lazy_sarray->materialize();
  }
}

bool unity_sarray::is_materialized() {
  return this->m_lazy_sarray && this->m_lazy_sarray->is_materialized();
}

std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> unity_sarray::get_query_tree() {
  if (m_lazy_sarray) {
    return m_lazy_sarray->get_query_tree();
  } else {
    return std::shared_ptr<lazy_eval_op_imp_base<flexible_type>>();
  }
}

size_t unity_sarray::get_content_identifier() {
  if (is_materialized()) {
    index_file_information index_info = m_lazy_sarray->get_sarray_ptr()->get_index_info();
    // compute a hash which uniquely identifies the sarray.
    // This can be done by computing a hash of all the segment file names
    // and the segment sizes. (really, just the segment file names are
    // probably sufficient), but I have a bit of paranoia since it is technically
    // possible to interpret a longer sarray as a shorter one by changing the
    // perceived segment size.
    size_t hash_val = graphlab::hash64(index_info.segment_files);
    for (auto& segment_size: index_info.segment_sizes) {
      hash_val = graphlab::hash64_combine(hash_val, graphlab::hash64(segment_size));
    }
    return hash_val;
  } else {
    return random::rand();
  }
}

std::shared_ptr<unity_sarray_base> 
unity_sarray::copy_range(size_t start, size_t step, size_t end) {
  log_func_entry();
  if (step == 0) log_and_throw("Range step size must be at least 1");
  // end cannot be past the end
  end = std::min(end, size());
  if (end <= start) {
    // return an empty array of the appropriate type
    std::shared_ptr<unity_sarray> ret(new unity_sarray);
    ret->construct_from_vector(std::vector<flexible_type>(), dtype());
    return ret;
  }

  // construct output
  auto out_sarray = std::make_shared<sarray<flexible_type>>();
  out_sarray->open_for_write();
  out_sarray->set_type(dtype());

  // copy me into out
  auto sarray_ptr = get_underlying_sarray();
  graphlab::copy_range(*sarray_ptr, *out_sarray, start, step, end);
  out_sarray->close();

  std::shared_ptr<unity_sarray> ret(new unity_sarray);
  ret->construct_from_sarray(out_sarray);
  return ret;
}

std::shared_ptr<unity_sarray_base> unity_sarray::create_sequential_sarray(ssize_t size, ssize_t start, bool reverse) {
    if(size < 0) {
      log_and_throw("Must give size as >= 0");
    }

    auto row_num_sarray = std::make_shared<sarray<flexible_type>>();
    row_num_sarray->open_for_write(1);
    row_num_sarray->set_type(flex_type_enum::INTEGER);

    auto out_iter = row_num_sarray->get_output_iterator(0);
    for(ssize_t i = 0; i < size; ++i) {
      if(reverse) {
        *out_iter = start-i;
      } else {
        *out_iter = start+i;
      }
      ++out_iter;
    }

    row_num_sarray->close();

    std::shared_ptr<unity_sarray> row_num_column(new unity_sarray());
    row_num_column->construct_from_sarray(row_num_sarray);

    return row_num_column;
}

} // namespace graphlab
