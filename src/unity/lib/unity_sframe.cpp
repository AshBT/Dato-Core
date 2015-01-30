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
#include <set>
#include <boost/algorithm/string.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sarray.hpp>
#include <sframe/algorithm.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/sanitize_url.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/query_process/lazy_sframe.hpp>
#include <unity/query_process/algorithm_parallel_iter.hpp>
#include <unity/query_process/lazy_groupby_aggregate.hpp>
#include <unity/query_process/lazy_eval_op_imp.hpp>
#include <sframe/groupby_aggregate.hpp>
#include <sframe/groupby_aggregate_operators.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/csv_writer.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>
#include <sframe/join.hpp>
#include <unity/lib/auto_close_sarray.hpp>
#include <unity/query_process/sort.hpp>

namespace graphlab {

unity_sframe::unity_sframe() { }

unity_sframe::~unity_sframe() {
  clear();
}

void unity_sframe::construct_from_dataframe(const dataframe_t& df) {
  log_func_entry();
  clear();
  auto sframe_ptr = std::make_shared<sframe>(df);
  m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);
}

void unity_sframe::construct_from_sframe(const sframe& sf) {
  log_func_entry();
  clear();
  this->set_sframe(std::make_shared<sframe>(sf));
}

void unity_sframe::construct_from_sframe_index(std::string location) {
  logstream(LOG_INFO) << "Construct sframe from location: " << sanitize_url(location) << std::endl;
  clear();

  auto status = fileio::get_file_status(location);
  if (fileio::is_web_protocol(location)) {
    // if it is a web protocol, we cannot be certain what type of file it is.
    // HEURISTIC:
    //   assume it is a "directory" and try to load dir_archive.ini
    //   if we can open it, it is a regular file. Otherwise not.
    if (fileio::try_to_open_file(location + "/dir_archive.ini")) {
      status = fileio::file_status::DIRECTORY;
    } else {
      status = fileio::file_status::REGULAR_FILE;
    }
  }

  if (status == fileio::file_status::MISSING) {
    // missing file. fail quick
    log_and_throw_io_failure(sanitize_url(location) + " not found.");
  } if (status == fileio::file_status::REGULAR_FILE) {
    // its a regular file, load it normally
    auto sframe_ptr = std::make_shared<sframe>(location);
    m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);
  } else if (status == fileio::file_status::DIRECTORY) {
    // its a directory, open the directory and verify that it contains an
    // sarray and then load it if it does
    dir_archive dirarc;
    dirarc.open_directory_for_read(location);
    std::string content_value;
    if (dirarc.get_metadata("contents", content_value) == false ||
        content_value != "sframe") {
      log_and_throw_io_failure("Archive does not contain an SFrame");
    }
    std::string prefix = dirarc.get_next_read_prefix();
    auto sframe_ptr = std::make_shared<sframe>(prefix + ".frame_idx");
    m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);
    dirarc.close();
  }
}

std::map<std::string, std::shared_ptr<unity_sarray_base>> unity_sframe::construct_from_csvs(
    std::string url,
    std::map<std::string, flexible_type> csv_parsing_config,
    std::map<std::string, flex_type_enum> column_type_hints) {
  logstream(LOG_INFO) << "Construct sframe from csvs at "
                      << sanitize_url(url) << std::endl;
  std::stringstream ss;
  ss << "Parsing config:\n";
  for (auto& pair: csv_parsing_config) {
    ss << "\t" << pair.first << ": " << pair.second << "\n";
  }
  logstream(LOG_INFO) << ss.str();

  clear();
  csv_line_tokenizer tokenizer;
  // first the defaults
  bool use_header = true;
  bool continue_on_failure = false;
  bool store_errors = false;
  size_t row_limit = 0;
  tokenizer.delimiter = ",";
  tokenizer.comment_char = '\0';
  tokenizer.escape_char = '\\';
  tokenizer.double_quote = true;
  tokenizer.quote_char = '\"';
  tokenizer.skip_initial_space = true;
  tokenizer.na_values.clear();

  if (csv_parsing_config.count("use_header")) {
    use_header = !csv_parsing_config["use_header"].is_zero();
  }
  if (csv_parsing_config.count("continue_on_failure")) {
    continue_on_failure = !csv_parsing_config["continue_on_failure"].is_zero();
  }
  if (csv_parsing_config.count("store_errors")) {
    store_errors = !csv_parsing_config["store_errors"].is_zero();
  }
  if (csv_parsing_config.count("row_limit")) {
    row_limit = (flex_int)(csv_parsing_config["row_limit"]);
  }
  if (csv_parsing_config["delimiter"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)csv_parsing_config["delimiter"];
    if(tmp.length() > 0) tokenizer.delimiter = tmp;
  }
  if (csv_parsing_config["comment_char"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)csv_parsing_config["comment_char"];
    if (tmp.length() > 0) tokenizer.comment_char= tmp[0];
  }
  if (csv_parsing_config["escape_char"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)csv_parsing_config["escape_char"];
    if (tmp.length() > 0) tokenizer.escape_char = tmp[0];
  }
  if (csv_parsing_config.count("double_quote")) {
    tokenizer.double_quote = !csv_parsing_config["double_quote"].is_zero();
  }
  if (csv_parsing_config["quote_char"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)csv_parsing_config["quote_char"];
    if (tmp.length() > 0) tokenizer.quote_char = tmp[0];
  }
  if (csv_parsing_config.count("skip_initial_space")) {
    tokenizer.skip_initial_space = !csv_parsing_config["skip_initial_space"].is_zero();
  }
  if (csv_parsing_config["na_values"].get_type() == flex_type_enum::LIST) {
    flex_list rec = csv_parsing_config["na_values"];
    tokenizer.na_values.clear();
    for (size_t i = 0;i < rec.size(); ++i) {
      if (rec[i].get_type() == flex_type_enum::STRING) {
        tokenizer.na_values.push_back((std::string)rec[i]);
      }
    }
  }

  tokenizer.init();

  auto sframe_ptr = std::make_shared<sframe>();

  auto errors = sframe_ptr->init_from_csvs(url, tokenizer, use_header, continue_on_failure,
                                           store_errors, column_type_hints, row_limit);

  m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);

  std::map<std::string, std::shared_ptr<unity_sarray_base>> errors_unity;
  for (auto& kv : errors) {
    std::shared_ptr<unity_sarray> sa(new unity_sarray());
    sa->construct_from_sarray(kv.second);
    errors_unity.insert(std::make_pair(kv.first, sa));
  }

  return errors_unity;
}


void unity_sframe::construct_from_lazy_sframe(std::shared_ptr<lazy_sframe> lazy_sframe_ptr) {
  clear();
  m_lazy_sframe = lazy_sframe_ptr;
}

void unity_sframe::save_frame(std::string target_directory) {
  try {
    dir_archive dirarc;
    dirarc.open_directory_for_write(target_directory);
    dirarc.set_metadata("contents", "sframe");
    std::string prefix = dirarc.get_next_write_prefix();
    save_frame_by_index_file(prefix + ".frame_idx");
    dirarc.close();
  } catch(...) {
    throw;
  }
}


void unity_sframe::save_frame_by_index_file(std::string index_file) {
  if (!m_lazy_sframe) {
    // save an empty Sframe
    auto sframe_ptr = std::make_shared<sframe>();
    sframe_ptr->open_for_write(std::vector<std::string>(),
                               std::vector<flex_type_enum>(),
                               index_file,
                               1 /* 1 segment... I guess... its empty */);
    sframe_ptr->close();
  } else {
    graphlab::save_sframe(m_lazy_sframe, this->column_names(),
                          this->dtype(), index_file);
  }
}

void unity_sframe::clear() {
  m_lazy_sframe.reset();
}

size_t unity_sframe::size() {
  if (m_lazy_sframe) {
    return m_lazy_sframe->size();
  } else {
    return 0;
  }
}

size_t unity_sframe::num_columns() {
  if (m_lazy_sframe) {
    return m_lazy_sframe->num_columns();
  } else {
    return 0;
  }
}

std::shared_ptr<unity_sarray_base> unity_sframe::select_column(const std::string &name) {
  Dlog_func_entry();
  logstream(LOG_DEBUG) << "Select Column " << name << std::endl;
  if(this->num_columns() > 0) {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    ret->construct_from_lazy_sarray(m_lazy_sframe->select_column(name));
    return ret;
  } else {
    throw (std::string("Column name " + name + " does not exist."));
  }
}

std::shared_ptr<unity_sframe_base> unity_sframe::select_columns(
    const std::vector<std::string> &names) {
  Dlog_func_entry();
  if (!m_lazy_sframe) {
    log_and_throw("The sframe is not initialized");
  }

  // Check if there is duplicate column names
  std::set<std::string> name_set(names.begin(), names.end());
  if (name_set.size() != names.size()) {
    log_and_throw("There are duplicate column names in the name list");
  }

  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  ret->m_lazy_sframe = m_lazy_sframe->select_columns(names);
  return ret;
}

void unity_sframe::add_column(std::shared_ptr<unity_sarray_base> data,
                              const std::string &name) {
  Dlog_func_entry();
  // Sanity check
  ASSERT_TRUE(data != nullptr);

  // Get underlying sarray
  std::shared_ptr<unity_sarray> us_data = std::static_pointer_cast<unity_sarray>(data);

  if(m_lazy_sframe && num_columns() > 0) {
    if (m_lazy_sframe->size() != data->size()) {
      log_and_throw("New column has different size than current columns!");
    }
  }

  if (!m_lazy_sframe) {
    m_lazy_sframe = std::make_shared<lazy_sframe>(
      std::vector<std::shared_ptr<lazy_sarray<flexible_type>>>({us_data->get_lazy_sarray()}),
      std::vector<std::string>({name}));

  } else {
    m_lazy_sframe->add_column(us_data->get_lazy_sarray(), name);
  }
}

void unity_sframe::add_columns(
    std::list<std::shared_ptr<unity_sarray_base>> data_list,
    std::vector<std::string> name_vec) {
  Dlog_func_entry();
  std::vector<std::shared_ptr<unity_sframe_base>> ret_vec;
  std::vector<std::shared_ptr<unity_sarray_base>> data_vec(data_list.begin(), data_list.end());
  const std::string empty_str = std::string("");
  for(size_t i = 0; i < data_vec.size(); ++i) {
    try {
      if(i >= name_vec.size()) {
        // Name not given!
        this->add_column(data_vec[i], empty_str);
      } else {
        this->add_column(data_vec[i], name_vec[i]);
      }
    } catch(...) {
      //TODO: Back up and delete the columns we added! Not implemented yet!
      log_and_throw(std::string("Column " + std::to_string(i) + " in list could not be added!"));
    }
  }
}

void unity_sframe::set_column_name(size_t i, std::string name) {
  Dlog_func_entry();
  logstream(LOG_DEBUG) << "Args: " << i << "," << name << std::endl;
  ASSERT_MSG(i < num_columns(), "Column index out of bound.");
  std::vector<std::string> colnames = column_names();
  for (size_t j = 0; j < num_columns(); ++j) {
    if (j != i && colnames[j] == name) {
      log_and_throw(std::string("Column name " + name + " already exists"));
    }
  }

  m_lazy_sframe->set_column_name(i, name);
}

void unity_sframe::remove_column(size_t i) {
  Dlog_func_entry();
  logstream(LOG_INFO) << "Args: " << i << std::endl;
  ASSERT_MSG(i < num_columns(), "Column index out of bound.");

  m_lazy_sframe->remove_column(i);
}

void unity_sframe::swap_columns(size_t i, size_t j) {
  Dlog_func_entry();
  logstream(LOG_DEBUG) << "Args: " << i << ", " << j << std::endl;
  ASSERT_MSG(i < num_columns(), "Column index 1 out of bound.");
  ASSERT_MSG(j < num_columns(), "Column index 2 out of bound.");

  m_lazy_sframe->swap_columns(i, j);
}

std::shared_ptr<sframe> unity_sframe::get_underlying_sframe() const {
  Dlog_func_entry();
  if (m_lazy_sframe) {
    return m_lazy_sframe->get_sframe_ptr();
  } else {
    // construct an empty sframe
    auto sframe_ptr = std::make_shared<sframe>();
    sframe_ptr->open_for_write(std::vector<std::string>(), std::vector<flex_type_enum>());
    sframe_ptr->close();
    m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);
    return sframe_ptr;
  }
}


std::shared_ptr<unity_sframe_base> unity_sframe::clone() {
  Dlog_func_entry();
  std::shared_ptr<unity_sframe> sf(new unity_sframe);
  sf->m_lazy_sframe = m_lazy_sframe;
  return sf;
}

void unity_sframe::set_sframe(const std::shared_ptr<sframe>& sf_ptr) {
  Dlog_func_entry();
  m_lazy_sframe = std::make_shared<lazy_sframe>(sf_ptr);
}


std::shared_ptr<unity_sarray_base> unity_sframe::transform(const std::string& lambda,
                                           flex_type_enum type,
                                           bool skip_undefined,
                                           int seed) {
  log_func_entry();

  if (m_lazy_sframe) {
    // create a le_transform operator to lazily evaluate this
    auto transform_operator = std::make_shared<le_transform<std::vector<flexible_type>>>(
      m_lazy_sframe->get_query_tree(),
      lambda,
      skip_undefined,
      seed,
      type,
      this->column_names());

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, type);

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    ret->construct_from_const(0.0, 0);
    return ret;
  }
}

std::shared_ptr<unity_sarray_base> unity_sframe::transform_native(const function_closure_info& toolkit_fn_name,
                                           flex_type_enum type,
                                           bool skip_undefined,
                                           int seed) {
  log_func_entry();
  // find the function
  auto native_execute_function = 
                  get_unity_global_singleton()
                  ->get_toolkit_function_registry()
                  ->get_native_function(toolkit_fn_name);
  std::vector<std::string> colnames = column_names();
  auto lambda = 
      [native_execute_function, colnames](
          const std::vector<flexible_type>& f)->flexible_type {
        std::vector<std::pair<flexible_type, flexible_type> > input(colnames.size());
        ASSERT_EQ(f.size(), colnames.size());
        for (size_t i = 0;i < colnames.size(); ++i) {
          input[i] = {colnames[i], f[i]};
        }
        variant_type var = to_variant(input);
        return variant_get_value<flexible_type>(native_execute_function({var})); 
      };
  return transform_lambda(lambda, type, skip_undefined, seed);
}

std::shared_ptr<unity_sarray_base> unity_sframe::transform_lambda(
    std::function<flexible_type(const std::vector<flexible_type>&)> lambda,
    flex_type_enum type,
    bool skip_undefined,
    int seed) {
  log_func_entry();
  if (m_lazy_sframe) {
    // make the lambda. If the function is 1 argument, we sent it vector<flexible_type>
    // Otherwise, send it the flexible type in separate columns
    auto transform_operator = std::make_shared<le_transform<std::vector<flexible_type>>>(
      m_lazy_sframe->get_query_tree(),
      lambda,
      skip_undefined,
      seed,
      type,
      this->column_names());

    std::shared_ptr<unity_sarray> ret_unity_sarray(new unity_sarray());
    ret_unity_sarray->construct_from_lazy_operator(transform_operator, false, type);

    return ret_unity_sarray;
  } else {
    std::shared_ptr<unity_sarray> ret(new unity_sarray());
    ret->construct_from_const(0.0, 0);
    return ret;
  }
}

std::shared_ptr<unity_sframe_base> unity_sframe::flat_map(
    const std::string& lambda,
    std::vector<std::string> column_names,
    std::vector<flex_type_enum> column_types,
    bool skip_undefined,
    int seed) {
  log_func_entry();
  DASSERT_EQ(column_names.size(), column_types.size());
  DASSERT_TRUE(!column_names.empty());
  DASSERT_TRUE(!column_types.empty());

  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  sframe sf;
  if (m_lazy_sframe) {
    // create a le_flat_map operator to lazily evaluate the input.
    auto flat_map_operator = std::make_shared<le_lambda_flat_map> (
      m_lazy_sframe->get_query_tree(),
      lambda,
      skip_undefined,
      seed,
      m_lazy_sframe->column_names(),
      column_types);
    sf.open_for_write(column_names, column_types, "", SFRAME_DEFAULT_NUM_SEGMENTS);

    auto input_iterators = parallel_iterator<std::vector<flexible_type>>::create(
      flat_map_operator, SFRAME_DEFAULT_NUM_SEGMENTS);

    const size_t batch_size = 1024;
    parallel_for(0, SFRAME_DEFAULT_NUM_SEGMENTS, [&](size_t segment_id) {
          std::vector<std::vector<flexible_type>> buffer;
          auto output_iterator = sf.get_output_iterator(segment_id);
          while (true) {
            buffer = std::move(input_iterators->get_next(segment_id, batch_size));
            if (buffer.empty()) {
              break;
            } else {
              for (auto& row : buffer) {
                *output_iterator = std::move(row);
                ++output_iterator;
              }
            }
          }
        });
    sf.close();
  } else {
    sf.open_for_write(column_names, column_types, "");
    sf.close();
  }
  ret->construct_from_sframe(sf);
  return ret;
}



std::vector<flex_type_enum> unity_sframe::dtype() {
  Dlog_func_entry();
  if (m_lazy_sframe) {
    return m_lazy_sframe->column_types();
  } else {
    return std::vector<flex_type_enum>();
  }
}


std::vector<std::string> unity_sframe::column_names() {
  Dlog_func_entry();
  if (m_lazy_sframe) {
    return m_lazy_sframe->column_names();
  } else {
    return std::vector<std::string>();
  }
}

std::shared_ptr<unity_sframe_base> unity_sframe::head(size_t nrows) {
  log_func_entry();
  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  sframe out_sframe;
  out_sframe.open_for_write(column_names(), dtype(), "", 1);
  if (m_lazy_sframe) {
    graphlab::copy<std::vector<flexible_type>>(m_lazy_sframe, out_sframe.get_output_iterator(0), nrows);
  }
  out_sframe.close();
  ret->construct_from_sframe(out_sframe);
  return ret;
}


dataframe_t unity_sframe::_head(size_t nrows) {
  auto result = head(nrows);
  dataframe_t ret = result->to_dataframe();
  return ret;
};

dataframe_t unity_sframe::_tail(size_t nrows) {
  auto result = tail(nrows);
  dataframe_t ret = result->to_dataframe();
  return ret;
};

std::shared_ptr<unity_sframe_base> unity_sframe::tail(size_t nrows) {
  log_func_entry();
  logstream(LOG_INFO) << "Args: " << nrows << std::endl;
  size_t end = size();
  nrows = std::min<size_t>(nrows, end);
  size_t start = end - nrows;
  return copy_range(start, 1, end);
}

std::shared_ptr<unity_sframe_base> unity_sframe::logical_filter(
    std::shared_ptr<unity_sarray_base> index) {
  log_func_entry();
  if(!m_lazy_sframe) {
    return std::shared_ptr<unity_sframe>(new unity_sframe());
  }

  ASSERT_TRUE(index != nullptr);

  // empty arrays all around. quick exit
  if (this->size() == 0 && index->size() == 0) {
    return std::static_pointer_cast<unity_sframe>(shared_from_this());
  }

  // we are both non-empty. check that size matches
  if (this->size() != index->size()) {
    log_and_throw(std::string("Array size mismatch"));
  }

  // no columns. do nothing
  if (this->num_columns() == 0) {
    return std::static_pointer_cast<unity_sframe>(shared_from_this());
  }

  std::shared_ptr<unity_sarray> us_array = std::static_pointer_cast<unity_sarray>(index);

  auto logical_filter_op = std::make_shared<le_logical_filter<std::vector<flexible_type>>>(
    m_lazy_sframe->get_query_tree(),
    us_array->get_query_tree(),
    flex_type_enum::VECTOR);

  // construct return
  auto ret_lazy_sframe = std::make_shared<lazy_sframe>(logical_filter_op,
                                                       this->column_names(),
                                                       this->dtype());
  std::shared_ptr<unity_sframe> ret_unity_sframe(new unity_sframe());
  ret_unity_sframe->construct_from_lazy_sframe(ret_lazy_sframe);

  return ret_unity_sframe;
}

std::shared_ptr<unity_sframe_base> unity_sframe::append(
    std::shared_ptr<unity_sframe_base> other) {
  log_func_entry();
  ASSERT_TRUE(other != nullptr);

  std::shared_ptr<unity_sframe> other_sframe = std::static_pointer_cast<unity_sframe>(other);
  std::shared_ptr<unity_sframe> ret_unity_sframe(new unity_sframe());

  if(!m_lazy_sframe) {
    if (!other_sframe->m_lazy_sframe) {
      return ret_unity_sframe;
    } else {
      return other;
    }
  } else if (!other_sframe->m_lazy_sframe) {
    return std::static_pointer_cast<unity_sframe>(shared_from_this());
  }

  // zero columns
  if (this->num_columns() == 0) {
    return ret_unity_sframe;
  }

  // check column number matches
  if (this->num_columns() != other_sframe->num_columns()) {
    log_and_throw("Two SFrames have different number of columns");
  }

  // check column name matchs
  auto column_names = this->column_names();
  auto other_column_names = other_sframe->column_names();
  auto column_types = this->dtype();
  auto other_column_types = other_sframe->dtype();
  size_t num_columns = column_names.size();

  // check column type matches
  for(size_t i = 0; i < num_columns; i++) {
    if (column_names[i] != other_column_names[i]) {
      log_and_throw("Column names are not the same in two SFrames");
    }
    if (column_types[i] != other_column_types[i]) {
      log_and_throw("Column types are not the same in two SFrames");
    }
  }

  // short cut for SFrames that are already materialized
  if (this->m_lazy_sframe->is_materialized() && other_sframe->is_materialized()) {
    auto this_sframe_ptr = this->get_underlying_sframe();
    auto other_sframe_ptr = other_sframe->get_underlying_sframe();
    auto return_sframe_ptr = std::make_shared<sframe>(this_sframe_ptr->append(*other_sframe_ptr));
    ret_unity_sframe->m_lazy_sframe = std::make_shared<lazy_sframe>(return_sframe_ptr);

  } else {

    // Append needs size from both left and right in order to know how to hand
    // out values from each segment
    size_t left_size = this->m_lazy_sframe->size();
    size_t right_size = other_sframe->m_lazy_sframe->size();
    auto lazy_append = std::make_shared<le_append<std::vector<flexible_type>>>(
      this->m_lazy_sframe->to_lazy_sarray()->get_query_tree(),
      other_sframe->m_lazy_sframe->to_lazy_sarray()->get_query_tree(),
      left_size + right_size);

    // construct return object
    ret_unity_sframe->m_lazy_sframe = std::make_shared<lazy_sframe>(
      lazy_append,
      this->column_names(),
      this->dtype());
  }

  return ret_unity_sframe;
}

void unity_sframe::begin_iterator() {
  log_func_entry();

  if (m_lazy_sframe) {
    m_sframe_iterator = m_lazy_sframe->get_iterator(1);
  }
}

std::vector< std::vector<flexible_type> > unity_sframe::iterator_get_next(size_t len) {
  std::vector< std::vector<flexible_type> > ret;

  if (!m_lazy_sframe) {
    return ret;
  } else if (!m_sframe_iterator) {
    return ret;
  } else {
    return m_sframe_iterator->get_next(0, len);
  }
}

void unity_sframe::save_as_csv(const std::string& url,
                               std::map<std::string, flexible_type> writing_config) {
  log_func_entry();
  logstream(LOG_INFO) << "Args: " << sanitize_url(url) << std::endl;

  csv_writer writer;
  // first the defaults
  writer.delimiter = ",";
  writer.escape_char = '\\';
  writer.double_quote = true;
  writer.quote_char = '\"';
  writer.use_quote_char = true;
  writer.header = true;

  if (writing_config["delimiter"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string) writing_config["delimiter"];
    if(tmp.length() > 0) writer.delimiter = tmp;
  }
  if (writing_config["escape_char"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)writing_config["escape_char"];
    if (tmp.length() > 0) writer.escape_char = tmp[0];
  }
  if (writing_config.count("double_quote")) {
    writer.double_quote = !writing_config["double_quote"].is_zero();
  }
  if (writing_config["quote_char"].get_type() == flex_type_enum::STRING) {
    std::string tmp = (flex_string)writing_config["quote_char"];
    if (tmp.length() > 0) writer.quote_char = tmp[0];
  }
  if (writing_config.count("use_quote_char")) {
    writer.use_quote_char = !writing_config["use_quote_char"].is_zero();
  }
  if (writing_config.count("header")) {
    writer.header= !writing_config["header"].is_zero();
  }


  if (!m_lazy_sframe) return;

  save_sframe_to_csv(
    url,
    m_lazy_sframe,
    column_names(),
    writer);
}

std::shared_ptr<unity_sframe_base> unity_sframe::sample(float percent, int random_seed) {
  logstream(LOG_INFO) << "Args: " << percent << ", " << random_seed << std::endl;
  if(!m_lazy_sframe) {
    return std::shared_ptr<unity_sframe>(new unity_sframe());
  }

  // empty arrays all around. quick exit
  if (this->size() == 0 || this->num_columns() == 0) {
    return std::static_pointer_cast<unity_sframe>(shared_from_this());
  }

  auto logical_filter_op = std::make_shared<le_logical_filter<std::vector<flexible_type>>>(
    m_lazy_sframe->get_query_tree(),
    std::make_shared<le_random>(percent, random_seed, this->size()),
    flex_type_enum::VECTOR);

  // construct return
  auto ret_lazy_sframe = std::make_shared<lazy_sframe>(logical_filter_op,
                                                       this->column_names(),
                                                       this->dtype());
  std::shared_ptr<unity_sframe> ret_unity_sframe(new unity_sframe());
  ret_unity_sframe->construct_from_lazy_sframe(ret_lazy_sframe);

  return ret_unity_sframe;
}

void unity_sframe::materialize() {
  if (m_lazy_sframe) {
    m_lazy_sframe->materialize();
  }
}


bool unity_sframe::is_materialized() {
  return !m_lazy_sframe || m_lazy_sframe->is_materialized();
}

bool unity_sframe::has_size() {
  return !m_lazy_sframe || m_lazy_sframe->has_size();
}



std::list<std::shared_ptr<unity_sframe_base>> 
unity_sframe::random_split(float percent, int random_seed) {
  log_func_entry();
  logstream(LOG_INFO) << "Args: " << percent << ", " << random_seed << std::endl;

  // construct the first split
  sframe writer1;
  writer1.open_for_write(column_names(),
                         dtype(),
                         std::string(""));

  // construct the second split
  sframe writer2;
  writer2.open_for_write(column_names(),
                         dtype(),
                         std::string(""));
  split(
    m_lazy_sframe,
    writer1,
    writer2,
    [percent](const std::vector<flexible_type>& x) {return random::rand01() <= percent;},
    random_seed);

  writer1.close();
  writer2.close();
  std::shared_ptr<unity_sframe> left(new unity_sframe());
  left->construct_from_sframe(writer1);
  std::shared_ptr<unity_sframe> right(new unity_sframe());
  right->construct_from_sframe(writer2);
  return {left, right};
}

std::shared_ptr<unity_sframe_base> unity_sframe::group(std::string key_column) {
  log_func_entry();
  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  if (m_lazy_sframe) {
    auto sframe_ptr = get_underlying_sframe();
    sframe grouped_sf = graphlab::group(*sframe_ptr, key_column);
    ret->construct_from_sframe(grouped_sf);
  };
  return ret;
}

std::shared_ptr<unity_sframe_base> unity_sframe::groupby_aggregate(
    const std::vector<std::string>& key_columns,
    const std::vector<std::vector<std::string>>& group_columns,
    const std::vector<std::string>& group_output_columns,
    const std::vector<std::string>& group_operations) {

  std::vector<std::shared_ptr<group_aggregate_value>> operators;
  for (const auto& op: group_operations) operators.push_back(get_builtin_group_aggregator(op));
  return groupby_aggregate(key_columns, group_columns, group_output_columns, operators);
}

std::shared_ptr<unity_sframe_base> unity_sframe::groupby_aggregate(
    const std::vector<std::string>& key_columns,
    const std::vector<std::vector<std::string>>& group_columns,
    const std::vector<std::string>& group_output_columns,
    const std::vector<std::shared_ptr<group_aggregate_value>>& group_operations) {
  log_func_entry();
  logstream(LOG_INFO) << "Args: Keys: ";
  for (auto i: key_columns) logstream(LOG_INFO) << i << ",";
  logstream(LOG_INFO) << "\tGroups: ";
  for (auto cols: group_columns) {
    for(auto col: cols) {
      logstream(LOG_INFO) << col << ",";
    }
    logstream(LOG_INFO) << " | ";
  }
  logstream(LOG_INFO) << "\tOperations: ";
  for (auto i: group_operations) logstream(LOG_INFO) << i << ",";
  logstream(LOG_INFO) << std::endl;

  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  if (m_lazy_sframe) {
    ASSERT_EQ(group_columns.size(), group_operations.size());
    std::vector<std::pair<std::vector<std::string>,
        std::shared_ptr<group_aggregate_value> > > operators;
    for (size_t i = 0;i < group_columns.size(); ++i) {
      // avoid copying empty column string
      // this is the case for aggregate::COUNT()
      std::vector<std::string> column_names;
      for (const auto& col : group_columns[i]) {
        if (!col.empty()) column_names.push_back(col);
      }
      operators.push_back( {column_names, group_operations[i]} );
    }

    if (is_materialized() || m_lazy_sframe == NULL) {
      // if empty lazy_sframe, make one
      logstream(LOG_INFO) << "Groupby aggregate on materialized SFrame" << std::endl;
      auto sframe_ptr = get_underlying_sframe();
      sframe grouped_sf = graphlab::groupby_aggregate(*sframe_ptr, key_columns,
                                                      group_output_columns, operators);
      ret->construct_from_sframe(grouped_sf);
    } else if (m_lazy_sframe) {
      logstream(LOG_INFO) << "Groupby aggregate on lazy SFrame" << std::endl;
      sframe grouped_sf = graphlab::lazy_groupby_aggregate(*m_lazy_sframe, key_columns,
                                                           group_output_columns, operators);
      ret->construct_from_sframe(grouped_sf);
    }
  };
  return ret;
};


std::shared_ptr<unity_sframe_base> unity_sframe::join(
    std::shared_ptr<unity_sframe_base> right,
    const std::string join_type,
    std::map<std::string,std::string> join_keys) {
  log_func_entry();
  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  std::shared_ptr<unity_sframe> us_right = std::static_pointer_cast<unity_sframe>(right);

  if(m_lazy_sframe) {
    auto sframe_ptr = get_underlying_sframe();
    auto right_sframe_ptr = us_right->get_underlying_sframe();
    sframe joined_sf = graphlab::join(*sframe_ptr,
                                      *right_sframe_ptr,
                                      join_type,
                                      join_keys);
    ret->construct_from_sframe(joined_sf);
  }

  return ret;
}

std::shared_ptr<unity_sframe_base> 
unity_sframe::sort(const std::vector<std::string>& sort_keys,
                   const std::vector<int>& sort_ascending) {
  log_func_entry();
  std::shared_ptr<unity_sframe> ret(new unity_sframe());

  if (sort_keys.size() != sort_ascending.size()) {
    log_and_throw("sframe::sort key vector and ascending vector size mismatch");
  }

  if (sort_keys.size() == 0) {
    log_and_throw("sframe::sort, nothing to sort");
  }

  if(m_lazy_sframe && m_lazy_sframe->size() > 0) {
    std::vector<bool> b_sort_ascending(sort_ascending.size());
    size_t i = 0;
    for(auto sort_order: sort_ascending) {
      b_sort_ascending[i++] = (bool)sort_order;
    }

    auto sorted_sf = graphlab::sort(m_lazy_sframe, sort_keys, b_sort_ascending);
    ret->set_sframe(sorted_sf);
  }

  return ret;
}

std::shared_ptr<unity_sarray_base> unity_sframe::pack_columns(
    const std::vector<std::string>& pack_column_names,
    const std::vector<std::string>& key_names,
    flex_type_enum dtype,
    const flexible_type& fill_na) {

  log_func_entry();

  if (pack_column_names.size() == 0) {
    throw "There is no column to pack";
  }

  if (!m_lazy_sframe) {
    throw ("SFrame is not initialized yet");
  }

  if (dtype != flex_type_enum::DICT &&
    dtype != flex_type_enum::LIST &&
    dtype != flex_type_enum::VECTOR) {
    log_and_throw("Resulting sarray dtype should be list/array/dict type");
  }

  std::set<flexible_type> pack_column_set(pack_column_names.begin(), pack_column_names.end());
  if (pack_column_set.size() != pack_column_names.size()) {
    throw "There are duplicate names in packed columns";
  }

  // select packing columns
  std::vector<std::shared_ptr<lazy_sarray<flexible_type>>> columns_to_pack =
    m_lazy_sframe->select_columns(pack_column_names)->get_lazy_sarrays();

  std::shared_ptr<lazy_sarray<flexible_type>> ret_column;

  {
    auto_close_sarrays wrapper({dtype});
    graphlab::pack(columns_to_pack, key_names, fill_na, dtype, wrapper.get_sarrays()[0]);
    ret_column = wrapper.get_lazy_sarrays()[0];
  }

  std::shared_ptr<unity_sarray> ret(new unity_sarray());
  ret->construct_from_lazy_sarray(ret_column);
  return ret;
}

std::shared_ptr<unity_sframe_base> unity_sframe::stack(
    const std::string& stack_column_name,
    const std::vector<std::string>& new_column_names,
    const std::vector<flex_type_enum>& new_column_types,
    bool drop_na) {
  log_func_entry();

  if (!m_lazy_sframe) {
    log_and_throw("SFrame is not initialized!");
  }

  // check validity of column names
  auto all_column_names = this->column_names();
  std::set<std::string> my_columns(all_column_names.begin(), all_column_names.end());
  bool stack_column_exists = false;
  for(auto name : new_column_names) {
    if (my_columns.count(name) && name != stack_column_name) {
      throw "Column name '" + name + "' is already used by current SFrame, pick a new column name";
    }

    if (my_columns.count(stack_column_name) > 0) {
      stack_column_exists = true;
    }
  }

  if (!stack_column_exists) {
    log_and_throw("Cannot find stack column " + stack_column_name);
  }

  // validate column types
  size_t new_column_count = 0;
  flex_type_enum stack_column_type = this->select_column(stack_column_name)->dtype();
  if (stack_column_type == flex_type_enum::DICT) {
    new_column_count = 2;
  } else if (stack_column_type == flex_type_enum::VECTOR) {
    new_column_count = 1;
  } else if (stack_column_type == flex_type_enum::LIST) {
    new_column_count = 1;
  } else {
    throw "Column type is not supported for stack";
  }

  if (new_column_types.size() != new_column_count) {
    throw "column types given is not matching the expected number";
  }

  if (new_column_names.size() != new_column_count) {
    throw "column names given is not matching the expected number";
  }

  // check uniqueness of output column name if given
  if (new_column_names.size() == 2 &&
     new_column_names[0] == new_column_names[1] &&
     !new_column_names[0].empty()) {
      throw "There is duplicate column names in new_column_names parameter";
  }

  // create return SFrame
  size_t num_columns = this->num_columns();
  std::vector<std::string> ret_column_names;
  std::vector<flex_type_enum> ret_column_types;
  ret_column_names.reserve(num_columns + new_column_count - 1);
  ret_column_types.reserve(num_columns + new_column_count - 1);

  for(auto name: all_column_names) {
    if (name != stack_column_name) {
      ret_column_names.push_back(name);
      ret_column_types.push_back(m_lazy_sframe->column_type(name));
    }
  }

  ret_column_names.insert(ret_column_names.end(),new_column_names.begin(), new_column_names.end());
  ret_column_types.insert(ret_column_types.end(),new_column_types.begin(), new_column_types.end());

  auto sframe_ptr = std::make_shared<sframe>();
  sframe_ptr->open_for_write(ret_column_names, ret_column_types);

  size_t stack_col_idx = m_lazy_sframe->column_index(stack_column_name);
  auto transform_fn = [&](std::vector<flexible_type> row) {

    ASSERT_TRUE(num_columns == row.size());
    std::vector<std::vector<flexible_type>> ret_rows;

    flexible_type& val = row[stack_col_idx];
    if (val.get_type() == flex_type_enum::UNDEFINED || val.size() == 0) {
      if (!drop_na) {
        std::vector<flexible_type> ret_row;
        ret_row.resize(num_columns + new_column_count - 1);
        if (stack_column_type == flex_type_enum::DICT) {
          ret_row[num_columns - 1] = FLEX_UNDEFINED;
          ret_row[num_columns] = FLEX_UNDEFINED;
        } else {
          ret_row[num_columns - 1] = FLEX_UNDEFINED;
        }

        // copy the rest columns
        for(size_t i = 0, j = 0; i < num_columns; i++) {
          if (i != stack_col_idx) {
            ret_row[j++] = std::move(row[i]);
          }
        }

        ret_rows.push_back(std::move(ret_row));
      }
    } else {
      ret_rows.resize(val.size());

      for(size_t row_idx = 0; row_idx < val.size(); row_idx++) {
        ret_rows[row_idx].resize(num_columns + new_column_count - 1);

        if (stack_column_type == flex_type_enum::DICT) {
          const flex_dict& dict_val = val.get<flex_dict>();
          ret_rows[row_idx][num_columns - 1] = dict_val[row_idx].first;
          ret_rows[row_idx][num_columns] = dict_val[row_idx].second;
        } else if (stack_column_type == flex_type_enum::LIST) {
          ret_rows[row_idx][num_columns - 1] = val.array_at(row_idx);
        } else {
          ret_rows[row_idx][num_columns - 1] = val[row_idx];
        }

        // copy the rest columns
        for(size_t i = 0, j = 0; i < num_columns; i++) {
          if (i != stack_col_idx) {
            ret_rows[row_idx][j++] = row[i];
          }
        }
      }
    }

    return ret_rows;
  };

  graphlab::multi_transform(m_lazy_sframe, *sframe_ptr, transform_fn);

  sframe_ptr->close();

  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  ret->m_lazy_sframe = std::make_shared<lazy_sframe>(sframe_ptr);

  return ret;
}

std::shared_ptr<unity_sframe_base>
unity_sframe::copy_range(size_t start, size_t step, size_t end) {
  log_func_entry();
  if (step == 0) log_and_throw("Range step size must be at least 1");
  // end cannot be past the end
  end = std::min(end, size());

  sframe writer;
  writer.open_for_write(column_names(),
                        dtype(),
                        std::string(""));
  if (end > start) {
    auto sframe_ptr = get_underlying_sframe();
    graphlab::copy_range(*sframe_ptr, writer, start, step, end);
  }
  writer.close();
  std::shared_ptr<unity_sframe> ret(new unity_sframe());
  ret->construct_from_sframe(writer);
  return ret;
}

std::list<std::shared_ptr<unity_sframe_base>> unity_sframe::drop_missing_values(
    const std::vector<std::string> &column_names, bool all, bool split) {
  log_func_entry();

  // Create empty lazy sframe
  std::shared_ptr<unity_sframe> ret_unity_sframe(new unity_sframe());
  std::shared_ptr<unity_sframe> filtered_out(new unity_sframe());

  if(m_lazy_sframe) {
    // Sanity checks
    if(column_names.size() > m_lazy_sframe->num_columns()) {
      log_and_throw("Too many column names given.");
    }

    std::unordered_set<size_t> column_indices =
      _convert_column_names_to_indices(column_names);

    auto filterfn = [column_indices,all](const sframe::value_type f)->bool {
      bool filter = false;
      size_t num_missing_values = 0;

      for(const auto &i : column_indices) {
        if(f[i].is_na()) {
          ++num_missing_values;
        }

        if(!all && num_missing_values) {
          filter = true;
          break;
        }
      }

      if(!filter && all && (num_missing_values == column_indices.size())) {
        filter = true;
      }

      return !filter;
    };

    if(!split) {
      // This option IS lazy
      // Create a lazy sarray of indices to use for later filtering
      auto filter_indices_op =
        std::make_shared<le_transform<std::vector<flexible_type>>>(
            m_lazy_sframe->get_query_tree(),
            filterfn,
            flex_type_enum::INTEGER);
      auto lazy_filter = std::make_shared<lazy_sarray<flexible_type>>(
          filter_indices_op, false, flex_type_enum::INTEGER);


      // Feed this unmaterialized lazy_sarray to the logical_filter operator
      auto drop_missing_op =
        std::make_shared<le_logical_filter<std::vector<flexible_type>>>(
            m_lazy_sframe->get_query_tree(),
            lazy_filter->get_query_tree(),
            flex_type_enum::VECTOR);

      auto ret_lazy_sframe = std::make_shared<lazy_sframe>(drop_missing_op,
          m_lazy_sframe->column_names(),
          m_lazy_sframe->column_types());

      ret_unity_sframe->construct_from_lazy_sframe(ret_lazy_sframe);
    } else {
      // This option IS NOT lazy
      sframe out_frame;
      sframe out_frame_nas;
      out_frame.open_for_write(m_lazy_sframe->column_names(),
          m_lazy_sframe->column_types());
      out_frame_nas.open_for_write(m_lazy_sframe->column_names(),
          m_lazy_sframe->column_types());
      graphlab::split(m_lazy_sframe, out_frame, out_frame_nas, filterfn);

      out_frame.close();
      out_frame_nas.close();

      ret_unity_sframe->construct_from_sframe(out_frame);
      filtered_out->construct_from_sframe(out_frame_nas);
    }
  }

  return {ret_unity_sframe, filtered_out};
}



void unity_sframe::save(oarchive& oarc) const {
  if (m_lazy_sframe) {
    oarc << true;
    oarc << *get_underlying_sframe();
  } else {
    oarc << false;
  }
}

void unity_sframe::load(iarchive& iarc) {
  clear();
  bool has_sframe;
  iarc >> has_sframe;
  if (has_sframe) {
    std::shared_ptr<sframe> sf = std::make_shared<sframe>();
    iarc >> *sf;
    m_lazy_sframe.reset(new lazy_sframe(sf));
  }
}

dataframe_t unity_sframe::to_dataframe() {
  dataframe_t ret;
  for (size_t i = 0; i < num_columns(); ++i) {
    auto name = column_names()[i];
    auto type = dtype()[i];
    ret.names.push_back(name);
    ret.types[name] = type;
    ret.values[name] = select_column(name)->to_vector();
  }
  return ret;
}


std::unordered_set<size_t> unity_sframe::_convert_column_names_to_indices(
    const std::vector<std::string> &column_names) {
  // Convert column names to column indices
  std::unordered_set<size_t> column_indices;
  if(column_names.size()) {
    for(auto &i : column_names) {
      // Fine if this throws, it will just be propagated back with a fine message
      size_t index_to_add = m_lazy_sframe->column_index(i);
      ASSERT_LT(index_to_add, m_lazy_sframe->num_columns());
      column_indices.insert(index_to_add);
    }
  } else {
    // Add all columns
    for(size_t i = 0; i < m_lazy_sframe->num_columns(); ++i) {
      column_indices.insert(i);
    }
  }

  return column_indices;
}

void unity_sframe::delete_on_close() {
  if(m_lazy_sframe) {
    auto sf_ptr = m_lazy_sframe->get_sframe_ptr();
    sf_ptr->delete_files_on_destruction(true);
  }
}

} // namespace graphlab
