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
#include <ctime>
#include <mutex>
#include <parallel/pthread_tools.hpp>
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sarray_reader.hpp>
#include <sframe/sarray_reader_buffer.hpp>
#include <image/image_util.hpp>

namespace graphlab {

static graphlab::mutex reader_shared_ptr_lock;

/**
 * Given an array of flexible_type of mixed type, find the common base type
 * among all of them that I can use to represent the entire array.
 * Fails if no such type exists.
 */
flex_type_enum infer_type_of_list(const std::vector<flexible_type>& vec) {
  std::set<flex_type_enum> types;
  for (const flexible_type& val: vec) {
    if (val.get_type() != flex_type_enum::UNDEFINED) {
      types.insert(val.get_type());
    }
  }
  if (types.size() == 0) return flex_type_enum::FLOAT;
  else if (types.size() == 1) return *(types.begin());
  else if (types.size() == 2) {
    if (types.count(flex_type_enum::INTEGER) && types.count(flex_type_enum::FLOAT)) {
      return flex_type_enum::FLOAT;
    }
    if (types.count(flex_type_enum::LIST) && types.count(flex_type_enum::VECTOR)) {
      return flex_type_enum::LIST;
    }
  }
  throw std::string("Cannot infer Array type. Not all elements of array are the same type.");
}

/**************************************************************************/
/*                                                                        */
/*                         gl_sarray Constructors                         */
/*                                                                        */
/**************************************************************************/

gl_sarray::gl_sarray() {
  instantiate_new();
}

gl_sarray::gl_sarray(const gl_sarray& other) {
  m_sarray = other.get_proxy();
}
gl_sarray::gl_sarray(gl_sarray&& other) {
  m_sarray = std::move(other.get_proxy());
}

gl_sarray::gl_sarray(const std::string& directory) {
  instantiate_new();
  m_sarray->construct_from_sarray_index(directory);
}

gl_sarray& gl_sarray::operator=(const gl_sarray& other) {
  m_sarray = other.get_proxy();
  return *this;
}

gl_sarray& gl_sarray::operator=(gl_sarray&& other) {
  m_sarray = std::move(other.get_proxy());
  return *this;
}

std::shared_ptr<unity_sarray> gl_sarray::get_proxy() const {
  return m_sarray;
}

gl_sarray::gl_sarray(const std::vector<flexible_type>& values, flex_type_enum dtype) {
  if (dtype == flex_type_enum::UNDEFINED) dtype = infer_type_of_list(values);
  instantiate_new();
  get_proxy()->construct_from_vector(values, dtype);
}

gl_sarray::gl_sarray(const std::initializer_list<flexible_type>& values) {
  flex_type_enum dtype = infer_type_of_list(values);
  instantiate_new();
  get_proxy()->construct_from_vector(values, dtype);
}

gl_sarray gl_sarray::from_const(const flexible_type& value, size_t size) {
  gl_sarray ret;
  ret.get_proxy()->construct_from_const(value, size);
  return ret;
}

gl_sarray gl_sarray::from_sequence(size_t start, size_t end, bool reverse) {
  if (end < start) throw std::string("End must be greater than start");
  return unity_sarray::create_sequential_sarray(end - start, 
                                                start, 
                                                reverse);
}

gl_sarray gl_sarray::from_avro(const std::string& directory) {
  gl_sarray ret;
  ret.get_proxy()->construct_from_avro(directory);
  return ret;
}

/**************************************************************************/
/*                                                                        */
/*                   gl_sarray Implicit Type Converters                   */
/*                                                                        */
/**************************************************************************/

gl_sarray::gl_sarray(std::shared_ptr<unity_sarray> sarray) {
  m_sarray = sarray;
}

gl_sarray::gl_sarray(std::shared_ptr<unity_sarray_base> sarray) {
  m_sarray = std::dynamic_pointer_cast<unity_sarray>(sarray);
}

gl_sarray::operator std::shared_ptr<unity_sarray>() const {
  return get_proxy();
}
gl_sarray::operator std::shared_ptr<unity_sarray_base>() const {
  return get_proxy();
}

/**************************************************************************/
/*                                                                        */
/*                      gl_sarray Operator Overloads                      */
/*                                                                        */
/**************************************************************************/

#define DEFINE_OP(OP)\
    gl_sarray gl_sarray::operator OP(const gl_sarray& other) const { \
      return get_proxy()->vector_operator(other.get_proxy(), #OP); \
    } \
    gl_sarray gl_sarray::operator OP(const flexible_type& other) const { \
      return get_proxy()->left_scalar_operator(other, #OP); \
    } \
    gl_sarray operator OP(const flexible_type& opnd, const gl_sarray& opnd2) { \
      return opnd2.get_proxy()->right_scalar_operator(opnd, #OP); \
    }\
    gl_sarray gl_sarray::operator OP ## =(const gl_sarray& other) { \
      (*this) = get_proxy()->vector_operator(other.get_proxy(), #OP); \
      return *this; \
    } \
    gl_sarray gl_sarray::operator OP ## =(const flexible_type& other) { \
      (*this) = get_proxy()->left_scalar_operator(other, #OP); \
      return *this; \
    } 

DEFINE_OP(+)
DEFINE_OP(-)
DEFINE_OP(*)
DEFINE_OP(/)
#undef DEFINE_OP


#define DEFINE_COMPARE_OP(OP) \
    gl_sarray gl_sarray::operator OP(const gl_sarray& other) const { \
      return get_proxy()->vector_operator(other.get_proxy(), #OP); \
    } \
    gl_sarray gl_sarray::operator OP(const flexible_type& other) const { \
      return get_proxy()->left_scalar_operator(other, #OP); \
    } 

DEFINE_COMPARE_OP(<)
DEFINE_COMPARE_OP(>)
DEFINE_COMPARE_OP(<=)
DEFINE_COMPARE_OP(>=)
DEFINE_COMPARE_OP(==)
#undef DEFINE_COMPARE_OP

gl_sarray gl_sarray::operator&(const gl_sarray& other) const {
  return get_proxy()->vector_operator(other.get_proxy(), "&");
} 

gl_sarray gl_sarray::operator|(const gl_sarray& other) const {
  return get_proxy()->vector_operator(other.get_proxy(), "|");
} 

gl_sarray gl_sarray::operator&&(const gl_sarray& other) const {
  return get_proxy()->vector_operator(other.get_proxy(), "&");
}

gl_sarray gl_sarray::operator||(const gl_sarray& other) const {
  return get_proxy()->vector_operator(other.get_proxy(), "|");
}

flexible_type gl_sarray::operator[](int i) const {
  if (i < 0 || (size_t)i >= get_proxy()->size()) {
    throw std::string("Index out of range");
  }
  ensure_has_sarray_reader();
  std::vector<flexible_type> rows(1);
  size_t rows_read  = m_sarray_reader->read_rows(i, i + 1, rows);
  ASSERT_TRUE(rows.size() > 0);
  ASSERT_EQ(rows_read, 1);
  return rows[0];
}


gl_sarray gl_sarray::operator[](const gl_sarray& slice) const {
  return get_proxy()->logical_filter(slice.get_proxy());
}

gl_sarray gl_sarray::operator[](const std::initializer_list<int>& _slice) const {
  std::vector<int> slice(_slice);
  long start = 0, step = 1, stop = 0;
  if (slice.size() == 2) {
    start = slice[0]; stop = slice[1];
  } else if (slice.size() == 3) {
    start = slice[0]; step = slice[1]; stop = slice[2];
  } else {
    throw std::string("Invalid slice. Slice must be of the form {start, end} or {start, step, end}");
  }
  if (start < 0) start = size() + start;
  if (stop < 0) stop = size() + stop;
  return get_proxy()->copy_range(start, step, stop);
}
/**************************************************************************/
/*                                                                        */
/*                               Iterators                                */
/*                                                                        */
/**************************************************************************/

gl_sarray_range gl_sarray::range_iterator(size_t start, size_t end) const {
  if (end == (size_t)(-1)) end = get_proxy()->size();
  if (start > end) {
    throw std::string("start must be less than end");
  }
  // basic range check. start must point to existing element, end can point
  // to one past the end. 
  // but additionally, we need to permit the special case start == end == 0
  // so that you can iterate over empty frames.
  if (!((start < get_proxy()->size() && end <= get_proxy()->size()) ||
        (start == 0 && end == 0))) {
    throw std::string("Index out of range");
  }
  ensure_has_sarray_reader();
  return gl_sarray_range(m_sarray_reader, start, end);
}

/**************************************************************************/
/*                                                                        */
/*                          All Other Functions                           */
/*                                                                        */
/**************************************************************************/

void gl_sarray::save(const std::string& directory, const std::string& format) const {
  if (format == "binary") {
    get_proxy()->save_array(directory);
  } else if (format == "text" || format == "csv") {
    gl_sframe sf;
    sf["X1"] = (*this);
    sf.save(directory, "csv");
  } else {
    throw std::string("Unknown format");
  }
}

size_t gl_sarray::size() const {
  return get_proxy()->size();
}
bool gl_sarray::empty() const {
  return size() == 0;
}
flex_type_enum gl_sarray::dtype() const {
  return get_proxy()->dtype();
}
void gl_sarray::materialize() {
  get_proxy()->materialize();
}
bool gl_sarray::is_materialized() const {
  return get_proxy()->is_materialized();
}

gl_sarray gl_sarray::head(size_t n) const {
  return get_proxy()->head(n);
}

gl_sarray gl_sarray::tail(size_t n) const {
  return get_proxy()->tail(n);
}

gl_sarray gl_sarray::count_words(bool to_lower) const {
  return get_proxy()->count_bag_of_words({{"to_lower",to_lower}});
}
gl_sarray gl_sarray::count_ngrams(size_t n, std::string method, 
                                  bool to_lower, bool ignore_space) const {
  if (method == "word") {
    return get_proxy()->count_ngrams(n, {{"to_lower",to_lower}, 
                                      {"ignore_space",ignore_space}});
  } else if (method == "character") {
    return get_proxy()->count_character_ngrams(n, {{"to_lower",to_lower}, 
                                                {"ignore_space",ignore_space}});
  } else {
    throw std::string("Invalid 'method' input  value. Please input either 'word' or 'character' ");
    __builtin_unreachable();
  }

}
gl_sarray gl_sarray::dict_trim_by_keys(const std::vector<flexible_type>& keys,
                            bool exclude) const {
  return get_proxy()->dict_trim_by_keys(keys, exclude);
}
gl_sarray gl_sarray::dict_trim_by_values(const flexible_type& lower,
                                         const flexible_type& upper) const {
  return get_proxy()->dict_trim_by_values(lower, upper);
}

gl_sarray gl_sarray::dict_keys() const {
  return get_proxy()->dict_keys();
}
gl_sarray gl_sarray::dict_values() const {
  return get_proxy()->dict_values();
}
gl_sarray gl_sarray::dict_has_any_keys(const std::vector<flexible_type>& keys) const {
  return get_proxy()->dict_has_any_keys(keys);
}
gl_sarray gl_sarray::dict_has_all_keys(const std::vector<flexible_type>& keys) const {
  return get_proxy()->dict_has_all_keys(keys);
}

gl_sarray gl_sarray::apply(std::function<flexible_type(const flexible_type&)> fn,
                           flex_type_enum dtype,
                           bool skip_undefined) const {
  return get_proxy()->transform_lambda(fn, dtype, skip_undefined, time(NULL));
}

gl_sarray gl_sarray::filter(std::function<bool(const flexible_type&)> fn,
                            bool skip_undefined) const {
  return (*this)[apply([fn](const flexible_type& arg)->flexible_type {
                         return fn(arg);
                       }, flex_type_enum::INTEGER, skip_undefined)];
}

gl_sarray gl_sarray::sample(double fraction) const {
  return get_proxy()->sample(fraction, time(NULL));
}
gl_sarray gl_sarray::sample(double fraction, size_t seed) const {
  return get_proxy()->sample(fraction, seed);
}
bool gl_sarray::all() const {
  return get_proxy()->all();
}
bool gl_sarray::any() const {
  return get_proxy()->any();
}
flexible_type gl_sarray::max() const {
  return get_proxy()->max();
}
flexible_type gl_sarray::min() const {
  return get_proxy()->min();
}
flexible_type gl_sarray::sum() const {
  return get_proxy()->sum();
}
flexible_type gl_sarray::mean() const {
  return get_proxy()->mean();
}
flexible_type gl_sarray::std() const {
  return get_proxy()->std();
}
size_t gl_sarray::nnz() const {
  return get_proxy()->nnz();
}
size_t gl_sarray::num_missing() const {
  return get_proxy()->num_missing();
}

gl_sarray gl_sarray::datetime_to_str(const std::string& str_format) const {
  return get_proxy()->datetime_to_str(str_format);
}
gl_sarray gl_sarray::str_to_datetime(const std::string& str_format) const {
  return get_proxy()->str_to_datetime(str_format);
}
gl_sarray gl_sarray::pixel_array_to_image(size_t width, size_t height, size_t channels,
                                          bool undefined_on_failure) const {
  return image_util:: vector_sarray_to_image_sarray(
      std::dynamic_pointer_cast<unity_sarray>(get_proxy()), 
      width, height, channels, undefined_on_failure);
}

gl_sarray gl_sarray::astype(flex_type_enum dtype, bool undefined_on_failure) const {
  return get_proxy()->astype(dtype, undefined_on_failure);
}
gl_sarray gl_sarray::clip(flexible_type lower, flexible_type upper) const {
  if (lower == FLEX_UNDEFINED) lower = NAN;
  if (upper == FLEX_UNDEFINED) upper = NAN;
  return get_proxy()->clip(lower, upper);
}
gl_sarray gl_sarray::clip_lower(flexible_type threshold) const {
  return get_proxy()->clip(threshold, NAN);
}
gl_sarray gl_sarray::clip_upper(flexible_type threshold) const {
  return get_proxy()->clip(NAN, threshold);
}


gl_sarray gl_sarray::dropna() const {
  return get_proxy()->drop_missing_values();
}
gl_sarray gl_sarray::fillna(flexible_type value) const {
  return get_proxy()->fill_missing_values(value);
}
gl_sarray gl_sarray::topk_index(size_t topk, bool reverse) const {
  return get_proxy()->topk_index(topk, reverse);
}

gl_sarray gl_sarray::append(const gl_sarray& other) const {
  return get_proxy()->append(other.get_proxy());
}

gl_sarray gl_sarray::unique() const {
  gl_sframe sf({{"a",(*this)}});
  sf = sf.groupby({"a"});
  return sf.select_column("a");
}

gl_sarray gl_sarray::item_length() const {
  return get_proxy()->item_length();
}

gl_sframe gl_sarray::split_datetime(const std::string& column_name_prefix,
                                    const std::vector<std::string>& _limit,
                                    bool tzone) const {
  std::vector<std::string> limit = _limit;
  if (tzone && std::find(limit.begin(), limit.end(), "tzone") == limit.end()) {
    limit.push_back("tzone");
  }
  std::map<std::string, flex_type_enum> default_types{
    {"year", flex_type_enum::INTEGER},
    {"month", flex_type_enum::INTEGER},
    {"day", flex_type_enum::INTEGER},
    {"hour", flex_type_enum::INTEGER},
    {"minute", flex_type_enum::INTEGER},
    {"second", flex_type_enum::INTEGER},
    {"tzone", flex_type_enum::FLOAT}};

  std::vector<flex_type_enum> column_types(limit.size());
  for (size_t i = 0;i < limit.size(); ++i) {
    if (default_types.count(limit[i]) == 0) {
      throw std::string("Unrecognized date time limit specifier");
    } else {
      column_types[i] = default_types[limit[i]];
    }
  }
  std::vector<flexible_type> flex_limit(limit.begin(), limit.end());
  return get_proxy()->expand(column_name_prefix, flex_limit, column_types);
}

gl_sframe gl_sarray::unpack(const std::string& column_name_prefix, 
                           const std::vector<flex_type_enum>& _column_types,
                           const flexible_type& na_value, 
                           const std::vector<flexible_type>& _limit) const {
  auto column_types = _column_types;
  auto limit = _limit;
  if (dtype() != flex_type_enum::DICT && dtype() != flex_type_enum::LIST &&
      dtype() != flex_type_enum::VECTOR) {
    throw std::string("Only SArray of dict/list/array type supports unpack");
  }
  if (limit.size() > 0) {
    std::set<flex_type_enum> limit_types;
    for (const flexible_type& l : limit) limit_types.insert(l.get_type());
    if (limit_types.size() != 1) {
      throw std::string("\'limit\' contains values that are different types");
    } 
    if (dtype() != flex_type_enum::DICT && 
        *(limit_types.begin()) != flex_type_enum::INTEGER) {
      throw std::string("\'limit\' must contain integer values.");
    }
    if (std::set<flexible_type>(limit.begin(), limit.end()).size() != limit.size()) {
      throw std::string("\'limit\' contains duplicate values.");
    }
  }

  if (column_types.size() > 0) {
    if (limit.size() > 0) {
      if (limit.size() != column_types.size()) {
        throw std::string("limit and column_types do not have the same length");
      }
    } else if (dtype() == flex_type_enum::DICT) {
      throw std::string("if 'column_types' is given, 'limit' has to be provided to unpack dict type.");
    } else {
      limit.reserve(column_types.size());
      for (size_t i = 0;i < column_types.size(); ++i) limit.push_back(i);
    }
  } else {
    auto head_rows = head(100).dropna();
    std::vector<size_t> lengths(head_rows.size());
    for (size_t i = 0;i < head_rows.size(); ++i) lengths[i] = head_rows[i].size();
    if (lengths.size() == 0 || *std::max_element(lengths.begin(), lengths.end()) == 0) {
      throw std::string("Cannot infer number of items from the SArray, "
                        "SArray may be empty. please explicitly provide column types");
    }
    if (dtype() != flex_type_enum::DICT) {
      size_t length = *std::max_element(lengths.begin(), lengths.end());
      if (limit.size() == 0) {
        limit.resize(length);
        for (size_t i = 0;i < length; ++i) limit[i] = i;
      } else {
        length = limit.size();  
      }

      if (dtype() == flex_type_enum::VECTOR) {
        column_types.resize(length, flex_type_enum::FLOAT);
      } else {
        column_types.clear();
        for(const auto& i : limit) {
          std::vector<flexible_type> f;
          for (size_t j = 0;j < head_rows.size(); ++j) {
            auto x = head_rows[j];
            if (x != flex_type_enum::UNDEFINED && x.size() > i) {
              f.push_back(x.array_at(i));
            }
          }
          column_types.push_back(infer_type_of_list(f));
        }
      }

    }
  }
  if (dtype() == flex_type_enum::DICT && column_types.size() == 0) {
    return get_proxy()->unpack_dict(column_name_prefix,
                                 limit,
                                 na_value);
  } else {
    return get_proxy()->unpack(column_name_prefix,
                            limit,
                            column_types,
                            na_value);
  } 
}


gl_sarray gl_sarray::sort(bool ascending) const {
  gl_sframe sf({{"a",(*this)}});
  sf = sf.sort("a", ascending);
  return sf.select_column("a");
}

std::ostream& operator<<(std::ostream& out, const gl_sarray& other) {
  auto t = other.head(10);
  auto dtype = other.dtype();
  out << "dtype: " << flex_type_enum_to_name(dtype) << "\n";
  out << "Rows: " << other.size() << "\n";
  out << "[";
  bool first = true;
  for(auto i : t.range_iterator()) {
    if (!first) out << ",";
    if (dtype == flex_type_enum::STRING) out << "\"";
    if (i.get_type() == flex_type_enum::UNDEFINED) out << "None";
    else out << i;
    if (dtype == flex_type_enum::STRING) out << "\"";
    first = false;
  }
  out << "]" << "\n";
  return out;
}

void gl_sarray::instantiate_new() {
  m_sarray = std::make_shared<unity_sarray>();
}

void gl_sarray::ensure_has_sarray_reader() const {
  if (!m_sarray_reader) {
    std::lock_guard<mutex> guard(reader_shared_ptr_lock);
    if (!m_sarray_reader) {
      m_sarray_reader = 
          std::move(get_proxy()->get_underlying_sarray()->get_reader());
    }
  }
}
/**************************************************************************/
/*                                                                        */
/*                            gl_sarray_range                             */
/*                                                                        */
/**************************************************************************/

gl_sarray_range::gl_sarray_range(
    std::shared_ptr<sarray_reader<flexible_type> > m_sarray_reader,
    size_t start, size_t end) {
  m_sarray_reader_buffer = 
      std::make_shared<sarray_reader_buffer<flexible_type>>
          (m_sarray_reader, start, end);
  // load the first value if available
  if (m_sarray_reader_buffer->has_next()) {
    m_current_value = std::move(m_sarray_reader_buffer->next());
  }
}
gl_sarray_range::iterator gl_sarray_range::begin() {
  return iterator(*this, true);
}
gl_sarray_range::iterator gl_sarray_range::end() {
  return iterator(*this, false);
}

/**************************************************************************/
/*                                                                        */
/*                       gl_sarray_range::iterator                        */
/*                                                                        */
/**************************************************************************/

gl_sarray_range::iterator::iterator(gl_sarray_range& range, bool is_start) {
  m_owner = &range;
  if (is_start) m_counter = 0;
  else m_counter = range.m_sarray_reader_buffer->size();
}

void gl_sarray_range::iterator::increment() {
  ++m_counter;
  if (m_owner->m_sarray_reader_buffer->has_next()) {
    m_owner->m_current_value = std::move(m_owner->m_sarray_reader_buffer->next());
  }
}
void gl_sarray_range::iterator::advance(size_t n) {
  n = std::min(n, m_owner->m_sarray_reader_buffer->size());
  for (size_t i = 0;i < n ; ++i) increment();
}

const gl_sarray_range::type& gl_sarray_range::iterator::dereference() const {
  return m_owner->m_current_value;
}

/**************************************************************************/
/*                                                                        */
/*                         gl_sarray_writer_impl                          */
/*                                                                        */
/**************************************************************************/

class gl_sarray_writer_impl {
 public:
  gl_sarray_writer_impl(flex_type_enum type, size_t num_segments);
  void write(const flexible_type& f, size_t segmentid);
  size_t num_segments() const;
  gl_sarray close();
 private:
  std::shared_ptr<sarray<flexible_type> > m_out_sarray;
  std::vector<sarray<flexible_type>::iterator> m_output_iterators;
};

gl_sarray_writer_impl::gl_sarray_writer_impl(flex_type_enum type, size_t num_segments) {
  // open the output array
  if (num_segments == (size_t)(-1)) num_segments = SFRAME_DEFAULT_NUM_SEGMENTS;
  m_out_sarray = std::make_shared<sarray<flexible_type>>();
  m_out_sarray->open_for_write(num_segments);
  m_out_sarray->set_type(type);

  // store the iterators
  m_output_iterators.resize(m_out_sarray->num_segments());
  for (size_t i = 0;i < m_out_sarray->num_segments(); ++i) {
    m_output_iterators[i] = m_out_sarray->get_output_iterator(i);
  }
}

void gl_sarray_writer_impl::write(const flexible_type& f, size_t segmentid) {
  ASSERT_LT(segmentid, m_output_iterators.size());
  *(m_output_iterators[segmentid]) = f;
}

size_t gl_sarray_writer_impl::num_segments() const {
  return m_output_iterators.size();
}

gl_sarray gl_sarray_writer_impl::close() {
  m_output_iterators.clear();
  m_out_sarray->close();
  auto usarray = std::make_shared<unity_sarray>();
  usarray->construct_from_sarray(m_out_sarray);
  return usarray;
}

/**************************************************************************/
/*                                                                        */
/*                            gl_sarray_writer                            */
/*                                                                        */
/**************************************************************************/

gl_sarray_writer::gl_sarray_writer(flex_type_enum type, 
                                   size_t num_segments) {
  // create the pimpl
  m_writer_impl.reset(new gl_sarray_writer_impl(type, num_segments));
}

void gl_sarray_writer::write(const flexible_type& f, 
                             size_t segmentid) {
  m_writer_impl->write(f, segmentid);
}

size_t gl_sarray_writer::num_segments() const {
  return m_writer_impl->num_segments();
}
gl_sarray gl_sarray_writer::close() {
  return m_writer_impl->close();
}


gl_sarray_writer::~gl_sarray_writer() { }
} // namespace graphlab

