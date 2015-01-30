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
#ifndef GRAPHLAB_UNITY_SFRAME_HPP
#define GRAPHLAB_UNITY_SFRAME_HPP

#include <memory>
#include <string>
#include <vector>
#include <unity/lib/api/unity_sframe_interface.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <sframe/group_aggregate_value.hpp>
#include <cppipc/server/cancel_ops.hpp>

namespace graphlab {


// forward declarations
class lazy_sframe;
template <typename T>
class lazy_sarray;
template <typename T>
class lazy_eval_op_imp_base;
template <typename T>
class parallel_iterator;
class sframe;
class dataframe;
/**
 * This is the SFrame object exposed to Python. It stores internally an
 * \ref sframe object which is a collection of named columns, each of flexible
 * type. The SFrame represents a complete immutable collection of columns.
 * Once created, it cannot be modified. However, shallow copies or sub-selection
 * of columns can be created cheaply.
 *
 * Internally it is simply a single shared_ptr to a \ref sframe object. The
 * sframe construction is delayed until one of the construct calls are made.
 *
 * \code
 * unity_sframe frame;
 * // construct
 * frame.construct(...)
 * // frame is now immutable.
 * \endcode
 *
 * The SFrame may require temporary on disk storage which will be deleted
 * on program termination. Temporary file names are obtained from
 * \ref graphlab::get_temp_name
 */
class unity_sframe : public unity_sframe_base {
    typedef std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> IteratorType;

 public:
  /**
   * Default constructor. Does nothing
   */
  unity_sframe();

  /**
   * Destructor. Calls clear().
   */
  ~unity_sframe();

  /**
   * Constructs an Sframe using a dataframe as input.
   * Dataframe must not contain NaN values.
   */
  void construct_from_dataframe(const dataframe_t& df);

  /**
   * Constructs an Sframe using a sframe as input.
   */
  void construct_from_sframe(const sframe& sf);

  /**
   * Constructs an SFrame from an existing directory on disk saved with
   * save_frame() or a on disk sarray prefix (saved with
   * save_frame_by_index_file()). This function will automatically detect if
   * the location is a directory, or a file. The files will not be deleted on
   * destruction.  If the current object is already storing an frame, it is
   * cleared (\ref clear()). May throw an exception on failure. If an exception
   * occurs, the contents of SArray is empty.
   */
  void construct_from_sframe_index(std::string index_file);

  /**
   * Constructs an SFrame from one or more csv files.
   * To keep the interface stable, the CSV parsing configuration read from a
   * map of string->flexible_type called parsing_config. The URL can be a single
   * filename or a directory name. When passing in a directory and the pattern
   * is non-empty, we will attempt to treat it as a glob pattern.
   *
   * The default parsing configuration is the following:
   * \code
   * bool use_header = true;
   * tokenizer.delimiter = ",";
   * tokenizer.comment_char = '\0';
   * tokenizer.escape_char = '\\';
   * tokenizer.double_quote = true;
   * tokenizer.quote_char = '\"';
   * tokenizer.skip_initial_space = true;
   * \endcode
   *
   * The fields in parsing config are:
   *  - use_header : True if not is_zero()
   *  - delimiter : The entire delimiter string
   *  - comment_char : First character if flexible_type is a string
   *  - escape_char : First character if flexible_type is a string
   *  - double_quote : True if not is zero()
   *  - quote_char : First character if flexible_type is a string
   *  - skip_initial_space : True if not is zero()
   */
  std::map<std::string, std::shared_ptr<unity_sarray_base>> construct_from_csvs(
      std::string url,
      std::map<std::string, flexible_type> parsing_config,
      std::map<std::string, flex_type_enum> column_type_hints);

  void construct_from_lazy_sframe(std::shared_ptr<lazy_sframe> lazy_sframe_ptr);

  /**
   * Saves a copy of the current sframe into a directory.
   * Does not modify the current sframe.
   */
  void save_frame(std::string target_directory);



  /**
   * Saves a copy of the current sframe into a target location defined by
   * an index file. DOes not modify the current sframe.
   */
  void save_frame_by_index_file(std::string index_file);

  /**
   * Clears the contents of the SFrame.
   */
  void clear();

  /**
   * Returns the number of rows in the SFrame. Returns 0 if the SFrame is empty.
   */
  size_t size();

  /**
   * Returns the number of columns in the SFrame.
   * Returns 0 if the sframe is empty.
   */
  size_t num_columns();

  /**
   * Returns an array containing the datatype of each column. The length
   * of the return array is equal to num_columns(). If the sframe is empty,
   * this returns an empty array.
   */
  std::vector<flex_type_enum> dtype();

  /**
   * Returns an array containing the name of each column. The length
   * of the return array is equal to num_columns(). If the sframe is empty,
   * this returns an empty array.
   */
  std::vector<std::string> column_names();

  /**
   * Returns some number of rows of the SFrame in a dataframe representation.
   * if nrows exceeds the number of rows in the SFrame ( \ref size() ), this
   * returns only \ref size() rows.
   */
  std::shared_ptr<unity_sframe_base> head(size_t nrows);

  /**
   * Same as head, returning dataframe.
   */
  dataframe_t _head(size_t nrows);

  /**
   * Returns some number of rows from the end of the SFrame in a dataframe
   * representation. If nrows exceeds the number of rows in the SFrame
   * ( \ref size() ), this returns only \ref size() rows.
   */
  std::shared_ptr<unity_sframe_base> tail(size_t nrows);

  /**
   * Same as head, returning dataframe.
   */
  dataframe_t _tail(size_t nrows);

  /**
   * Returns an SArray with the column that corresponds to 'name'.  Throws an
   * exception if the name is not in the current SFrame.
   */
  std::shared_ptr<unity_sarray_base> select_column(const std::string &name);

  /**
   * Returns a new SFrame which is filtered by a given logical column.
   * The index array must be the same length as the current array. An output
   * array is returned containing only the elements in the current where are the
   * corresponding element in the index array evaluates to true.
   */
  std::shared_ptr<unity_sframe_base> logical_filter(std::shared_ptr<unity_sarray_base> index);

  /**
   * Returns an lazy sframe with the columns that have the given names. Throws an
   * exception if ANY of the names given are not in the current SFrame.
   */
  std::shared_ptr<unity_sframe_base> select_columns(const std::vector<std::string> &names);

  /**
   * Mutates the current SFrame by adding the given column.
   *
   * Throws an exception if:
   *  - The given column has a different number of rows than the SFrame.
   */
  void add_column(std::shared_ptr<unity_sarray_base >data, const std::string &name);

  /**
   * Mutates the current SFrame by adding the given columns.
   *
   * Throws an exception if ANY given column cannot be added
   * (for one of the reasons that add_column can fail).
   *
   * \note Currently leaves the SFrame in an unfinished state if one of the
   * columns fails...the columns before that were added successfully will
   * be there. This needs to be changed.
   */
  void add_columns(std::list<std::shared_ptr<unity_sarray_base>> data_list,
                   std::vector<std::string> name_vec);

  /**
   * Returns a new sarray which is a transform of each row in the sframe
   * using a Python lambda function pickled into a string.
   */
  std::shared_ptr<unity_sarray_base> transform(const std::string& lambda,
                                               flex_type_enum type,
                                               bool skip_undefined,
                                               int seed);


  /**
   * Returns a new sarray which is a transform of each row in the sframe
   * using a Python lambda function pickled into a string.
   */
  std::shared_ptr<unity_sarray_base> transform_native(const function_closure_info& lambda,
                                                      flex_type_enum type,
                                                      bool skip_undefined,
                                                      int seed);

  /**
   * Returns a new sarray which is a transform of each row in the sframe
   * using a Python lambda function pickled into a string.
   */
  std::shared_ptr<unity_sarray_base> transform_lambda(
      std::function<flexible_type(const std::vector<flexible_type>&)> lambda,
      flex_type_enum type,
      bool skip_undefined,
      int seed);
  /**
   * Returns a new sarray which is a transform of each row in the sframe
   * using a Python lambda function pickled into a string.
   */
  std::shared_ptr<unity_sframe_base> flat_map(const std::string& lambda,
                                              std::vector<std::string> output_column_names,
                                              std::vector<flex_type_enum> output_column_types,
                                              bool skip_undefined,
                                              int seed);

  /**
   * Returns a new sarray which is a transform of each row in the sframe
   * using a Python lambda function pickled into a string.
   */
  std::shared_ptr<unity_sframe_base> flat_map_lambda(
      std::function<flexible_type(const std::vector<flexible_type>&)> lambda,
      std::vector<std::string> output_column_names,
      std::vector<flex_type_enum> output_column_types,
      bool skip_undefined,
      int seed);

  /**
   * Set the ith column name.
   *
   * Throws an exception if index out of bound or name already exists.
   */
  void set_column_name(size_t i, std::string name);

  /**
   * Remove the ith column.
   */
  void remove_column(size_t i);

  /**
   * Swap the ith and jth columns.
   */
  void swap_columns(size_t i, size_t j);

  /**
   * Returns the underlying shared_ptr to the sframe object.
   * TODO -- will slowly replace all user of this with lazy_sframe over time
   */
  std::shared_ptr<sframe> get_underlying_sframe() const;

  /**
   * Returns a copy of the unity_sframe.
   */
  std::shared_ptr<unity_sframe_base> clone();

  /**
   * Sets the private shared pointer to an sframe.
   */
  void set_sframe(const std::shared_ptr<sframe>& sf_ptr);

  /**
   * Begin iteration through the SFrame.
   *
   * Works together with \ref iterator_get_next(). The usage pattern
   * is as follows:
   * \code
   * sframe.begin_iterator();
   * while(1) {
   *   auto ret = sframe.iterator_get_next(64);
   *   // do stuff
   *   if (ret.size() < 64) {
   *     // we are done
   *     break;
   *   }
   * }
   * \endcode
   *
   * Note that use of pretty much any of the other data-dependent SArray
   * functions will invalidate the iterator.
   */
  void begin_iterator();

  /**
   * Obtains the next block of elements of size len from the SFrame.
   * Works together with \ref begin_iterator(). See the code example
   * in \ref begin_iterator() for details.
   *
   * This function will always return a vector of length 'len' unless
   * at the end of the array, or if an error has occured.
   *
   * \param len The number of elements to return
   * \returns The next collection of elements in the array. Returns less then
   * len elements on end of file or failure.
   */
  std::vector< std::vector<flexible_type> > iterator_get_next(size_t len);

  /**
   * Save the sframe to url in csv format.
   * To keep the interface stable, the CSV parsing configuration read from a
   * map of string->flexible_type called writing_config.
   *
   * The default writing configuration is the following:
   * \code
   * writer.delimiter = ",";
   * writer.escape_char = '\\';
   * writer.double_quote = true;
   * writer.quote_char = '\"';
   * writer.use_quote_char = true;
   * \endcode
   *
   * For details on the meaning of each config see \ref csv_writer
   *
   * The fields in parsing config are:
   *  - delimiter : First character if flexible_type is a string
   *  - escape_char : First character if flexible_type is a string
   *  - double_quote : True if not is zero()
   *  - quote_char : First character if flexible_type is a string
   *  - use_quote_char : First character if flexible_type is a string
   */
  void save_as_csv(const std::string& url,
                   std::map<std::string, flexible_type> writing_config);

  /**
   * Randomly split the sframe into two parts, with ratio = percent, and  seed = random_seed.
   *
   * Returns a list of size 2 of the unity_sframes resulting from the split.
   */
  std::list<std::shared_ptr<unity_sframe_base>> random_split(float percent, int random_seed);

  /**
   * Sample the rows of sframe uniformly with ratio = percent, and seed = random_seed.
   *
   * Returns unity_sframe* containing the sampled rows.
   */
  std::shared_ptr<unity_sframe_base> sample(float percent, int random_seed);

  /**
   * materialize the sframe, this is different from save() as this is a temporary persist of
   * all sarrays underneath the sframe to speed up some computation (for example, lambda)
   * this will NOT create a new uity_sframe.
  **/
  void materialize();

  /**
   * Returns whether or not this sframe is materialized
   **/
  bool is_materialized();

  /**
   * Return true if the sframe size is known.
   */
  bool has_size();

  /*
   * Returns unity_sframe* where the rows are grouped in consecutive order
   * by the value of the key column
   */
  std::shared_ptr<unity_sframe_base> group(std::string key_column);

  /**
   * Returns unity_sframe* where there is one row for each unique value of the
   * key_column.
   * group_operations is a collection of pairs of {column_name, operation_name}
   * where operation_name is a builtin operator.
   */
  std::shared_ptr<unity_sframe_base> groupby_aggregate(
      const std::vector<std::string>& key_columns,
      const std::vector<std::vector<std::string>>& group_columns,
      const std::vector<std::string>& group_output_columns,
      const std::vector<std::string>& group_operations);

  /**
   * \overload
   */
  std::shared_ptr<unity_sframe_base> groupby_aggregate(
      const std::vector<std::string>& key_columns,
      const std::vector<std::vector<std::string>>& group_columns,
      const std::vector<std::string>& group_output_columns,
      const std::vector<std::shared_ptr<group_aggregate_value>>& group_operations);

  /**
   * Returns a new SFrame which contains all rows combined from current SFrame and "other"
   * The "other" SFrame has to have the same number of columns with the same column names
   * and same column types as "this" SFrame
   */
  std::shared_ptr<unity_sframe_base> append(std::shared_ptr<unity_sframe_base> other);

  std::shared_ptr<unity_sframe_base> join(std::shared_ptr<unity_sframe_base >right,
                          const std::string join_type,
                          std::map<std::string,std::string> join_keys);

  std::shared_ptr<unity_sframe_base> sort(const std::vector<std::string>& sort_keys,
                          const std::vector<int>& sort_ascending);

  /**
    * Pack a subset columns of current SFrame into one dictionary column, using
    * column name as key in the dictionary, and value of the column as value
    * in the dictionary, returns a new SFrame that includes other non-packed
    * columns plus the newly generated dict column.
    * Missing value in the original column will not show up in the packed
    * dictionary value.

    * \param pack_column_names : list of column names to pack
    * \param dict_key_names : dictionary key name to give to the packed dictionary
    * \param dtype: the result SArray type
      missing value is maintained, it could be filled with fill_na value is specified.
    * \param fill_na: the value to fill when missing value is encountered

    * Returns a new SArray that contains the newly packed column
  **/
  std::shared_ptr<unity_sarray_base> pack_columns(
      const std::vector<std::string>& pack_column_names,
      const std::vector<std::string>& dict_key_names,
      flex_type_enum dtype,
      const flexible_type& fill_na);

  /**
   * Convert a dictionary column of the SFrame to two columns with first column
   * as the key for the dictionary and second column as the value for the
   * dictionary. Returns a new SFrame with the two newly created columns, plus
   * all columns other than the stacked column. The values from those columns
   * are duplicated for all rows created from the same original row.

   * \param column_name: string
      The column to stack. The name must come from current SFrame and must be of dict type

   * \param new_column_names: a list of str, optional
      Must be length of two. The two column names to stack the dict value to.
      If not given, the name is automatically generated.

   * \param new_column_types: a list of types, optional
      Must be length of two. The type for the newly created column. If not
      given, the default to [str, int].

   * \param drop_na if true, missing values from dictionary will be ignored. If false,
      for missing dict value, one row will be created with the two new columns' value
      being missing value

   * Retruns a new unity_sframe with stacked columns
  **/
  std::shared_ptr<unity_sframe_base> stack(
      const std::string& column_name,
      const std::vector<std::string>& new_column_names,
      const std::vector<flex_type_enum>& new_column_types,
      bool drop_na);

   /**
    * Extracts a range of rows from an SFrame as a new SFrame.
    * This will extract rows beginning at start (inclusive) and ending at
    * end(exclusive) in steps of "step".
    * step must be at least 1.
    */
  std::shared_ptr<unity_sframe_base> copy_range(size_t start, size_t step, size_t end);

  /**
   * Returns a new SFrame with missing values dropped.
   *
   * Missing values are only searched for in the columns specified in the
   * 'column_names'.  If this vector is empty, all columns will be considered.
   * If 'all' is true, a row is only dropped if all specified columns contain a
   * missing value.  If false, the row is dropped if any of the specified
   * columns contain a missing value.
   *
   * If 'split' is true, this function returns two SFrames, the first being the
   * SFrame with missing values dropped, and the second consisting of all the
   * rows removed.
   *
   * Throws if the column names are not in this SFrame, or if too many are given. 
   */
  std::list<std::shared_ptr<unity_sframe_base>> 
      drop_missing_values(const std::vector<std::string> &column_names, bool all, bool split);

  void save(oarchive& oarc) const;

  void load(iarchive& iarc);

  dataframe_t to_dataframe();

  void delete_on_close();

 private:
  // Helper functions
  std::unordered_set<size_t> _convert_column_names_to_indices(
      const std::vector<std::string> &column_names);

  mutable std::shared_ptr<lazy_sframe> m_lazy_sframe;
  std::unique_ptr<parallel_iterator<std::vector<flexible_type>>> m_sframe_iterator;
};

}

#endif
