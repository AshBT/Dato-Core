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
#include <string>
#include <regex>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <boost/algorithm/string.hpp>
#include <logger/logger.hpp>
#include <timer/timer.hpp>
#include <parallel/thread_pool.hpp>
#include <parallel/atomic.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/parallel_csv_parser.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/sanitize_url.hpp>
#include <fileio/fs_utils.hpp>
#include <cppipc/server/cancel_ops.hpp>
#include <sframe/sframe_constants.hpp>

namespace graphlab {

using fileio::file_status;

/**
 * Code from 
 * http://stackoverflow.com/questions/6089231/getting-std-ifstream-to-handle-lf-cr-and-crlf
 *
 * A getline implementation which supports '\n', '\r' and '\r\n'
 */
std::istream& eol_safe_getline(std::istream& is, std::string& t) {
  t.clear();

  // The characters in the stream are read one-by-one using a std::streambuf.
  // That is faster than reading them one-by-one using the std::istream.
  // Code that uses streambuf this way must be guarded by a sentry object.
  // The sentry object performs various tasks,
  // such as thread synchronization and updating the stream state.

  std::istream::sentry se(is, true);
  std::streambuf* sb = is.rdbuf();

  for(;;) {
    int c = sb->sbumpc();
    switch (c) {
      case '\n':
        return is;
      case '\r':
        if(sb->sgetc() == '\n')
          sb->sbumpc();
        return is;
      case EOF:
        // Also handle the case when the last line has no line ending
        if(t.empty())
          is.setstate(std::ios::eofbit);
        return is;
      default:
        t += (char)c;
    }
  }
}
namespace {

struct csv_info {
  size_t ncols = 0;
  std::vector<std::string> column_names;
  std::vector<flex_type_enum> column_types;
};

inline bool is_end_line_char(char c) {
  return c == '\n' || c == '\r';
}

class parallel_csv_parser {
 public:
  /**
   * Constructs a parallel CSV parser which parses a CSV into SFrames
   * \param column_types This must match the number of columns of the CSV,
   *                     and also specifies the type of each column
   * \param tokenizer The tokenizer rules to use
   * \param num_threads Amount of parallelism to use. (Currently has a default
   *                    upper limit of 4. More seems to be not useful)
   */
  parallel_csv_parser(std::vector<flex_type_enum> column_types, 
                      csv_line_tokenizer tokenizer,
                      bool continue_on_failure,
                      bool store_errors,
                      size_t row_limit,
                      size_t num_threads = thread_pool::get_instance().size()):
      nthreads(std::max<size_t>(num_threads, 2) - 1), 
      parsed_buffer(nthreads), parsed_buffer_last_elem(nthreads), 
      writing_buffer(nthreads), writing_buffer_last_elem(nthreads), 
      error_buffer(nthreads), writing_error_buffer(nthreads),
      thread_local_tokenizer(nthreads, tokenizer), 
    read_group(thread_pool::get_instance()), 
    write_group(thread_pool::get_instance()), column_types(column_types),
    row_limit(row_limit), continue_on_failure(continue_on_failure), 
    store_errors(store_errors) {};
  
  parallel_csv_parser();

  /**
   * Sets the total size of all inputs. Required if multiple output segments
   * are desired. Otherwise all outputs will go to segment 0.
   */
  void set_total_input_size(size_t input_size) {
    total_input_file_sizes = input_size;
  }
  /**
   * Parses an input file into an output frame
   */
  void parse(general_ifstream& fin, sframe& output_frame, sarray<flexible_type>& errors) {
    size_t num_output_segments = output_frame.num_segments();
    size_t current_input_file_size = fin.file_size();
    try {
      timer ti;
      bool fill_buffer_is_good = true;
      while(fin.good() && fill_buffer_is_good && 
            (row_limit == 0 || lines_read.value < row_limit)) { 
        fill_buffer_is_good = fill_buffer(fin);
        if (buffer.size() == 0) break;
        parallel_parse();
        /**************************************************************************/
        /*                                                                        */
        /*      This handles the end of the buffer which needs to be joined       */
        /*                   with the start of the next buffer.                   */
        /*                                                                        */
        /**************************************************************************/
        write_group.join();
        //join_background_write();

        if(cppipc::must_cancel()) {
          log_and_throw(std::string("CSV parsing cancelled"));
        }

        // we need to truncate the current writing buffer to ensure that we don't output more than
        // row_limit rows
        bool incomplete_write = false;
        if (row_limit > 0) {
          size_t remainder = 0;
          if (row_limit > lines_read.value) remainder = row_limit - lines_read.value;
          else remainder = 0;
          for (size_t i = 0; i < parsed_buffer_last_elem.size(); ++i) {
            if (parsed_buffer_last_elem[i] > remainder) {
              parsed_buffer_last_elem[i] = remainder;
              incomplete_write = true;
            }
            remainder -= parsed_buffer_last_elem[i];
          }
        }

        // if we are doing multiple segments
        if (total_input_file_sizes > 0) {
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
        }

        start_background_write(output_frame, errors, current_output_segment);
        if (lines_read.value > 0) {
          logprogress_stream_ontick(5) << "Read " << lines_read.value
                                       << " lines. Lines per second: "
                                       << lines_read.value / get_time_elapsed()
                                       << "\t"
                                       << std::endl;
        }
        if (incomplete_write) {
          write_group.join();
        }
      }
      // file read complete. join the writer
      write_group.join();

      cumulative_file_read_sizes += current_input_file_size;
    } catch (...) {
      try { read_group.join(); } catch (...) { } 
      try { write_group.join(); } catch (...) { } 
      // even on a failure, we still increment the cumulative read count 
      cumulative_file_read_sizes += current_input_file_size;
      throw;
    }
  }

  /**
   * Returns the number of lines which failed to parse
   */
  size_t num_lines_failed() const {
    return num_failures;
  }

  /**
   * Returns the number of CSV lines read
   */  
  size_t num_lines_read() const {
    return lines_read.value;
  }

  /**
   * Returns the number of columns
   */
  size_t num_columns() const {
    return column_types.size();
  }
  
  /**
   * Start timer
   */
  void start_timer() {
    ti.start();
  }

  /**
   * Current time elapsed
   */
  double get_time_elapsed() {
    return ti.current_time();
  }

 private:
  /// number of threads
  size_t nthreads;
  /// thread local parse output buffer
  std::vector<std::vector<std::vector<flexible_type> > > parsed_buffer;
  /// Number of elements in each thread local parse output buffer
  std::vector<size_t> parsed_buffer_last_elem;
  /// thread local write buffer
  std::vector<std::vector<std::vector<flexible_type> > > writing_buffer;
  /// Number of elements in each thread local write buffer
  std::vector<size_t> writing_buffer_last_elem;
  /// Thread local error buffer
  std::vector<std::vector<flexible_type>> error_buffer;
  /// Error buffer used for when writing
  std::vector<std::vector<flexible_type>> writing_error_buffer;
  /// Thread local tokenizer
  std::vector<csv_line_tokenizer> thread_local_tokenizer;
  /// shared buffer
  std::string buffer;
  /// Reading thread pool
  parallel_task_queue read_group;
  /// writing thread pool
  parallel_task_queue write_group;

  std::vector<flex_type_enum> column_types;

  size_t current_output_segment = 0;

  atomic<size_t> lines_read = 0;
  timer ti;
  size_t row_limit = 0;
  size_t cumulative_file_read_sizes = 0;
  size_t total_input_file_sizes = 0;

  volatile bool background_thread_running = false;
  atomic<size_t> num_failures = 0;
  bool continue_on_failure = false;
  bool store_errors = false;

  /**
   * Performs the parse on a section of the buffer (threadid in nthreads)
   * adding new rows into the parsed_buffer.
   * Returns a pointer to the last unparsed character.
   */
  const char* parse_thread(size_t threadid) {
    size_t step = buffer.size() / nthreads;
    // cut the buffer into "nthread uniform pieces"
    const char* pstart = buffer.data() + threadid * step;
    const char* pend = buffer.data() + (threadid + 1) * step;
    const char* bufend = buffer.data() + buffer.size();
    if (threadid == nthreads - 1) pend = bufend;

    // ok, this is important. Pay attention.
    // We are sweeping from 
    //  - the first line which begins AFTER pstart, but before pend
    //  - And we are finishing on the last line which ends AFTER pend.
    // (if we are the last thread, something special happens and
    //  the last unparsed line gets pushed into the next buffer)
    //  This correctly takes care of all cases; even where a line spans
    //  multiple pieces.

    /**************************************************************************/
    /*                                                                        */
    /*                        Find the Start Position                         */
    /*                                                                        */
    /**************************************************************************/
    // threadid 0 begins at start
    bool start_position_found = (threadid == 0);
    if (threadid > 0) {
      // find the first line beginning after pstart but before pend
      char prevchar = 0;
      while(pstart != pend && !is_end_line_char(*pstart)) {
        prevchar = (*pstart);
        ++pstart;
      }
      // the first loop may halt at a '\r'. Windows line endings are 
      // annoyingly, \r\n. So we need to trim one more character.
      if (prevchar == '\r' && pstart != pend && (*pstart) == '\n') ++pstart;
      if (pstart != pend) {
        ++pstart;
        start_position_found = true;
      }
    } 
    if (start_position_found) {
      /**************************************************************************/
      /*                                                                        */
      /*                         Find the End Position                          */
      /*                                                                        */
      /**************************************************************************/
      // find the end position
      char prevchar = 0;
      while(pend != bufend && !is_end_line_char(*pend)) {
        prevchar = (*pstart);
        ++pend;
      }
      // the first loop may halt at a '\r'. Windows line endings are 
      // annoyingly, \r\n. So we need to trim one more character.
      if (prevchar == '\r' && pend != bufend && (*pend) == '\n') ++pend;
      if (pend != bufend) ++pend; // we need one past the '\n'

      // make a thread local parsed tokens set
      /**************************************************************************/
      /*                                                                        */
      /*                          start parsing lines                           */
      /*                                                                        */
      /**************************************************************************/
      // this is the current character I am scanning
      const char comment_char = thread_local_tokenizer[threadid].comment_char;
      const char* pnext = pstart;
      while(pnext < pend) {
        // search for a new line
        if (is_end_line_char(*pnext)) {
          // parse pstart until pnext
          // clear local tokens
          size_t nextelem = parsed_buffer_last_elem[threadid];
          if (nextelem >= parsed_buffer[threadid].size()) parsed_buffer[threadid].resize(nextelem + 1);
          std::vector<flexible_type>& local_tokens = parsed_buffer[threadid][nextelem];
          local_tokens.resize(column_types.size());
          for (size_t i = 0;i < column_types.size(); ++i) {
            if (local_tokens[i].get_type() != column_types[i]) {
              local_tokens[i].reset(column_types[i]);
            }
          }
          size_t num_tokens_parsed = 
              thread_local_tokenizer[threadid].
              tokenize_line(pstart, pnext - pstart,
                            local_tokens, 
                            true /*permit undefined*/);

          if (num_tokens_parsed == local_tokens.size()) {
            ++parsed_buffer_last_elem[threadid];
          } else {

            // incomplete parse
            std::string badline(pstart, pnext - pstart);
            boost::algorithm::trim(badline);

            if (!badline.empty() && badline[0] != comment_char) {
              // keep track of line for error reporting
              if (store_errors) error_buffer[threadid].push_back(badline);
              if (continue_on_failure) {
                if (num_failures.value < 10) {
                  logprogress_stream << std::string("Unable to parse line \"") + 
                      std::string(pstart, pnext - pstart) + "\"" << std::endl;
                }
                ++num_failures;
              } else {
                log_and_throw(std::string("Unable to parse line \"") + 
                              std::string(pstart, pnext - pstart) + "\"\n" +
                              "Set error_bad_lines=False to skip bad lines");
              }
            } 
          } 
          // done. go to next token
          // if pnext is a '\r' and we are not at the end of buffer
          // we try to drop the next '\n' to capture the windows newline
          if ((*pnext) == '\r' && pnext != pend && (*pnext) == '\n') ++pnext;
          pstart = pnext;
          ++pstart;
        }
        ++pnext;
      }
    }
    return pstart;
  }

  /**
   * Adds a large chunk of bytes to the buffer. Returns true if all requested
   * lines were read. False otherwise: indicating this is the last block.
   */
  bool fill_buffer(general_ifstream& fin) {
    if (fin.good()) {
      size_t oldsize = buffer.size();
      size_t amount_to_read = SFRAME_CSV_PARSER_READ_SIZE;
      buffer.resize(buffer.size() + amount_to_read);
      fin.read(&(buffer[0]) + oldsize, buffer.size() - oldsize);
      if ((size_t)fin.gcount() < amount_to_read) {
        // if we did not read till the entire buffer , this is an EOF
        buffer.resize(oldsize + fin.gcount());
        // EOF. Put a '\n' to catch the last line
        if (buffer.size() > 0 && buffer[buffer.size() - 1] != '\n') {
          buffer.push_back('\n');
        }
        return false;
      } else {
        return true;
      }
    } else {
      // EOF. Put a '\n' to catch the last line
      if (buffer.size() > 0) buffer.push_back('\n');
      return false;
    }
  }

  /**
   * Performs a parallel parse of the contents of the buffer, adding to
   * parsed_buffer.
   */
  void parallel_parse() {
    // take a pointer to the last token that has been succesfully parsed.
    // this will tell me how much buffer I need to shift to the next read.
    // parse buffer in parallel
    mutex last_parsed_token_lock;
    const char* last_parsed_token = buffer.data();
    
    for (size_t threadid = 0; threadid < nthreads; ++threadid) {
      read_group.launch(
          [=,&last_parsed_token_lock,&last_parsed_token](void) {
            const char* ret = parse_thread(threadid);
            std::lock_guard<mutex> guard(last_parsed_token_lock);
            if (last_parsed_token < ret) last_parsed_token = ret;
          } );
    }
    read_group.join();
    // ok shift the buffer left
    if ((size_t)(last_parsed_token - buffer.data()) < buffer.size()) {
      buffer = buffer.substr(last_parsed_token - buffer.data());
    } else {
      buffer.clear();
    }
  }

  /**
   * Spins up a background thread to write parse results from parallel_parse 
   * to the output frame. First the parsed_buffer is swapped into the 
   * writing_buffer, thus permitting the parsed_buffer to be used again in
   * a different thread.
   */
  void start_background_write(sframe& output_frame, 
                              sarray<flexible_type>& errors_array, 
                              size_t output_segment) {
    // switch the parse buffer with the write buffer
    writing_buffer.swap(parsed_buffer);
    writing_buffer_last_elem.swap(parsed_buffer_last_elem);
    background_thread_running = true;
    // reset the parsed buffer
    parsed_buffer_last_elem.clear();
    parsed_buffer_last_elem.resize(nthreads, 0);
    writing_error_buffer.swap(error_buffer);

    write_group.launch([&, output_segment] {
        auto iter = output_frame.get_output_iterator(output_segment);
        for (size_t i = 0; i < writing_buffer.size(); ++i) {
          std::copy(writing_buffer[i].begin(), 
                    writing_buffer[i].begin() + writing_buffer_last_elem[i], iter);
          lines_read.inc(writing_buffer_last_elem[i]);
        }
        if (store_errors) { 
          auto errors_iter = errors_array.get_output_iterator(0);
          for (auto& chunk_errors : writing_error_buffer) {
            std::copy(chunk_errors.begin(), chunk_errors.end(), errors_iter);
            chunk_errors.clear();
          }
        }        
        background_thread_running = false;
      });
  }

};

/**
 * Makes unique column names R-style.
 *
 * if we have duplicated column names, the duplicated names get a .1, .2, .3
 * etc suffix.
 *
 * e.g.
 * {"A", "A", "A"} --> {"A", "A.1", "A.2"}
 *
 * If the name with the suffix already exists, the suffix is skipped:
 *
 * e.g.
 * {"A", "A", "A.1"} --> {"A", "A.2", "A.1"}
 * 
 * \param column_names The set of column names to be renamed. The vector
 *                     will be modified in place.
 */
void make_unique_column_names(std::vector<std::string>& column_names) {
  // this is the set of column names to the left of the column we 
  // are current inspected. i.e. these column names are already validated to
  // be correct.
  log_func_entry();
  std::set<std::string> accepted_column_names;
  for (size_t i = 0;i < column_names.size(); ++i) {
    std::string colname = column_names[i];

    if (accepted_column_names.count(colname)) {
      // if this is a duplicated name
      // we need to rename this column
      // insert the rest of the columns into a new set to test if the suffix
      // already exists.
      std::set<std::string> all_column_names(column_names.begin(),
                                             column_names.end());
      // start incrementing at A.1, A.2, etc. 
      size_t number = 1;
      std::string new_column_name;
      while(1) {
        new_column_name = colname + "." + std::to_string(number);
        if (all_column_names.count(new_column_name) == 0) break;
        ++number;
      }
      column_names[i] = new_column_name;
    }
    accepted_column_names.insert(column_names[i]);
  }
}

/**************************************************************************/
/*                                                                        */
/* Parse Header Line                                                      */
/* -----------------                                                      */
/* Read the header line if "use_header" is specified.                     */
/* Use the column names from the header. Performing column name mangling  */
/* R-style if there are duplicated column names                           */
/*                                                                        */
/* If use_header is not specified, we read the first line anyway,         */
/* and try to figure out the number of columns. Use the sequence X1,X2,X3 */
/* as column names.                                                       */
/*                                                                        */
/* At completion of this section,                                         */
/* - ncols is set                                                         */
/* - column_names.size() == ncols                                         */
/* - column_names contain all the column headers.                         */
/* - all column names are different                                       */
/*                                                                        */
/**************************************************************************/
void read_csv_header(csv_info& info,
                     const std::string& path,
                     csv_line_tokenizer& tokenizer,
                     bool use_header) {
  std::string first_line;
  std::vector<std::string> first_line_tokens;
  general_ifstream probe_fin(path);

  if (!probe_fin.good()) {
    log_and_throw("Fail reading " + sanitize_url(path));
  }

  // skip rows with no data
  while (first_line_tokens.size() == 0 && probe_fin.good()) {
    eol_safe_getline(probe_fin, first_line);
    boost::algorithm::trim(first_line);
    tokenizer.tokenize_line(first_line.c_str(), 
                            first_line.length(), 
                            first_line_tokens);
  }

  info.ncols = first_line_tokens.size();
  if (info.ncols == 0) log_and_throw("First line is empty. Invalid CSV File?");

  if (use_header) {
    info.column_names = first_line_tokens;
    make_unique_column_names(info.column_names);
    first_line.clear();
    first_line_tokens.clear();
  } else {
    info.column_names.resize(info.ncols);
    for (size_t i = 0;i < info.ncols; ++i) {
      info.column_names[i] = "X" + std::to_string(i + 1);
    }
    // do not clear first_line and first_line_tokens. They are actual data.
  }
}

/**************************************************************************/
/*                                                                        */
/* Type Determination                                                     */
/* ------------------                                                     */
/* At this stage we fill column_types. This stage can be expanded in      */
/* the future to perform more comprehensive type inference. But at the    */
/* moment, we simply default all column types to STRING, and use the      */
/* types specified in column_type_hints if there is a column name         */
/* which matches.                                                         */
/*                                                                        */
/* At completion of this section:                                         */
/* - column_types.size() == column_names.size() == ncols                  */
/*                                                                        */
/**************************************************************************/
void get_column_types(csv_info& info, 
                      std::map<std::string, flex_type_enum> column_type_hints) {
  info.column_types.resize(info.ncols, flex_type_enum::STRING);

  if (column_type_hints.count("__all_columns__")) {
    info.column_types = std::vector<flex_type_enum>(info.ncols, column_type_hints["__all_columns__"]);
  } else if (column_type_hints.count("__X0__")) {
    if (column_type_hints.size() != info.column_types.size()) {
      std::stringstream warning_msg;
      warning_msg << "column_type_hints has different size from actual number of columns: " 
                  << "column_type_hints.size()=" << column_type_hints.size()
                  << ";number of columns=" << info.ncols
                  << std::endl;
      log_and_throw(warning_msg.str());
    }
    for (size_t i = 0; i < info.ncols; ++i) {
      std::stringstream key;
      key << "__X" << i << "__";
      if (!column_type_hints.count(key.str())) {
        log_and_throw("Bad column type hints");
      }
      info.column_types[i] = column_type_hints[key.str()];
    }
  } else {
    for (size_t i = 0; i < info.column_names.size(); ++i) {
      if (column_type_hints.count(info.column_names[i])) {
        flex_type_enum coltype = column_type_hints.at(info.column_names[i]);
        /*
         * if (coltype == flex_type_enum::UNDEFINED) {
         *   log_and_throw("Cannot specify a column type as undefined");
         * }
         */
        info.column_types[i] = coltype;
        column_type_hints.erase(info.column_names[i]);
      }
    }
    if (column_type_hints.size() > 0) {
      std::stringstream warning_msg;
      warning_msg << "These column type hints were not used:";
      for(const auto &hint : column_type_hints) {
        warning_msg << " " << hint.first;
      }
      logprogress_stream << warning_msg.str() << std::endl;
    }
  }
}

/**
 * Get info about a CSV.
 */
void get_csv_info(csv_info& info, 
                  std::string path, 
                  csv_line_tokenizer& tokenizer, 
                  bool use_header,
                  std::map<std::string, flex_type_enum> column_type_hints) {
  timer ti;
  
  ti.start();
  read_csv_header(info, path, tokenizer, use_header);
  
  ti.start();
  get_column_types(info, column_type_hints);
  
  logstream(LOG_INFO) << "Type Determination in " 
                      << ti.current_time() 
                      << std::endl;
}

} // anonymous namespace

/**
 * Parsed a CSV file to an SFrame.
 *
 * \param path The file to open as a csv
 * \param tokenizer The tokenizer configuration to use. This should be 
 *                  filled with all the tokenization rules (like what
 *                  separator character to use, what quoting character to use, 
 *                  etc.)
 * \param use_header Whether the first (non-commented) line of the file
 *                   is the column name header.
 * \param continue_on_failure Whether we should just skip line errors.
 * \param store_errors Whether failed parses will be stored in an sarray of 
 *                     strings and returned.
 * \param column_type_hints A collection of column name->type. Every other 
 *                          column type will be parsed as a string
 * \param row_limit  The number of rows to read. 
 * If this value is 0, all rows are read.
 * \param writer The sframe writer to use.
 * \param frame_sidx_file Where to write the frame to 
 * \param parallel_csv_parser A parallel_csv_parser
 * \param errors A reference to a map in which to store an sarray of bad lines 
 * for each input file.
 */
void parse_csv_to_sframe(
    const std::string& path,
    csv_line_tokenizer& tokenizer,
    bool use_header,
    bool continue_on_failure,
    bool store_errors,
    std::map<std::string, flex_type_enum> column_type_hints,
    size_t row_limit,
    sframe& frame,
    std::string frame_sidx_file,
    parallel_csv_parser& parser,
    std::map<std::string, std::shared_ptr<sarray<flexible_type>>>& errors) {

  logstream(LOG_INFO) << "Loading sframe from " << sanitize_url(path) << std::endl;

  // load; For each line, insert into the frame
  {
    general_ifstream fin(path);
    if (!fin.good()) log_and_throw("Cannot open " + sanitize_url(path));

    // if use_header, we keep throwing away empty or comment lines until we 
    // get one good line
    if (use_header) {
      std::vector<std::string> first_line_tokens;
      // skip rows with no data, and skip the head
      while (first_line_tokens.size() == 0 && fin.good()) {
        std::string line;
        eol_safe_getline(fin, line);
        tokenizer.tokenize_line(line.c_str(), line.length(), first_line_tokens);
      }
      // if we are going to store errors, we don't do early skippng on 
      // mismatched files
      if (!store_errors && first_line_tokens.size() != parser.num_columns()) {
        logprogress_stream << "Unexpected number of columns found in " << path
                           << ". Skipping this file." << std::endl;
        return;
      }
    }
    
    // store errors for this particular file in an sarray
    auto file_errors = std::make_shared<sarray<flexible_type>>();
    if (store_errors) {
      file_errors->open_for_write();
      file_errors->set_type(flex_type_enum::STRING);
    }

    try {
      parser.parse(fin, frame, *file_errors);
    } catch(const std::string& s) {
      frame.close();
      if (store_errors) file_errors->close();
      log_and_throw(s);
    }

    if (continue_on_failure && parser.num_lines_failed() > 0) {
      logprogress_stream << parser.num_lines_failed() 
                         << " lines failed to parse correctly" 
                         << std::endl;
    }

    if (store_errors) {
      file_errors->close();
      if (file_errors->size() > 0) {
        errors.insert(std::make_pair(path, file_errors));
      }
    }

    logprogress_stream << "Finished parsing file " << sanitize_url(path) << std::endl;
  }
}

std::map<std::string, std::shared_ptr<sarray<flexible_type>>> parse_csvs_to_sframe(
    const std::string& url,
    csv_line_tokenizer& tokenizer,
    bool use_header,
    bool continue_on_failure,
    bool store_errors,
    std::map<std::string, flex_type_enum> column_type_hints,
    size_t row_limit,                          
    sframe& frame,
    std::string frame_sidx_file) {
  
  if (store_errors) continue_on_failure = true;
  // otherwise, check that url is valid directory, and get its listing if no 
  // pattern present
  std::vector<std::string> files;
  std::vector<std::pair<std::string, file_status>> file_and_status = fileio::get_glob_files(url);
  
  for (auto p : file_and_status) {
    if (p.second == file_status::REGULAR_FILE) {
      logstream(LOG_INFO) << "Adding CSV file " 
                          << sanitize_url(p.first)
                          << " to list of files to parse"
                          << std::endl;
      files.push_back(p.first);
    }
  }

  file_and_status.clear(); // don't need these anymore
  
  // ensure that we actually found some valid files
  if (files.empty()) {
    log_and_throw(std::string("No files corresponding to the specified path (") + 
                  sanitize_url(url) + std::string(")."));
  }

  // get CSV info from first file
  csv_info info;
  get_csv_info(info, files[0], tokenizer, use_header, column_type_hints);
  logstream(LOG_INFO) << "CSV num. columns: " << info.ncols << std::endl;

  if (info.ncols <= 0)
    log_and_throw(std::string("CSV parsing cancelled: 0 columns found"));

  parallel_csv_parser parser(info.column_types, tokenizer,
                             continue_on_failure, store_errors, row_limit);
  // get the total input file size so I can stripe it across segments
  size_t total_input_file_sizes = 0;
  for (auto file : files) {
    general_ifstream fin(file);
    total_input_file_sizes += fin.file_size();
  }
  parser.set_total_input_size(total_input_file_sizes);

  if (!frame.is_opened_for_write()) {
    // open as many segments as there are temp directories.
    // But at least one segment
    frame.open_for_write(info.column_names, info.column_types, 
                         frame_sidx_file, 
                         std::max<size_t>(1, num_temp_directories()));
  }

  // create the errors map
  std::map<std::string, std::shared_ptr<sarray<flexible_type>>> errors;

  // start parser timer for cumulative time consumed (in seconds)
  parser.start_timer();

  for (auto file : files) {
    // check that we've read < row_limit  
    if (parser.num_lines_read() < row_limit || row_limit == 0) {      
      parse_csv_to_sframe(file, tokenizer, use_header, continue_on_failure, 
                          store_errors, column_type_hints, row_limit, frame, 
                          frame_sidx_file, parser, errors);
    } else break;
  }
  
  logprogress_stream << "Parsing completed. Parsed " << parser.num_lines_read()
                     << " lines in " << parser.get_time_elapsed() << " secs."  << std::endl;

  
  if (frame.is_opened_for_write()) frame.close();
  
  return errors;
}

} // namespace graphlab
