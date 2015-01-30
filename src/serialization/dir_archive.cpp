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
#define BOOST_SPIRIT_THREADSAFE

#include <set>
#include <string>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <ini/boost_property_tree_utils.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/fs_utils.hpp>
#include <fileio/sanitize_url.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/s3_api.hpp>
#include <serialization/dir_archive.hpp>
#include <serialization/dir_archive_cache.hpp>
#include <random/random.hpp>

namespace graphlab {


/**
 * This file is the human readable INI file in the directory containing
 * information about the archive.
 */
const char* DIR_ARCHIVE_INI_FILE = "dir_archive.ini";

/**
 * This file is the binary archive used to hold serializable object.
 */
const char* DIR_ARCHIVE_OBJECTS_BIN = "objects.bin";

/**************************************************************************/
/*                                                                        */
/*                             Some Utilities                             */
/*                                                                        */
/**************************************************************************/

namespace {

/**
 * Reads an index file into a struct. Throws an exception on failure.
 */
dir_archive_impl::archive_index_information read_index_file(std::string index_file) {
  general_ifstream fin(index_file);
  if (fin.fail()) {
    log_and_throw(std::string("Unable to open archive index file at ") + index_file);
  }

  // parse the file
  boost::property_tree::ptree data;
  try {
    boost::property_tree::ini_parser::read_ini(fin, data);
  } catch(boost::property_tree::ini_parser_error e) {
    log_and_throw(std::string("Unable to parse archive index file ") + index_file);
  }

  // read the data
  dir_archive_impl::archive_index_information ret;
  ret.version = data.get<int>("archive.version");
  size_t num_prefixes = data.get<size_t>("archive.num_prefixes");
  ret.metadata = ini::read_dictionary_section<std::string>(data, "metadata");
  ret.prefixes = ini::read_sequence_section<std::string>(data, "prefixes", num_prefixes);
  // make prefixes absolute
  std::string index_dir = fileio::get_dirname(index_file);
  for(auto& prefix: ret.prefixes) prefix = fileio::make_absolute_path(index_dir, prefix);
  return ret;
}

/**
 * Writes an index file from a struct. Throws an exception on failure.
 */
void write_index_file(std::string index_file, const dir_archive_impl::archive_index_information& info) {
  logstream(LOG_INFO) << "Writing to index file " << sanitize_url(index_file) << std::endl;

  boost::property_tree::ptree data;
  data.put("archive.version", info.version);
  data.put("archive.num_prefixes", info.prefixes.size());
  ini::write_dictionary_section<std::string>(data, "metadata", info.metadata);
  // make prefixes relative
  std::vector<std::string> relative_paths;
  std::string index_dir = fileio::get_dirname(index_file);
  for(auto prefix: info.prefixes) {
    relative_paths.push_back(fileio::make_relative_path(index_dir, prefix));
  }
  ini::write_sequence_section(data, "prefixes", relative_paths);

  // now write the index
  general_ofstream fout(index_file);
  boost::property_tree::ini_parser::write_ini(fout, data);
  if (!fout.good()) {
    log_and_throw_io_failure("Fail to write.");
  }
  fout.close();
}

/**
 * returns true if there is an element in the search set which is a prefix
 * of the value.
 */
bool is_prefix_in(const std::string& value,
                  const std::set<std::string>& search) {
  // I need to check lower_bound and lower_bound - 1
  auto iter = search.lower_bound(value);
  auto begin = search.begin();
  auto end = search.end();

  // check lower_bound see if it is a prefix
  if (iter != end && boost::starts_with(value, *iter)) {
    return true;
  }
  // check lower_bound - 1 see if it is a prefix
  else if (iter != begin) {
    iter--;
    if (boost::starts_with(value, *iter)) {
      return true;
    }
  }
  return false;
}

} // anonymous namespace



/**************************************************************************/
/*                                                                        */
/*                       dir_archive implementation                       */
/*                                                                        */
/**************************************************************************/
dir_archive::~dir_archive() {
  try {
    close();
  } catch (...) {
  }
}

void dir_archive::make_s3_read_cache(const std::string& s3_url) {
    std::string local_url = dir_archive_cache::get_instance().get_directory(s3_url);
    m_cache_archive.reset(new dir_archive());
    m_cache_archive->open_directory_for_read(local_url);
}

void dir_archive::make_s3_write_cache(const std::string& s3_url) {
    m_cache_archive.reset(new dir_archive());
    std::string temp_dir = graphlab::get_temp_name();
    m_cache_archive->open_directory_for_write(temp_dir);
    std::function<void()> s3_upload_callback = [=] () {
      std::string error = webstor::upload_to_s3_recursive(temp_dir, s3_url).get();
      delete_archive(temp_dir);
      delete_temp_file(temp_dir);
      if (!error.empty()) {
        log_and_throw_io_failure(error);
      }
    };
    m_cache_archive->set_close_callback(s3_upload_callback);
}


void check_directory_writable(std::string directory, bool fail_on_existing_archive) {
  if (!fileio::is_writable_protocol(fileio::get_protocol(directory))) {
      log_and_throw_io_failure("Cannot write to " + sanitize_url(directory));
  }
  // check for DIR_ARCHIVE_INI
  // now the mild annoyance is that the directory may be HDFS, or local disk
  fileio::file_status stat = fileio::get_file_status(directory);
  if (stat == fileio::file_status::REGULAR_FILE) {
    // Always fail trying to overwrite existing file with directory
    log_and_throw_io_failure("Cannot create directory " + sanitize_url(directory) +
                  ". It already exists as a file.");
  } else if (stat == fileio::file_status::DIRECTORY) {
    // enumerate contents of directory
    auto dirlisting = fileio::get_directory_listing(directory);
    bool dir_has_archive = dir_archive::directory_has_existing_archive(dirlisting);

    // a few failure cases
    if (dir_has_archive && fail_on_existing_archive) {
      log_and_throw_io_failure("Directory already contains a GraphLab archive.");
    }
    else if (!dir_has_archive && dirlisting.size() > 0) {
      log_and_throw_io_failure("Directory already exists and does not contain a GraphLab archive.");
    }
    else if (dir_has_archive && !fail_on_existing_archive) {
      // there is an archive, and we are not supposed to fail on existing archive
      // so we delete the directory.
      dir_archive::delete_archive(directory);
    }
  }
}

void dir_archive::init_for_write(const std::string& directory) {
  // ok if we get here, everything is good. begin from scratch and create the
  // archive
  m_directory = directory;
  fileio::create_directory(directory);

  // clear index info
  m_index_info = dir_archive_impl::archive_index_information();
  m_index_info.version = 1;
  // try to write an index file to make sure that the location is writeable
  write_index_file(m_directory + "/" + DIR_ARCHIVE_INI_FILE, m_index_info);

  // begin by putting in ini and the bin files
  m_index_info.prefixes.push_back(m_directory + "/" + DIR_ARCHIVE_INI_FILE);
  m_index_info.prefixes.push_back(m_directory + "/" + DIR_ARCHIVE_OBJECTS_BIN);
  // set up the object stream pointers.
  m_objects_in.reset();
  m_objects_out.reset(new general_ofstream(m_index_info.prefixes[1]));
}

void dir_archive::init_for_read(const std::string& directory) {
  m_index_info = read_index_file(directory + "/" + DIR_ARCHIVE_INI_FILE);
  if (m_index_info.version != 1) log_and_throw_io_failure("Invalid Archive Version");
  m_directory = directory;
  m_objects_out.reset();
  m_objects_in.reset(new general_ifstream(directory + "/" + DIR_ARCHIVE_OBJECTS_BIN));

  // the first 2 elements of the index_info are the INI file and the object file.
  m_read_prefix_index = 2;
}

void dir_archive::open_directory_for_write(std::string directory,
                                           bool fail_on_existing_archive) {
  ASSERT_TRUE(m_objects_in == nullptr);
  ASSERT_TRUE(m_objects_out == nullptr);

  // if directory has a trailing "/" drop it
  if (boost::ends_with(directory, "/")) directory = directory.substr(0, directory.length() - 1);

  check_directory_writable(directory, fail_on_existing_archive);

  // If the target directory is on s3, we create a local directory cache
  if (fileio::get_protocol(directory) == "s3") {
    make_s3_write_cache(directory);
  } else {
    init_for_write(directory);
  }
}

std::string dir_archive::get_directory_metadata(std::string directory, const std::string& key) {
  // if directory has a trailing "/" drop it
  if (boost::ends_with(directory, "/")) {
    directory = directory.substr(0, directory.length() - 1);
  }

  auto index_info = read_index_file(directory + "/" + DIR_ARCHIVE_INI_FILE);
  if (index_info.version != 1) {
    log_and_throw_io_failure("Invalid Archive Version");
  }

  auto iter = index_info.metadata.find(key);
  if (iter == index_info.metadata.end()) {
    log_and_throw("Cannot find metadata '" + key + "'");
  } else {
    return iter->second;
  }
}

void dir_archive::open_directory_for_read(std::string directory) {
  ASSERT_TRUE(m_objects_in == nullptr);
  ASSERT_TRUE(m_objects_out == nullptr);
  // if directory has a trailing "/" drop it
  if (boost::ends_with(directory, "/")) directory = directory.substr(0, directory.length() - 1);

  if (fileio::get_protocol(directory) == "s3") {
    make_s3_read_cache(directory);
  } else {
    init_for_read(directory);
  }
}


std::string dir_archive::get_directory() const {
  return m_directory;
}

// helper for generating random prefixes
size_t get_next_random_number() {
  static graphlab::random::generator gen;
  static bool initialized = false;
  if (! initialized) {
    gen.nondet_seed();
    initialized = true;
  }
  return gen.fast_uniform<size_t>(0, std::numeric_limits<size_t>::max());
}


std::string dir_archive::get_next_write_prefix() {
  if (m_cache_archive)
    return m_cache_archive->get_next_write_prefix();

  ASSERT_TRUE(m_objects_out != nullptr);
  // create a new prefix. It will be called m_xxxxx... etc where xxxxx is some
  // reandomly generated number. If there is conflict by any chance, we will
  // try generate a new one
  std::string new_prefix;
  while (true) {
    std::stringstream strm;
    strm << m_directory << "/m_" << std::hex << get_next_random_number();

    new_prefix = strm.str();

    // If no file in the directory has the given prefix, we are done
    // otherwise, continue generating new prefix
    auto items = fileio::get_directory_listing(m_directory);
    bool prefix_exists = false;
    for(auto& item : items) {
      if (boost::starts_with(item.first, new_prefix)) {
        prefix_exists = true;
        break;
      }
    }

    if (!prefix_exists) break;
  }

  m_index_info.prefixes.push_back(new_prefix);
  return new_prefix;
}

std::string dir_archive::get_next_read_prefix() {
  if (m_cache_archive)
    return m_cache_archive->get_next_read_prefix();

  ASSERT_TRUE(m_objects_in != nullptr);
  ASSERT_LT(m_read_prefix_index, m_index_info.prefixes.size());
  return m_index_info.prefixes[m_read_prefix_index++];
}



bool dir_archive::directory_has_existing_archive(
    const std::vector<std::pair<std::string, fileio::file_status> >& dircontents) {
  // look in dircontents for a file terminating with DIR_ARCHIVE_INIT_FILE
  // put a "/" before it so we are actually searching for this as a file
  std::string archive_ini = std::string("/") + DIR_ARCHIVE_INI_FILE;
  for(auto direntry : dircontents) {
    if (boost::algorithm::ends_with(direntry.first, archive_ini)) {
      // our ini found!
      return true;
    }
  }
  return false;
}

general_ifstream* dir_archive::get_input_stream() {
  if (m_cache_archive)
    return m_cache_archive->get_input_stream();
  return m_objects_in.get();
}

general_ofstream* dir_archive::get_output_stream() {
  if (m_cache_archive)
    return m_cache_archive->get_output_stream();
  return m_objects_out.get();
}

void dir_archive::set_close_callback(std::function<void()>& fn) {
  m_close_callback = fn;
};

void dir_archive::close() {
  if (m_objects_out) {
    // write out the index file
    write_index_file(m_directory + "/" + DIR_ARCHIVE_INI_FILE, m_index_info);
  }
  m_objects_out.reset();
  m_objects_in.reset();
  m_directory = "";
  m_index_info = dir_archive_impl::archive_index_information();
  m_read_prefix_index = 0;

  if (m_close_callback) {
    m_close_callback();
    m_close_callback = NULL;
  }
  if (m_cache_archive) {
    m_cache_archive->close();
    m_cache_archive.reset();
  }
}


void dir_archive::set_metadata(std::string key, std::string val) {
  if (m_cache_archive) {
    m_cache_archive->set_metadata(key, val);
  } else {
    m_index_info.metadata[key] = val;
  }
}


bool dir_archive::get_metadata(std::string key, std::string &val) const {
  if (m_cache_archive) {
    return m_cache_archive->get_metadata(key, val);
  }
  auto iter = m_index_info.metadata.find(key);
  if (iter == m_index_info.metadata.end()) {
    return false;
  } else {
    val = iter->second;
    return true;
  }
}


void dir_archive::delete_archive(std::string directory) {
  try {
    dir_archive_impl::archive_index_information index_info =
        read_index_file(directory + "/" + DIR_ARCHIVE_INI_FILE);

    // stick the prefixes into a set so I can test if a file is part
    // of the prefix quickly
    std::set<std::string> prefixes;
    std::copy(index_info.prefixes.begin(), index_info.prefixes.end(),
              std::inserter(prefixes, prefixes.end()));

    // enumerate all the files in the directory, test if it is a prefix
    // managed by the archive, and delete
    auto dirlisting = fileio::get_directory_listing(directory);
    for (auto direntry: dirlisting) {
      if (is_prefix_in(direntry.first, prefixes)) {
        // its ok if we fail to delete
        try {
          fileio::delete_path(direntry.first);
        } catch (...) { }
      }
    }

    //  after finishing deletion, check if the directory is empty
    dirlisting = fileio::get_directory_listing(directory);
    // if it is empty, we delete the directory
    if (dirlisting.empty()) fileio::delete_path(directory);
  } catch(...) {
  }
}




} // namespace graphlab


