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
#ifndef FILEIO_TEMP_FILE_HPP 
#define FILEIO_TEMP_FILE_HPP 
#include <string>
#include <vector>
namespace graphlab {

/**
 * Returns a file name which can be used for a temp file.
 * Returns an empty string on failure, a temporary file name on success.
 * The file name returned is allowed to be a "prefix". i.e. arbitrary extensions
 * can be attached to be tail of the file. For instance, if get_temp_name()
 * returns /tmp/file51apTO, you can use /tmp/file51apTO.csv
 */
std::string get_temp_name();

/**
 * Deletes the temporary file with the name s. 
 * Returns true on success, false on failure (file does not exist, 
 * or cannot be deleted). The file will only be deleted
 * if a prefix of s was previously returned by get_temp_name(). This is done 
 * for safety to prevent this function from being used to delete arbitrary files.
 *
 * For instance, if get_temp_name() previously returned /tmp/file51apTO ,
 * delete_temp_file will succeed on /tmp/file51apTO.csv . delete_temp_file will
 * fail on stuff like /usr/bin/bash
 */
bool delete_temp_file(std::string s);

/**
 * Deletes a collection of temporary files.
 * The files will only be deleted if a prefix of s was previously returned by
 * get_temp_name(). This is done for safety to prevent this function from being
 * used to delete arbitrary files.
 *
 * For instance, if get_temp_name() previously returned /tmp/file51apTO ,
 * delete_temp_files will succeed on a collection of files
 * {/tmp/file51apTO.csv, /tmp/file51apTO.txt}. delete_temp_file will
 * fail on stuff like /usr/bin/bash
 */
void delete_temp_files(std::vector<std::string> files);


/**
 * Deletes all temporary directories in the temporary graphlab/ directory 
 * (/var/tmp/graphlab) which are no longer used. i.e. was created by a process
 * which no longer exists.
 */
void reap_unused_temp_files();

/**
 * Returns the set of temp directories
 */
std::vector<std::string> get_temp_directories();

/**
 * Returns the number of temp directories
 */
size_t num_temp_directories();
} // namespace graphlab
#endif
