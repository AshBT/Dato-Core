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
#ifndef GRAPHLAB_TABLE_ELEMENT_PRINTERS_H_
#define GRAPHLAB_TABLE_ELEMENT_PRINTERS_H_

#include <flexible_type/flexible_type.hpp>
#include <type_traits>
#include <atomic>

namespace graphlab {

struct progress_time;

namespace table_internal {

/** Printers for each of the primimtive types.  Called by the routines below.
 */

void _print_string(std::ostringstream& ss, size_t width, const std::string& s);
void _print_double(std::ostringstream& ss, size_t width, double s);
void _print_bool(std::ostringstream& ss, size_t width, bool b);
void _print_long(std::ostringstream& ss, size_t width, long v);
void _print_time(std::ostringstream& ss, size_t width, double pt);
void _print_flexible_type(std::ostringstream& ss, size_t width, const flexible_type& pt);

////////////////////////////////////////////////////////////////////////////////
// Now specific classes to direct how each type is printed and how
// values are stored in the type string.

class table_printer_element_base {
 public:
  virtual void print(std::ostringstream& ss, size_t width){};
  virtual flexible_type get_value(){return FLEX_UNDEFINED;}
};

template <typename T, class Enable = void>
struct table_printer_element : public table_printer_element_base
{
  static constexpr bool valid_type = false;
};

/** For printing doubles.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if<std::is_floating_point<T>::value>::type >
    : public table_printer_element_base {

 public:
  static constexpr bool valid_type = true;

  table_printer_element(T v)
      : value(double(v))
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_double(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  double value;
};



/** For printing bools.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if<std::is_same<T, bool>::value>::type >
    : public table_printer_element_base {
 public:
  static constexpr bool valid_type = true;

  table_printer_element(T v)
      : value(v)
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_bool(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  bool value;
};


/** For printing signed integers.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if< (std::is_integral<T>::value && (!std::is_same<T, bool>::value))>::type >
    : public table_printer_element_base {
public:
  static constexpr bool valid_type = true;

  table_printer_element(const T& v)
      : value(v)
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_long(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  long value;
};

/** For printing std atomics.
 */
template <typename T>
struct table_printer_element
<std::atomic<T>, typename std::enable_if<std::is_integral<T>::value>::type >
    : public table_printer_element<T> {
public:
  table_printer_element(const std::atomic<T>& v)
      : table_printer_element<T>(T(v))
  {}
};

/** For printing graphlab atomics.
 */
template <typename T>
struct table_printer_element
<graphlab::atomic<T>, typename std::enable_if<std::is_integral<T>::value>::type >
    : public table_printer_element<T> {
public:
  table_printer_element(const std::atomic<T>& v)
      : table_printer_element<T>(T(v))
  {}
};


/** For printing strings.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if<std::is_convertible<T, std::string>::value
                            && !std::is_same<T, flexible_type>::value>::type>
    : public table_printer_element_base {

 public:
  static constexpr bool valid_type = true;

  table_printer_element(const T& v)
      : value(v)
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_string(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  std::string value;
};

/** For printing progress time.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if<std::is_same<T, progress_time>::value>::type>
    : public table_printer_element_base {

 public:
  static constexpr bool valid_type = true;

  table_printer_element(double v)
      : value(v)
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_time(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  double value;
};

/** For printing flexible_type.
 */
template <typename T>
struct table_printer_element
<T, typename std::enable_if<std::is_same<T, flexible_type>::value>::type >
    : public table_printer_element_base {
 public:
  static constexpr bool valid_type = true;

  table_printer_element(const T& v)
      : value(v)
  {}

  void print(std::ostringstream& ss, size_t width) {
    _print_flexible_type(ss, width, value);
  }

  flexible_type get_value() {
    return value;
  }

private:
  flexible_type value;
};


}}

#endif /* GRAPHLAB_TABLE_PRINTER_IMPL_H_ */
