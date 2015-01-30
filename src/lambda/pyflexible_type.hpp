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
#ifndef GRAPHLAB_LAMBDA_PYFLEXIBLE_TYPE_HPP
#define GRAPHLAB_LAMBDA_PYFLEXIBLE_TYPE_HPP

#include <boost/python.hpp>
#include <typeinfo>
#include <flexible_type/flexible_type.hpp>
#include <cppipc/common/message_types.hpp>
#include <lambda/python_api.hpp>
#include <datetime.h>

namespace graphlab {

namespace lambda {

  /**
   * \ingroup lambda
   *
   * Provide functions that convert between python object
   * and flexible_types.
   *
   * Supported flexible_types are: flex_int, flex_float, flex_string, flex_vec, flex_dict
   * and flex_undefined.
   *
   * To convert from flexible_type to a python object:
   * \code
   * python::object py_int_val = PyObject_FromFlex(3);
   * python::object py_float_val = PyObject_FromFlex(3.0);
   * python::object py_str_val = PyObject_FromFlex("foo");
   * python::object py_vec_val = PyObject_FromFlex(array.array('i', [1,2,3]));
   * python::object py_recursive_val = PyObject_FromFlex([1,2,3]);
   * python::object py_dict_val = PyObject_FromFlex([{'a':1}, {1: 1.0}]);
   * python::object py_none = PyObject_FromFlex(flex_undefined());
   * \endcode
   *
   * To convert from python object to flexible_type:
   * \code
   * flexible_type int_flex_val = PyObject_AsFlex(py_int_val);
   * flexible_type float_flex_val = PyObject_AsFlex(py_float_val);
   * flexible_type str_flex_val = PyObject_AsFlex(py_str_val);
   * flexible_type vec_flex_val = PyObject_AsFlex(py_vec_val);
   * flexible_type recursive_flex_val = PyObject_AsFlex(py_vec_val);
   * flexible_type dict_flex_val = PyObject_AsFlex(py_dict_val);
   * flexible_type undef_flex_val = PyObject_AsFlex(py_none);
   * \endcode
   *
   * To convert from a list of flexible_type, use \ref PyObject_FromFlexList().
   */

  namespace python = boost::python;

  extern python::object image_class;

  python::object PyObject_FromFlex(const flexible_type& flex_value);

    /**
     * Converting basic flexible types into boost python object.
     *
     * Throws cppipc::bad_cast on type failure.
     */
    struct PyObjectVisitor {

      inline python::object operator()(const flex_int& i) const {
        return python::object(python::handle<>(PyLong_FromLong(i)));
      }

      inline python::object operator()(const flex_float& i) const {
        return python::object(python::handle<>(PyFloat_FromDouble(i)));
      }

      inline python::object operator()(const flex_string& i) const {
        return python::object(python::handle<>(PyString_FromString(i.c_str())));
      }
      inline python::object operator()(const flex_date_time& i) const {
        python::object utc = python::import("datetime").attr("datetime").attr("utcfromtimestamp")(python::object(python::handle<>(PyLong_FromLong(i.first))));
        python::object GMT = python::import("graphlab_util.timezone").attr("GMT");
        python::object to_zone = GMT(python::object(python::handle<>(PyFloat_FromDouble(i.second/2.0))));
        boost::python::dict kwargs;
        kwargs["tzinfo"] = GMT(python::object(python::handle<>(PyFloat_FromDouble(0))));
        boost::python::list emptyList;
        utc = utc.attr("replace")(*python::tuple(emptyList),**kwargs);
        return utc.attr("astimezone")(to_zone);
      } 
      inline python::object operator()(const flex_vec& i) const {

        // construct an array object
        python::object array = python::import("array").attr("array")(python::object("f"));

        for (const auto& v: i) {
          array.attr("append")((float)v);
        }
        return array;
      }

      inline python::object operator()(const flex_list& i) const {
        python::list l;
        for (const auto& v: i) l.append(lambda::PyObject_FromFlex(v));
        return l;
      }

      inline python::object operator()(const flex_dict& i) const {
        python::dict d;
        for (const auto& v: i) {
          d[lambda::PyObject_FromFlex(v.first)] = lambda::PyObject_FromFlex(v.second);
        }
        return d;
      }

      inline python::object operator()(const flex_undefined& i) const {
        return python::object();
      }

      inline python::object operator()(const flexible_type& t) const {
        throw cppipc::bad_cast("Cannot convert flexible_type " +
                            std::string(flex_type_enum_to_name(t.get_type())) +
                            " to python object.");
      }

      inline python::object operator()(const flex_image& i) const {

        const unsigned char* c_image_data = i.get_image_data();

        if (c_image_data == NULL){
            logstream(LOG_WARNING) << "Trying to apply lambda to flex_image with NULL data pointer" << std::endl;
        }
        PyObject* bytearray_ptr = PyByteArray_FromStringAndSize((const char*)c_image_data,i.m_image_data_size);

        boost::python::list arguments;

        boost::python::dict kwargs;
        kwargs["_image_data"] =  boost::python::handle<>(bytearray_ptr);
        kwargs["_height"] = i.m_height;
        kwargs["_width"] = i.m_width;
        kwargs["_channels"] = i.m_channels;
        kwargs["_image_data_size"] = i.m_image_data_size;
        kwargs["_version"] = static_cast<int>(i.m_version);
        kwargs["_format_enum"] = static_cast<int>(i.m_format);


        python::object img = image_class(*python::tuple(arguments), **kwargs);
        return img;
      }

      template<typename T>
      inline python::object operator()(const T& t) const {
        throw cppipc::bad_cast("Cannot convert non-flexible_type to python object.");
      }
    };

    /**
     * \ingroup lambda
     *
     * Convert basic flexible types into boost python object.
     * Throws exception on failure.
     */
    inline python::object PyObject_FromFlex(const flexible_type& flex_value) {
      return flex_value.apply_visitor(PyObjectVisitor());
    }

    /**
     * Update the given dictionary with given key and value vectors.
     */
    inline python::dict& PyDict_UpdateFromFlex(python::dict& d,
                                               const std::vector<std::string>& keys,
                                               const std::vector<graphlab::flexible_type>& values,
                                               bool erase_existing_keys = true) {
      DASSERT_EQ(keys.size(), values.size());
      if (erase_existing_keys)
        d.clear();
      for (size_t i = 0; i < values.size(); ++i) {
        d[keys[i]] = PyObject_FromFlex(values[i]);
      }
      return d;
    }

    /**
     * Update the given list with the value vectors. Assuming the length are same.
     */
    inline python::list& PyList_UpdateFromFlex(python::list& ls,
                                               const std::vector<graphlab::flexible_type>& values) {
      if (values.size() == 0) {
        return ls;
      }

      DASSERT_EQ(values.size(), python::len(ls));
      for (size_t i = 0; i < values.size(); ++i) {
        ls[i] = PyObject_FromFlex(values[i]);
      }
      return ls;
    }

    /*
     * \ingroup lambda
     *
     * Extract flexible types from boost python objects.
     * Throws exception on failure.
     */
    inline flexible_type PyObject_AsFlex(const python::object& object) {
      flexible_type ret;
      PyDateTime_IMPORT; //we need to import this macro for PyDateTime_Check()
      PyObject* objectptr = object.ptr();
      if (PyInt_Check(objectptr) || PyLong_Check(objectptr)) {
        flex_int x = python::extract<flex_int>(object);
        ret = x;
      } else if (PyFloat_Check(objectptr)) {
        flex_float x = python::extract<flex_float>(object);
        ret = x;
      } else if (PyString_Check(objectptr)) {
        flex_string x = python::extract<flex_string>(object);
        ret = x;
      } else if (PyUnicode_Check(objectptr)) {
        flex_string x = python::extract<flex_string>(object.attr("encode")("utf-8"));
        ret = x;
      } else if (PyDateTime_Check(objectptr)) { 
        if(PyDateTime_GET_YEAR(objectptr) < 1400 or PyDateTime_GET_YEAR(objectptr) > 10000) { 
             throw("Year is out of valid range: 1400..10000");
        }
        if(object.attr("tzinfo") != python::object()) {
             //store timezone offset at the granularity of half an hour 
             int offset = (int)python::extract<flex_float>(object.attr("tzinfo").attr("utcoffset")(object).attr("total_seconds")()) / 1800;
             ret = flex_date_time(python::extract<long>(python::import("calendar").attr("timegm")(object.attr("utctimetuple")())),offset);
        } else {
          ret = flex_date_time(python::extract<long>(python::import("calendar").attr("timegm")(object.attr("utctimetuple")())),0);
        }
      } else if (PyTuple_Check(objectptr)) { 
        python::tuple tuple_val(object);
          
        flex_list ret_recursive;
        for(size_t i = 0; i < (size_t)len(tuple_val); i++) {
          ret_recursive.push_back(PyObject_AsFlex(tuple_val[i]));
        }
        ret = ret_recursive;
         
      } else if (PyDict_Check(objectptr)) {
        python::dict dict_val(object);
        python::list keys = dict_val.keys();
        flex_dict x;
        x.reserve(len(keys));
        for(size_t i = 0; i < (size_t)len(keys); i++) {
          x.push_back(std::make_pair(
            PyObject_AsFlex(keys[i]),
            PyObject_AsFlex(dict_val[keys[i]])
          ));
        }

        ret = x;
      } else if (PyObject_HasAttrString(objectptr, "_image_data")) {
        flex_image img;
        int format_enum;
        img.m_image_data_size = python::extract<size_t>(object.attr("_image_data_size"));
        if (img.m_image_data_size > 0){
          python::object o = object.attr("_image_data");         
          char* buf = new char[img.m_image_data_size];
          memcpy(buf, PyByteArray_AsString(o.ptr()), img.m_image_data_size);
          img.m_image_data.reset(buf);
        }
        img.m_height = python::extract<size_t>(object.attr("_height"));
        img.m_width = python::extract<size_t>(object.attr("_width"));
        img.m_channels = python::extract<size_t>(object.attr("_channels"));
        img.m_version = python::extract<int>(object.attr("_version"));
        format_enum = python::extract<int>(object.attr("_format_enum"));
        img.m_format = static_cast<Format>(format_enum);
        ret = img;
      } else if (PyObject_HasAttrString(objectptr, "tolist")) {
        // array.array type
        // this is not the most efficient way, but its should be the most robust.
        ret = PyObject_AsFlex(object.attr("tolist")());
      } else if (PyList_Check(objectptr)) {
        // We try hard to convert return list to vector(numeric), only when we encounter the first
        // non-numeric value, would we convert the return value to recursive type.

        size_t list_size = PyList_Size(objectptr);

        bool all_numeric = true;
        flex_list ret_recursive;
        flex_vec ret_vector;

        for (size_t i = 0; i < list_size; ++i) {
          PyObject* a = PyList_GetItem(objectptr, i);
          if (a == NULL) {
            throw cppipc::bad_cast("Unable to read resultant Python list");
          }

          // first time encounter non-numeric value, move from flex_vec to flex_list
          if (all_numeric && !PyNumber_Check(a)) {
            all_numeric = false;

            // move all items from ret_vector to ret_recursive
            for (size_t j = 0; j < ret_vector.size(); ++j) {
              PyObject* ret_vector_read_obj = PyList_GetItem(objectptr, j); 
              ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(ret_vector_read_obj)))));
            }
            ret_vector.clear();

          }

          if (all_numeric) {
            ret_vector.push_back((flex_float)PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
          } else {
            ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
          }
        }

        if (all_numeric) {
          ret = ret_vector;
        } else {
          ret = ret_recursive;
        }
      } else if (objectptr == python::object().ptr()) {
        ret = flexible_type(flex_type_enum::UNDEFINED);
      } else {
        std::string tname = python::extract<std::string>(object.attr("__class__").attr("__name__"));
        throw cppipc::bad_cast("Cannot convert python object " + tname + " to flexible_type.");
      }
      return ret;
    }

    /*
     * \ingroup lambda
     *
     * Convert vector of flexible types into boost python list object.
     * Throws exception on failure.
     */
    inline python::object PyObject_FromFlexList(const std::vector<flexible_type>& flex_list) {
      python::list l;
      for (const auto& v: flex_list) {
        l.append(PyObject_FromFlex(v));
      }
      return l;
    }
}
}

// workaround python bug http://bugs.python.org/issue10910
#undef isspace
#undef isupper
#undef toupper
#endif
