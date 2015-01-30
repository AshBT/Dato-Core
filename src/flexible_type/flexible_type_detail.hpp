/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_DETAIL_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_DETAIL_HPP
#include <typeindex>
#include <sstream>
#include <cmath>
#include <string>
#include <logger/assertions.hpp>
#include <util/stl_util.hpp>
#include <util/cityhash_gl.hpp>
#include <serialization/serialization_includes.hpp>
#include <flexible_type/flexible_type_base_types.hpp>
namespace boost { namespace posix_time { 
class ptime;
} }

namespace graphlab {

namespace flexible_type_impl {

/**
 * custom implementation of my_from_time_t that handles  
 * time_t's that does not fit in int32_t integers.
 */
boost::posix_time::ptime my_from_time_t(std::time_t offset);

/**
 * Function that takes a ptime argument and generates _
 * its corresponding time_t. 
 */
flex_int my_to_time_t(const boost::posix_time::ptime & time);

/**
 * \ingroup unity
 * \internal
 * Used to issue a static_assert only if instantiated.
 */
template <bool b>
struct invalid_type_instantiation_assert {
  static_assert(b, "Invalid Type");
};


/**
 * \ingroup unity
 * \internal
 * A wrapper to convert a binary visitor into a regular
 * unary apply_visitor.
 */
template <typename Visitor, typename U>
struct const_visitor_wrapper {
  const Visitor& v;
  const U& u;

  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN auto operator()(T& t) const -> decltype(v(t, u)) {
    return v(t, u);
  }
};


/**************************************************************************/
/*                                                                        */
/*       Visitors used throughout the flexible type implementation        */
/*                                                                        */
/**************************************************************************/


/**
 * \ingroup unity
 * \internal
 * Handles less than comparison between flexible types
 */
struct lt_operator {
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); return false; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_float u) const { return t < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_int u) const { return t < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_int u) const { return t.first < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_date_time u) const { return t < u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_date_time u) const { return t.first < u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_float u) const { return t < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_int u) const { return t < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const std::string& t, const std::string& u) const { return t < u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_vec& t, const flex_vec& u) const {
    // [1,2,3] < [1,2,3,4] true
    // [1,2,3,4] < [1,2,3] false
    // [1,2,3,4] < [1,2,3,4] false
    for(size_t i = 0; i < t.size(); i++) {
      if (u.size() <= i || t[i] > u[i]) return false;
      if (t[i] < u[i]) return true;
    }
    return t.size() < u.size();
  }

  inline bool operator()(const flex_list& t, const flex_list& u) const {
    for(size_t i = 0; i < t.size(); i++) {
      if (u.size() <= i || t[i] > u[i]) return false;
      if (t[i] < u[i]) return true;
    }
    return t.size() < u.size();
  }
};



/**
 * \ingroup unity
 * \internal
 * Handles greater than comparison between flexible types
 */
struct gt_operator {
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); return false; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_float u) const { return t > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_int u) const { return t.first > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_date_time u) const { return t > u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_date_time u) const { return t.first > u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_int u) const { return t > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_float u) const { return t > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_int u) const { return t > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const std::string& t, const std::string& u) const { return t > u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_vec& t, const flex_vec& u) const {
    for(size_t i = 0; i < t.size(); i++) {
      if (u.size() <= i || t[i] > u[i]) return true;
      if (t[i] < u[i]) return false;
    }

    return t.size() > u.size();
  }

  inline bool operator()(const flex_list& t, const flex_list& u) const {
    for(size_t i = 0; i < t.size(); i++) {
      if (u.size() <= i || t[i] > u[i]) return true;
      if (t[i] < u[i]) return false;
    }
    return t.size() > u.size();
  }
};



/**
 * \ingroup unity
 * \internal
 * Handles equality comparison between flexible types.
 * They must be identical for the comparison to return true;
 */
struct equality_operator {
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(T& t, const U& u) const { return false; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_date_time u) const { return t.first == u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_int u) const { return t.first == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_date_time u) const { return t == u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_int u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_float u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_string& t, const flex_string& u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_vec& t, const flex_vec& u) const {
    if (t.size() != u.size()) return false;
    for (size_t i = 0;i < t.size(); ++i) if (t[i] != u[i]) return false;
    return true;
  }
  // Just use operator== which calls the approx_equality_operator
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_dict& t, const flex_dict& u) const {
    return t == u;
  };
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_list& t, const flex_list& u) const {
    return t == u;
  };

  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(flex_undefined t, flex_undefined u) const {
    return true;
  };
};


/**
 * \ingroup unity
 * \internal
 * A looser version of the equality operator which allows checking between
 * for equality between integer and floating point types
 */
struct approx_equality_operator {
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(T& t, const U& u) const { return false; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_undefined t, const flex_undefined u) const { return true; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_date_time u) const { return t.first == u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_date_time t, const flex_int u) const { return t.first == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_date_time u) const { return t == u.first; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_int u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_float u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_int t, const flex_float u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_float t, const flex_int u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_string& t, const flex_string& u) const { return t == u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN bool operator()(const flex_vec& t, const flex_vec& u) const {
    if (t.size() != u.size()) return false;
    for (size_t i = 0;i < t.size(); ++i) if (t[i] != u[i]) return false;
    return true;
  }
  // Implemented in flexible_type.cpp
  bool operator()(const flex_dict& t, const flex_dict& u) const;
  bool operator()(const flex_list& t, const flex_list& u) const;
};


/**
 * \ingroup unity
 * \internal
 * Handles negation of a flexible type
 */
struct negation_operator{
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t) const {
    FLEX_TYPE_ASSERT(false);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t) const { t = -t; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t) const { t = -t; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] = -t[i];
  }
};



/**
 * \ingroup unity
 * \internal
 * Handles increment of a flexible type
 */
struct increment_operator {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t) const {
    FLEX_TYPE_ASSERT(false);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t) const { t++; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t) const { t++; }
};


/**
 * \ingroup unity
 * \internal
 * Handles decrement of a flexible type
 */
struct decrement_operator {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t) const {
    FLEX_TYPE_ASSERT(false);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t) const { t--; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t) const { t--; }
};



/**
 * \ingroup unity
 * \internal
 * Handles += between flexible types
 */
struct plus_equal_operator{
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int u) const { t += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& t, const flex_int u) const { t.first += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float u) const { t += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int u) const { t += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float u) const { t += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(std::string& t, const std::string& u) const { t += u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const {
    FLEX_TYPE_ASSERT(t.size() == u.size());
    for (size_t i = 0;i < t.size(); ++i) t[i] += u[i];
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_int u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] += u;
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_float u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] += u;
  }
};


/**
 * \ingroup unity
 * \internal
 * Handles -= between flexible types
 */
struct minus_equal_operator{
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& t, const flex_int u) const { t.first -= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int& u) const { t -= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float& u) const { t -= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int& u) const { t -= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float& u) const { t -= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const {
    FLEX_TYPE_ASSERT(t.size() == u.size());
    for (size_t i = 0;i < t.size(); ++i) t[i] -= u[i];
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_int u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] -= u;
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_float u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] -= u;
  }
};


/**
 * \ingroup unity
 * \internal
 * Handles /= between flexible types
 */
struct divide_equal_operator{
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int& u) const { t /= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float& u) const { t /= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int& u) const { t /= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float& u) const { t /= u; }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const {
    FLEX_TYPE_ASSERT(t.size() == u.size());
    for (size_t i = 0;i < t.size(); ++i) t[i] /= u[i];
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_int u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] /= u;
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_float u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] /= u;
  }
};



/**
 * \ingroup unity
 * \internal
 * Handles %= between flexible types
 */
struct mod_equal_operator{
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int& u) const { t %= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float& u) const { t %= (flex_int)u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int& u) const { t = fmod(t, u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float& u) const { t = fmod(t, u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const {
    FLEX_TYPE_ASSERT(t.size() == u.size());
    for (size_t i = 0;i < t.size(); ++i) t[i] = fmod(t[i], u[i]);
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_int u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] = fmod(t[i], u);
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_float u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] = fmod(t[i], u);
  }
};


/**
 * \ingroup unity
 * \internal
 * Handles *= between flexible types
 */
struct multiply_equal_operator{
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int& u) const { t *= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float& u) const { t *= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int& u) const { t *= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float& u) const { t *= u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const {
    FLEX_TYPE_ASSERT(t.size() == u.size());
    for (size_t i = 0;i < t.size(); ++i) t[i] *= u[i];
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_int u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] *= u;
  }

  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_float u) const {
    for (size_t i = 0;i < t.size(); ++i) t[i] *= u;
  }
};


/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_date_time format 
 */
struct get_datetime_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_date_time operator()(T t) const { FLEX_TYPE_ASSERT(false); return flex_date_time(0,0); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_date_time operator()(flex_undefined t) const { return flex_date_time(0,0); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_date_time operator()(flex_int i) const {return flex_date_time(i,0); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_date_time operator()(flex_date_time dt) const {return dt;}
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_date_time operator()(flex_float i) const {return flex_date_time((float)i,0); }
};


/**
 * \ingroup unity
 * \internal
 * Converts the stored value to an integer
 */
struct get_int_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(T t) const { FLEX_TYPE_ASSERT(false); return 0; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(flex_undefined t) const { return 0; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(flex_int i) const {return i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(flex_date_time dt) const { return dt.first;}
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(flex_float i) const {return i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_int operator()(const flex_string& t) const { return std::atol(t.c_str()); }
};

/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_float
 */
struct get_float_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(T t) const { FLEX_TYPE_ASSERT(false); return 0.0; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(flex_undefined t) const { return 0.0; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(flex_date_time dt) const { return dt.first;}
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(flex_int i) const {return i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(flex_float i) const {return i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_float operator()(const flex_string& t) const { return std::atof(t.c_str()); }
};



/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_string
 */
struct get_string_visitor {
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_string operator()(flex_undefined u) const { return flex_string(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_string operator()(flex_float i) const { return tostr(i); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_string operator()(flex_int i) const { return tostr(i); }
  //i.seond is stored at the granularity of half and hour.
  flex_string operator()(const flex_date_time& i) const;
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_string operator()(const flex_string& i) const { return i; }

  flex_string operator()(const flex_vec& vec) const;
  flex_string operator()(const flex_list& vec) const;
  flex_string operator()(const flex_dict& vec) const;
  flex_string operator()(const flex_image& vec) const;
};


/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_vec
 */
struct get_vec_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(T t) const { FLEX_TYPE_ASSERT(false); return flex_vec(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(flex_undefined t) const { return flex_vec(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(flex_int i) const {return flex_vec{(double)i}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(flex_float i) const {return flex_vec{i}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(const flex_vec& i) const {return i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_vec operator()(flex_date_time i) const {return flex_vec{get_float_visitor()(i)}; }

  flex_vec operator()(const flex_image& img) const;
};

/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_list
 */
struct get_recursive_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(T t) const { FLEX_TYPE_ASSERT(false); return flex_list(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(flex_undefined t) const { return flex_list(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(flex_date_time i) const {
    return flex_list{get_float_visitor()(i)}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(flex_int i) const {
    return flex_list{flexible_type(i)}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(flex_float i) const {
    return flex_list{flexible_type(i)}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(const flex_string& i) const {
    return flex_list{flexible_type(i)}; }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(const flex_vec& v) const {
    flex_list r(v.size());
    for(size_t i = 0; i < v.size(); ++i)
      r[i] = v[i];
    return r;
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_list operator()(const flex_list& v) const {
    return v;
  }
};

/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_dict
 */
struct get_dict_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_dict operator()(T t) const { FLEX_TYPE_ASSERT(false); return flex_dict(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_dict operator()(flex_undefined t) const { return flex_dict(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_dict operator()(const flex_dict& v) const { return v; }
};

/**
 * \ingroup unity
 * \internal
 * Converts the stored value to a flex_image
 */
struct get_img_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_image operator()(T t) const { FLEX_TYPE_ASSERT(false); return flex_image(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_image operator()(flex_undefined t) const { return flex_image(); }
  inline FLEX_ALWAYS_INLINE_FLATTEN flex_image operator()(const flex_image& v) const { return v; }

};

/**
 * \ingroup unity
 * \internal
 * Soft assignment between flexible types
 *
 * \note Changes to this should update \ref flex_type_is_convertible.
 */
struct soft_assignment_visitor {
  template <typename T, typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& t, const U& u) const { FLEX_TYPE_ASSERT(false); }
  
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& t, const flex_int u) const { t.first = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_date_time u) const { t = get_int_visitor()(u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_date_time u) const { t = get_float_visitor()(u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& t, const flex_float u) const { t.first = (int64_t)u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& t, const flex_date_time u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_string& t, const flex_date_time u) const { t = get_string_visitor()(u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_int u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_float u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_int& t, const flex_float u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_float& t, const flex_int u) const { t = u; }
  template <typename U>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_string& t, const U& u) const { t = get_string_visitor()(u); }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_vec& u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_list& t, const flex_list& u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_dict& t, const flex_dict& u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_undefined& t, const flex_undefined& u) const { t = u; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_vec& t, const flex_image& u) const { t = get_vec_visitor()(u); }
  // defined in flexible_type.cpp
  void operator()(flex_list& t, const flex_vec& u) const;
};

/**
 * \ingroup unity
 * \internal
 * Serializes the contents of the flexible type
 */
struct serializer {
  oarchive& oarc;
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(const T& i) const { oarc << i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(const flex_date_time& i) const { 
    oarc << *reinterpret_cast<const int64_t*>(&i);
  }
};


/**
 * \ingroup unity
 * \internal
 * Deserializes the contents of the flexible type
 */
struct deserializer {
  iarchive& iarc;
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(T& i) const { iarc >> i; }
  inline FLEX_ALWAYS_INLINE_FLATTEN void operator()(flex_date_time& i) const { 
    union {
      flex_date_time dt;
      int64_t val;
    } date_time_serialized;
    iarc >> date_time_serialized.val;
    i = date_time_serialized.dt;
  }
};



/**
 * \ingroup unity
 * \internal
 * Get the type index of the underlying contents
 */
struct get_type_index {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN std::type_index operator()(T& i) const { return typeid(i); }
};


// /**
//  * Computes a hash of the flexible type. Uses MD5
//  */
// struct md5_hash_visitor {
//   template <typename T>
//   size_t operator()(T t) const {
//     return 0;
//   }
//   size_t operator()(flex_int t) const {
//     return t;
//   }
//   size_t operator()(const flex_float& t) const {
//     return *reinterpret_cast<const size_t*>(&t);
//   }
//   size_t operator()(const flex_string& t) const {
//     uint64_t md5low, md5high;
//     mg_md5_buf(&md5low, &md5high, t.c_str(), t.length());
//     return md5low;
//   }
//   size_t operator()(const flex_vec& t) const {
//     uint64_t md5low, md5high;
//     mg_md5_buf(&md5low, &md5high,
//                reinterpret_cast<const char*>(t.data()),
//                t.size() * sizeof(flex_vec::value_type));
//     return md5low;
//   }
// };
//


/**
 * \ingroup unity
 * \internal
 * Computes a hash of the flexible type. Uses cityhash
 */
struct city_hash_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(T t) const {
    return 0;
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(flex_int t) const {
    return graphlab::hash64(t);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(flex_date_time t) const {
    return graphlab::hash64(get_int_visitor()(t));
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(const flex_float& t) const {
    return *reinterpret_cast<const size_t*>(&t);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(const flex_string& t) const {
    return graphlab::hash64(t);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN size_t operator()(const flex_vec& t) const {
    return graphlab::hash64(reinterpret_cast<const char*>(t.data()),
                            t.size() * sizeof(flex_vec::value_type));
  }
  // Implemented in flexible_type.cpp
  size_t operator()(const flex_list& t) const;
  size_t operator()(const flex_dict& t) const;
};

/**
 * \ingroup unity
 * \internal
 * Computes a hash of the flexible type. Uses MD5
 */
struct city_hash128_visitor {
  template <typename T>
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(T t) const {
    return 0;
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(flex_date_time t) const {
    return graphlab::hash128(long(get_int_visitor()(t)));
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(flex_int t) const {
    return graphlab::hash128(long(t));
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(const flex_float& t) const {
    return graphlab::hash128(reinterpret_cast<const char*>(&t), sizeof(double));
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(const flex_string& t) const {
    return graphlab::hash128(t);
  }
  inline FLEX_ALWAYS_INLINE_FLATTEN uint128_t operator()(const flex_vec& t) const {
    return graphlab::hash128(reinterpret_cast<const char*>(t.data()),
               t.size() * sizeof(flex_vec::value_type));
  }

  // Implemented in flexible_type.cpp
  uint128_t operator()(const flex_list& t) const;
  uint128_t operator()(const flex_dict& t) const;
};



} // flexible_type_impl

} // graphlab

#endif
