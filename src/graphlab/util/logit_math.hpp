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
#include <util/code_optimization.hpp>
#include <cmath>


namespace graphlab {


static inline double sigmoid(double x) GL_HOT_INLINE_FLATTEN;

/** Numerically stable version of 1 / (1 + exp(-x));
 * 
 *  If x < 0, then this is equal to
 *
 *  exp(- abs(x)) / (1 + exp(-abs(x))) = 1 / (1 + exp(x))
 *
 *  If x >=0, then this is equal to 1 / (1 + exp(-x)).
 *
 *  This separation into the positive and negative case is so that the
 *  code never uses the result of exp(x) where x is large and
 *  positive, which could easily result in overflow.
 */
static inline double sigmoid(double x) {
  double m_abs_x = std::copysign(x, -1.0);
  double exp_neg = std::exp(m_abs_x);
  bool is_negative = std::signbit(x);
  double numerator = is_negative ? exp_neg : 1.0;

  return numerator / (1.0 + exp_neg);
}

static inline double log1pe(double x)  GL_HOT_INLINE_FLATTEN;

/** Numerically stable version of log(1 + exp(x) )
 */
static inline double log1pe(double x)
{
  return __builtin_expect((x > 48), 0) ? x : std::log1p(exp(x));
}

static inline double log1pen(double x) GL_HOT_INLINE_FLATTEN;

/** Numerically stable version of log(1 + exp(-x) )
 */
static inline double log1pen(double x)
{
  return __builtin_expect((x < -48), 0) ? -x : std::log1p(exp(-x));
}

static inline double log1pe_deriviative(double x) GL_HOT_INLINE_FLATTEN;

/** Numerically stable version of
 *  d/dx (log(1 + exp(x) )) = 1 / (1 + exp(-x)) = sigmoid(x).
 */
static inline double log1pe_deriviative(double x)
{
  return sigmoid(x);
}

static inline double log1pen_deriviative(double x) GL_HOT_INLINE_FLATTEN;

/** Numerically stable version of
 *  d/dx (log(1 + exp(-x) )) =  -1 / (1 + exp(x)).
 */
static inline double log1pen_deriviative(double x)
{
  double m_abs_x = std::copysign(x, -1.0);
  double exp_neg = std::exp(m_abs_x);
  bool is_negative = std::signbit(x);
  double numerator = is_negative ? 1.0 : exp_neg;

  return -numerator / (1.0 + exp_neg);
}

template <typename T> static inline T sq(const T& t) { return t*t; }


}
