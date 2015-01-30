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
#include <functional>
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/unity_sarray_binary_operations.hpp>
namespace graphlab {
namespace unity_sarray_binary_operations {
void check_operation_feasibility(flex_type_enum left,
                                 flex_type_enum right,
                                 std::string op) {
  bool operation_is_feasible = false;

  if (left == flex_type_enum::VECTOR || right == flex_type_enum::VECTOR) {
    // special handling for vectors
    // we can perform every numeric op against numbers
    if (left == flex_type_enum::VECTOR || left == flex_type_enum::INTEGER || left == flex_type_enum::FLOAT) {
      if (right == flex_type_enum::VECTOR || right == flex_type_enum::INTEGER || right == flex_type_enum::FLOAT) {
          operation_is_feasible = true;
      }
    }
  } else if (op == "+" || op == "-" || op == "*" || op == "/") {
    operation_is_feasible = flex_type_has_binary_op(left, right, op[0]);
  } else if (op == "<" || op == ">" || op == "<=" || op == ">=") {
    // the comparison operators are all compatible. we just need to check
    // the < operator
    operation_is_feasible = flex_type_has_binary_op(left, right, '<');
  } else if (op == "==" || op == "!=") {
    // equality comparison is always feasible
    operation_is_feasible = true;
  } else if (op == "&" || op == "|") {
    // boolean operations are always feasible
    operation_is_feasible = true;
  } else {
    log_and_throw("Invalid scalar operation");
  }

  if (!operation_is_feasible) {
    throw std::string("Unsupported type operation. cannot perform operation ") +
          op + " between " + 
          flex_type_enum_to_name(left) + " and " +
          flex_type_enum_to_name(right);
  }
}

flex_type_enum get_output_type(flex_type_enum left, 
                               flex_type_enum right,
                               std::string op) {
  if (left == flex_type_enum::VECTOR || right == flex_type_enum::VECTOR) {
    return flex_type_enum::VECTOR;
  } else if (op == "+" || op == "-" || op == "*") {
    if (left == flex_type_enum::INTEGER && right == flex_type_enum::FLOAT) {
      // operations against float always returns float
      return flex_type_enum::FLOAT;
    } else {
      // otherwise we take the type on the left hand side
      return left;
    }
  } else if (op == "/") {
    // division always returns float
    // unless one of them are a vector
    return flex_type_enum::FLOAT;
  } else if (op == "<" || op == ">" || op == "<=" || 
             op == ">=" || op == "==" || op == "!=") {
    // comparison always returns integer
    return flex_type_enum::INTEGER;
  } else if (op == "&" || op == "|") {
    // boolean operations always returns integer
    return flex_type_enum::INTEGER;
  } else {
    throw std::string("Invalid Operation Type");
  }
}


std::function<flexible_type(const flexible_type&, const flexible_type&)> 
get_binary_operator(flex_type_enum left, flex_type_enum right, std::string op) {
/**************************************************************************/
/*                                                                        */
/*                               Operator +                               */
/*                                                                        */
/**************************************************************************/
  if (op == "+") {
    if (left == flex_type_enum::INTEGER && right == flex_type_enum::FLOAT) {
      // integer + float is float
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{
        return (flex_float)l + (flex_float)r;
      };
    } else if (left == flex_type_enum::VECTOR && right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{
       if (l.size() != r.size()) return FLEX_UNDEFINED;
       return l + r;
     };
    } else if (left == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return l + r; };
    } else if (right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return r + l; };
    } else {
      // everything else, left hand side is good
      // int + int -> int
      // float + int -> float
      // float + float -> float
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{return l + r;};
    }
/**************************************************************************/
/*                                                                        */
/*                               Operator -                               */
/*                                                                        */
/**************************************************************************/
  } else if (op == "-") {
    if (left == flex_type_enum::INTEGER && right == flex_type_enum::FLOAT) {
      // integer - float is float
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{
        return (flex_float)l - (flex_float)r;
      };
    } else if (left == flex_type_enum::VECTOR && right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{
       if (l.size() != r.size()) return FLEX_UNDEFINED;
       return l - r;
     };
    } else if (left == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return l - r; };
    } else if (right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return -r + l; };
    } else {
      // everything else, left hand side is good
      // int - int -> int
      // float - int -> float
      // float - float -> float
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{return l - r;};
    }
/**************************************************************************/
/*                                                                        */
/*                               Operator *                               */
/*                                                                        */
/**************************************************************************/
  } else if (op == "*") {
    if (left == flex_type_enum::INTEGER && right == flex_type_enum::FLOAT) {
      // integer * float is float
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{
        return (flex_float)l * (flex_float)r;
      };
    } else if (left == flex_type_enum::VECTOR && right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{
       if (l.size() != r.size()) return FLEX_UNDEFINED;
       return l * r;
     };
    } else if (left == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return l * r; };
    } else if (right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return r * l; };
    } else {
      // everything else, left hand side is good
      // int * int -> int
      // float * int -> float
      // float * float -> float
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return l * r;};
    }
/**************************************************************************/
/*                                                                        */
/*                               Operator /                               */
/*                                                                        */
/**************************************************************************/
  } else if (op == "/") {

    if (left == flex_type_enum::VECTOR && right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{
       if (l.size() != r.size()) return FLEX_UNDEFINED;
       return l / r;
     };
    } else if (left == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{ return l / r; };
    } else if (right == flex_type_enum::VECTOR) {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{
       flexible_type ret = r;
       for (size_t i = 0;i < ret.size(); ++i) ret[i] = l / ret[i];
       return ret;
     };
    } else {
      // divide always returns floats
      return [](const flexible_type& l, const flexible_type& r)->flexible_type{
        return (flex_float)l / (flex_float)r;
      };
    }
/**************************************************************************/
/*                                                                        */
/*                          Comparison Operators                          */
/*                                                                        */
/**************************************************************************/
  } else if (op == "<") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l < r);};
  } else if (op == ">") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l > r);};
  } else if (op == "<=") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l <= r);};
  } else if (op == ">=") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l >= r);};
  } else if (op == "==") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l == r);};
  } else if (op == "!=") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)(l != r);};
  } else if (op == "&") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)((!l.is_zero()) && (!r.is_zero()));};
  } else if (op == "|") {
     return [](const flexible_type& l, const flexible_type& r)->flexible_type{return (int)((!l.is_zero()) || (!r.is_zero()));};
  } else {
    throw std::string("Invalid Operation Type");
  }
}



} // namespace unity_sarray_binary_operations
} // namespace graphlab
