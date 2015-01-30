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
#ifndef UNITY_SGRAPH_LAZY_OPS_HPP
#define UNITY_SGRAPH_LAZY_OPS_HPP
#include <sgraph/sgraph.hpp>
#include<unity/lib/unity_sgraph.hpp>

/**************************************************************************/
/*                                                                        */
/*                     Lazy operators on unity_sgraph                     */
/*                                                                        */
/**************************************************************************/

namespace graphlab {

  typedef lazy_eval_operation_base<sgraph> operator_type;

  template<typename DataType>
  struct add_vertices_op: operator_type {
    add_vertices_op(std::shared_ptr<DataType> data,
                    const std::string& id_field_name,
                    size_t group=0):
        data(data), id_field_name(id_field_name), group(group) { } 

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.add_vertices(*data, id_field_name);
    }

    std::shared_ptr<DataType> data;
    const std::string id_field_name;
    const size_t group;
  };

  template<typename DataType>
  struct add_edges_op: operator_type {
    add_edges_op(std::shared_ptr<DataType> data,
                 const std::string& source_field_name,
                 const std::string& target_field_name,
                 size_t groupa = 0, size_t groupb = 0):
        data(data),
        source_field_name(source_field_name),
        target_field_name(target_field_name),
        groupa(groupa), groupb(groupb) { }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.add_edges(*data, source_field_name, target_field_name, groupa, groupb);
    }
    std::shared_ptr<DataType> data;
    const std::string source_field_name;
    const std::string target_field_name;
    const size_t groupa, groupb;
  };

  struct copy_vertex_field_op: operator_type {
    copy_vertex_field_op(const std::string& field,
                         const std::string& new_field,
                         size_t group = 0):
      field(field), new_field(new_field), group(group){ }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.copy_vertex_field(field, new_field, group);
    }
    const std::string field;
    const std::string new_field;
    const size_t group;
  };

  struct copy_edge_field_op: operator_type {
    copy_edge_field_op(const std::string& field,
                       const std::string& new_field,
                       size_t groupa = 0, size_t groupb = 0):
      field(field), new_field(new_field), groupa(groupa), groupb(groupb){ }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.copy_edge_field(field, new_field, groupa, groupb);
    }
    const std::string field;
    const std::string new_field;
    const size_t groupa, groupb;
  };

  struct delete_vertex_field_op: operator_type {
    delete_vertex_field_op(const std::string& field, size_t group = 0):
      field(field), group(group) { }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.remove_vertex_field(field, group);
    }
    const std::string field;
    const size_t group;
  };

  struct delete_edge_field_op: operator_type {
    delete_edge_field_op(const std::string& field, size_t groupa = 0, size_t groupb = 0):
      field(field), groupa(groupa), groupb(groupb) { }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.remove_edge_field(field, groupa, groupb);
    }
    const std::string field;
    const size_t groupa, groupb;
  };

  struct select_vertex_fields_op: operator_type {
    select_vertex_fields_op(const std::vector<std::string>& _fields, size_t group) : 
        fields(_fields.begin(), _fields.end()), group(group) { 
          DASSERT_TRUE(fields.count(sgraph::VID_COLUMN_NAME) > 0);
        }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.select_vertex_fields(fields, group);
    }
    const std::set<std::string> fields;
    const size_t group;
  };

  struct select_edge_fields_op: operator_type {
    select_edge_fields_op(const std::vector<std::string>& _fields, size_t groupa, size_t groupb) :
        fields(_fields.begin(), _fields.end()), groupa(groupa), groupb(groupb) { 
          DASSERT_TRUE(fields.count(sgraph::SRC_COLUMN_NAME) > 0);
          DASSERT_TRUE(fields.count(sgraph::DST_COLUMN_NAME) > 0);
        }

    virtual size_t num_arguments() { return 1; }

    virtual void execute(sgraph& output,
                         const std::vector<sgraph*>& parents) {
      output.select_edge_fields(fields, groupa, groupb);
    }
    const std::set<std::string> fields;
    const size_t groupa, groupb;
  };
}

#endif
