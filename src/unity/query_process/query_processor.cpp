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
#ifndef GRAPHLAB_UNITY_QUERY_EXEC_CPP
#define GRAPHLAB_UNITY_QUERY_EXEC_CPP

#include <unity/query_process/query_processor.hpp>

/**
* This file implements functions required for process lazily evaluated query tree and get it ready for evaluation
**/
namespace graphlab {

template<typename T>
std::unique_ptr<parallel_iterator<T>> query_processor::start_exec(
  std::shared_ptr<lazy_eval_op_imp_base<T>> root, size_t dop) {

  size_t node_id = 1;
  query_processor::assign_node_ids(root, node_id);

  size_t next_pace_id = 1;
  size_t next_node_id = 1;
  std::map<std::tuple<size_t, size_t>, std::shared_ptr<lazy_eval_op_base>> object_dictionary;

  auto exec_tree = query_processor::smart_clone(root, (size_t)0, object_dictionary, next_pace_id, next_node_id);

  query_processor::clear_node_ids(root);

  // create iterators on the cloned tree
  auto exec_tree_ptr = std::dynamic_pointer_cast<lazy_eval_op_imp_base<T>>(exec_tree);
  return parallel_iterator<T>::create(exec_tree_ptr, dop);
}

// Force instantiate above template function
template
std::unique_ptr<parallel_iterator<flexible_type>> query_processor::start_exec<flexible_type>(
  std::shared_ptr<lazy_eval_op_imp_base<flexible_type>> root, size_t dop);
template
std::unique_ptr<parallel_iterator<std::vector<flexible_type>>> query_processor::start_exec<std::vector<flexible_type>>(
  std::shared_ptr<lazy_eval_op_imp_base<std::vector<flexible_type>>> root, size_t dop);

/**
* This function supports cloning a query definition tree efficiently and create a execution tree
* By smart, we mean we try as much as possible to share a node output among multiple consumers
* if those consumers consume the output of the node in exactly the same pace.
* An example of this -- sa1 is consumed by sa2 and sa3 and eventualy consumed by sa4
*             sa1 = ...
*             sa2 = sa1 > 10
*             sa3 = sa1 < 100
*             sa4 = sa2 & sa3
* An example of not sharable case is the following -- sa1 is consumed at different pace in sa4 than a2
*             sa1 = ...
*             sa2 = sa1 > 10
*             sa4 = sa1[sa2]
*
* The implementation of this function:
*   We use a object dictionary to remember the newly created nodes, the node is indexed by a tuple:
*   <pace_id, old_node_id>, the pace_id indicates the pace parent is going to consume this node. The pace_id
*   starts with 1, and will be passed along if a given node is not going to change pace (transform, for example),
*   a new pace_id is created if a node is going to change pace (filter, for example). For a node that hase
*   multiple children, the children may be consumed in different paces (union, for example), so different pace_id
*   may be used for children of the same node.
**/
std::shared_ptr<lazy_eval_op_base> query_processor::smart_clone(
  std::shared_ptr<lazy_eval_op_base> root,
  size_t pace_id,
  std::map<std::tuple<size_t, size_t>, std::shared_ptr<lazy_eval_op_base>>& object_dictionary,
  size_t& next_pace_id,
  size_t& next_node_id) {

  // Comput current node's index
  std::tuple<size_t, size_t> tuple = std::make_tuple(pace_id, root->get_node_id());

  // A new node with the same pace is already created, simply return
  if (object_dictionary.find(tuple) != object_dictionary.end()) {
    return object_dictionary[tuple];
  }

  // Create a new node and add to dictionary
  std::shared_ptr<lazy_eval_op_base> my_clone = root->clone();
  my_clone->set_node_id(next_node_id++);
  object_dictionary[tuple] = my_clone;

  // Now clone children
  auto children = root->get_children();
  if (children.size() > 0) {
    std::vector<std::shared_ptr<lazy_eval_op_base>> new_children;

    size_t next_child_pace_id = root->is_pace_changing() ? next_pace_id++ : pace_id;

    for(const auto& child : children) {
      if (!root->is_children_same_pace()) {
        next_child_pace_id = next_pace_id++;
      }

      new_children.push_back(smart_clone(child, next_child_pace_id, object_dictionary, next_pace_id, next_node_id));
    }

    my_clone->set_children(new_children);
  }

  return my_clone;
}

// Assign node id to a node in the tree, this is used for preparing the tree for smart cloning
void query_processor::assign_node_ids(std::shared_ptr<lazy_eval_op_base> root, size_t& next_id) {

  if (root->get_node_id() > 0) { return; }

  root->set_node_id(next_id++);
  auto children = root->get_children();
  if (children.size() > 0) {

    for(const auto& child : children) {
      assign_node_ids(child, next_id);
    }
  }
}

// Clear node id for all nodes in the tree, called after the smart clone is done
void query_processor::clear_node_ids(std::shared_ptr<lazy_eval_op_base> root) {
  if (root->get_node_id() == 0) { return; }

  root->clear_node_id();
  auto children = root->get_children();
  if (children.size() > 0) {

    for(const auto& child : children) {
      clear_node_ids(child);
    }
  }
}

void query_processor::print_tree(std::shared_ptr<lazy_eval_op_base> root, std::ostream& out) {
  out << "query_tree G {\n";
  print_node(root, out);
  out << "}\n";
}

void query_processor::print_node(std::shared_ptr<lazy_eval_op_base> root, std::ostream& out) {
     out << "\t\"" << root->get_node_id() << "\" ";
     // output the label
     out << "[label=\"" << root->get_name() << "\"";
     out << "]\n";

    auto children = root->get_children();
    if (children.size() > 0) {

      for(const auto& child : children) {
        out << "\t\"" << root->get_node_id() << "\" -> "
            << "\"" << child->get_node_id() << "\"\n";
      }

      for(const auto& child : children) {
        query_processor::print_node(child, out);
      }
    }
}

}

#endif