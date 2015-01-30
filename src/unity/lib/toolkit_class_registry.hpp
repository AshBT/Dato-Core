/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_TOOLKIT_CLASS_REGISTRY_HPP
#define GRAPHLAB_UNITY_TOOLKIT_CLASS_REGISTRY_HPP
#include <unity/lib/toolkit_class_specification.hpp>

namespace graphlab {
class model_base;
/**
 * \ingroup unity
 * Defines a collection of models. Has the ability to add/register new
 * toolkits, and get information about the model.
 */
class toolkit_class_registry {
 public:
  /**
   * Register a model (name, constructor) pair.
   *
   * Returns false if the model name already exists.
   *
   * The optional "description" argument describing the model.
   * The following keys are recognized.
   *  - "functions": A dictionary with key: function name, and value,
   *                     a list of input parameters.
   *  - "get_properties": The list of all readable properties of the model
   *  - "set_properties": The list of all writable properties of the model
   */
  typedef std::map<std::string, flexible_type> toolkit_class_description_type;

  /**
   * Registers a toolkit class.
   */
  bool register_toolkit_class(const std::string& class_name,
                      std::function<model_base*()> constructor,
                      toolkit_class_description_type description = toolkit_class_description_type());

  /**
   * Registers a toolkit class.
   */
  bool register_toolkit_class(std::vector<toolkit_class_specification> classes, 
                              std::string prefix = "");

  /**
   * Creates a new model object with the given model_name.
   *
   * Throws an exception if the model_name was not registered.
   */
  std::shared_ptr<model_base> get_toolkit_class(const std::string& class_name);

  /**
   * Returns the description associated with the model.
   *
   * Throws an exception if the model_name was not registered.
   */
  std::map<std::string, flexible_type> get_toolkit_class_description(const std::string& toolkit_class_name);
  /**
   * Returns a list of names of all registered models.
   *
   * \returns A vector of string where each string is a model name.
   */
  std::vector<std::string> available_toolkit_classes();

 private:
  std::map<std::string, std::function<model_base*()> > registry;
  std::map<std::string, std::map<std::string, flexible_type> > descriptions;
};

} // graphlab

#endif // GRAPHLAB_UNITY_toolkit_class_registry
