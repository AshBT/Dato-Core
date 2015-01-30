/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_SIMPLE_MODEL_HPP
#define GRAPHLAB_UNITY_SIMPLE_MODEL_HPP

#include <unity/lib/api/model_interface.hpp>
#include <unity/lib/api/client_base_types.hpp>
#include <unity/lib/variant.hpp>

namespace graphlab {
/**
 * \ingroup unity
 * The simple_model is the simplest implementation of the \ref model_base
 * object, containing just a map from string to variant and permitting
 * query operations on the map.
 */
class simple_model: public model_base {

 public:

  static constexpr size_t SIMPLE_MODEL_VERSION = 0;

  /// Default constructor
  simple_model() {}

  std::string name() { return "simple_model"; };

  /**
   * Constructs a simple_model from a variant map. 
   * A copy of the map is taken and stored.
   */
  explicit simple_model(const variant_map_type& params) : params(params) {}


  /// Lists all the keys stored in the variant map
  std::vector<std::string> list_keys();

  /** 
   * Gets the value of a key in the variant map. Throws an error if the key
   * is not found. opts is ignored.
   */
  variant_type get_value(std::string key, variant_map_type& opts);

  /**
   * Returns the current model version
   */
  size_t get_version() const;

  /**
   * Serializes the model. Must save the model to the file format version
   * matching that of get_version()
   */
  void save_impl(oarchive& oarc) const;

  /**
   * Loads a model previously saved at a particular version number.
   * Should raise an exception on failure.
   */
  void load_version(iarchive& iarc, size_t version); 

  /// Destructor
  ~simple_model();

  /// Internal map
  variant_map_type params;

};
}

#endif // GRAPHLAB_UNITY_SIMPLE_MODEL
