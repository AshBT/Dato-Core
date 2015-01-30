/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_IMAGE_IMAGE_TYPE_HPP
#define GRAPHLAB_IMAGE_IMAGE_TYPE_HPP

#include <string>
#include <serialization/serialization_includes.hpp>
#include <boost/shared_ptr.hpp>

const char IMAGE_TYPE_CURRENT_VERSION = 0;

namespace graphlab {

/**
 * \ingroup group_gl_flexible_type
 * Possible image formats stored in the image type
 */
enum class Format: size_t { 
  JPG = 0,  ///< JPEG Compressed
  PNG = 1,  ///< PNG Compressed
  RAW_ARRAY = 2,  ///< Not Compressed
  UNDEFINED = 3   ///< Unknown
};

/**
 * \ingroup group_gl_flexible_type
 * Image type, which is typedef'd to flex_image, part of the flexible_type union.
 * Holds image data and meta-data pertaining to image size and file type, but
 * does not hold meta-data like path or category. 
 */
class image_type {

public: 
  /// The image data, stored in the format indicated by m_format in a char array
  boost::shared_ptr<char[]> m_image_data;
  /// The height of the image 
  size_t m_height = 0;
  /// The width of the image*
  size_t m_width = 0; 
  /// The number of channels in the image: Grayscale = 1, RGB = 3, RGBA = 4.
  size_t m_channels = 0;
  /// Length of m_image_data char array
  size_t m_image_data_size = 0;
  /// Version of image_type object
  char m_version = IMAGE_TYPE_CURRENT_VERSION;
  /// Format of data, intitialized as UNDEFINED
  Format m_format = Format::UNDEFINED; 
  /// Constructor
  image_type() = default;
  /// Construct from existing data
  image_type(char* image_data, size_t height, size_t width, 
             size_t channels, size_t image_data_size, int version, int format);
  /// Check whether image is decoded
  inline bool is_decoded() const { return m_format == Format::RAW_ARRAY; }
  /// Serialization
  void save(oarchive& oarc) const;
  /// Deserialization
  void load(iarchive& iarc);
  /// Returns a char* pointer to the raw image data
  const unsigned char* get_image_data() const;
};




} // end of namespace graphlab 

#endif
