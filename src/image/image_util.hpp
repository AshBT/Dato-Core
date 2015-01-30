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
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/toolkit_function_specification.hpp>

namespace graphlab{

namespace image_util{

/**
* Return the head of passed sarray, but cast to string. Used for printing on python side. 
*/
std::shared_ptr<unity_sarray> _head_str(std::shared_ptr<unity_sarray> image_sarray, size_t num_rows);

/**
* Return flex_vec flexible type that is sum of all images with data in vector form. 
*/

flexible_type sum(std::shared_ptr<unity_sarray> unity_data);

/**
 * Generate image of mean pixel values of images in a unity sarray
 */
flexible_type generate_mean(std::shared_ptr<unity_sarray> unity_data);

/**
 * Construct an sframe of flex_images, with url pointing to directory where images reside. 
 */
std::shared_ptr<unity_sframe> load_images(std::string url, std::string format,
    bool with_path, bool recursive, bool ignore_failure, bool random_order);

/**
 * Construct a single image from url, and format hint.
 */
flexible_type load_image(const std::string& url, const std::string format);

/**
 * Decode the image data inplace
 */
void decode_image_inplace(image_type& data);

/**
 * Decode the image into raw pixels
 */
flexible_type decode_image(flexible_type data);

/**
 * Decode an sarray of flex_images into raw pixels
 */
std::shared_ptr<unity_sarray> decode_image_sarray(
    std::shared_ptr<unity_sarray> image_sarray);

/** Reisze an sarray of flex_images with the new size.
 */
flexible_type resize_image(flexible_type image, size_t resized_width, 
    size_t resized_height, size_t resized_channel);

/** Resize an sarray of flex_image with the new size.
 */
std::shared_ptr<unity_sarray> resize_image_sarray(
    std::shared_ptr<unity_sarray> image_sarray, size_t resized_width, 
    size_t resized_height, size_t resized_channels);

/** Convert sarray of image data to sarray of vector
 */
std::shared_ptr<unity_sarray>
  image_sarray_to_vector_sarray(std::shared_ptr<unity_sarray> image_sarray,
      bool undefined_on_failure);

/** Convert sarray of vector to sarray of image 
 */
std::shared_ptr<unity_sarray>
  vector_sarray_to_image_sarray(std::shared_ptr<unity_sarray> image_sarray,
      size_t width, size_t height, size_t channels, bool undefined_on_failure);


std::vector<toolkit_function_specification>
  get_toolkit_function_registration();


} // end of image_util
} // end of graphlab
