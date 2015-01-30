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
#include <image/io.hpp>
#ifndef png_infopp_NULL
#define png_infopp_NULL (png_infopp)NULL
#endif

#ifndef int_p_NULL
#define int_p_NULL (int*)NULL
#endif 

#include <boost/gil/gil_all.hpp>
#include "numeric_extension/sampler.hpp"
#include "numeric_extension/resample.hpp"

namespace graphlab {


namespace image_util_detail {

using namespace boost::gil;

template<typename current_pixel_type, typename new_pixel_type>
void resize_image_detail(const char* data, size_t& width, size_t& height, size_t channels, size_t& resized_width, size_t resized_height, size_t resized_channels, char** resized_data){
  if (data == NULL){
    log_and_throw("Trying to resize image with NULL data pointer");
  }
  char* buf = new char[resized_height * resized_width * resized_channels];
  auto view = interleaved_view(width, height, (current_pixel_type*)data, width * channels * sizeof(char));
  auto resized_view = interleaved_view(resized_width, resized_height, (new_pixel_type*)buf,
                                       resized_width * resized_channels * sizeof(char));
  resize_view(color_converted_view<new_pixel_type>(view), (resized_view), nearest_neighbor_sampler());
  *resized_data = buf;
}


/**
 * Resize the image, and set resized_data to resized image data.
 */
void resize_image_impl(const char* data, size_t& width, size_t& height, size_t& channels, size_t resized_width, size_t resized_height, size_t resized_channels, char** resized_data) {
  // This code should be simplified
  if (channels == 1) {
    if (resized_channels == 1){
      resize_image_detail<gray8_pixel_t, gray8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else if (resized_channels == 3){
      resize_image_detail<gray8_pixel_t, rgb8_pixel_t>(data, width, height, channels,
                                                     resized_width, resized_height,
                                                     resized_channels, resized_data);
    } else if (resized_channels == 4){
      resize_image_detail<gray8_pixel_t, rgba8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else {
      log_and_throw (std::string("Unsupported channel size ") + std::to_string(channels));
    }
  } else if (channels == 3) {
    if (resized_channels == 1){
      resize_image_detail<rgb8_pixel_t, gray8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else if (resized_channels == 3){
      resize_image_detail<rgb8_pixel_t, rgb8_pixel_t>(data, width, height, channels,
                                                     resized_width, resized_height,
                                                     resized_channels, resized_data);
    } else if (resized_channels == 4){
      resize_image_detail<rgb8_pixel_t, rgba8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else {
      log_and_throw (std::string("Unsupported channel size ") + std::to_string(channels));
    }
  } else if (channels == 4) {
    if (resized_channels == 1){
      resize_image_detail<rgba8_pixel_t, gray8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else if (resized_channels == 3){
      resize_image_detail<rgba8_pixel_t, rgb8_pixel_t>(data, width, height, channels,
                                                     resized_width, resized_height,
                                                     resized_channels, resized_data);
    } else if (resized_channels == 4){
      resize_image_detail<rgba8_pixel_t, rgba8_pixel_t>(data, width, height, channels,
                                                      resized_width, resized_height,
                                                      resized_channels, resized_data);
    } else {
      log_and_throw (std::string("Unsupported channel size ") + std::to_string(channels));
    }
  }
}

void decode_image_impl(image_type& image) {
  if (image.m_format == Format::RAW_ARRAY) {
    return;
  }

  char* buf = NULL;
  size_t length = 0;

  if (image.m_format == Format::JPG) {
    decode_jpeg((const char*)image.get_image_data(), image.m_image_data_size,
                &buf, length);
  } else if (image.m_format == Format::PNG) {
    decode_png((const char*)image.get_image_data(), image.m_image_data_size,
                &buf, length);
  } else {
    log_and_throw(std::string("Cannot decode image. Unknown format."));
  };
  image.m_image_data.reset(buf);
  image.m_image_data_size = length;
  image.m_format = Format::RAW_ARRAY;
}

void encode_image_impl(image_type& image) {
  if (image.m_format != Format::RAW_ARRAY){
    return;
  } 

  char* buf = NULL;
  size_t length = 0;

  encode_png((const char*)image.get_image_data(), image.m_width, image.m_height, image.m_channels, &buf, length);
  image.m_image_data.reset(buf);
  image.m_image_data_size = length;
  image.m_format = Format::PNG;
}

} // end of image_util_detail
} // end of graphlab
