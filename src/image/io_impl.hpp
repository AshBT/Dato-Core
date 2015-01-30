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
#ifndef GRAPHLAB_IMAGE_IMAGE_IO_IMPL_HPP
#define GRAPHLAB_IMAGE_IMAGE_IO_IMPL_HPP

#ifndef png_infopp_NULL
#define png_infopp_NULL (png_infopp)NULL
#endif

#ifndef int_p_NULL
#define int_p_NULL (int*)NULL
#endif 

#include <image/image_type.hpp>
#include <boost/gil/extension/io/jpeg_dynamic_io.hpp>
#include <boost/gil/extension/io/jpeg_io.hpp>
#include <boost/gil/extension/io/png_dynamic_io.hpp>
#include <boost/gil/extension/io/png_io.hpp>
#include <logger/logger.hpp>

using namespace boost::gil;

namespace graphlab {

template<typename pixel_type>
void write_image_impl(std::string filename, char* data, size_t& width, size_t& height, size_t& channels, Format format ) {
  auto view = interleaved_view(width, height, (pixel_type*)data, width * channels * sizeof(char));
  if (format == Format::JPG) {
    jpeg_write_view(filename, view);
  } else  if (format == Format::PNG){
    png_write_view(filename, view);
  }
} 

// Template specialization: JPEG does not support RGBA
template<>
void write_image_impl<rgba8_pixel_t>(std::string filename, char* data, size_t& width, size_t& height, size_t& channels, Format format ) {
  auto view = interleaved_view(width, height, (rgba8_pixel_t*)data, width * channels * sizeof(char));
  if (format == Format::JPG) {
    throw ("JPEG does not support RGBA color type");
  } else  if (format == Format::PNG){
    png_write_view(filename, view);
  }
}

/**************************************************************************/
/*                                                                        */
/*             Boost Prototype Code, Not used in actual code              */
/*                                                                        */
/**************************************************************************/
template<typename pixel_type>
void boost_read_image_impl(std::string filename, char** data, size_t& width, size_t& height, size_t& channels, Format format ) {
  char* buf = new char[width * height * channels];
  auto view = interleaved_view(width, height, (pixel_type*)buf, width * channels * sizeof(char));
  if (format == Format::JPG) {
    jpeg_read_view(filename, view);
  } else  if (format == Format::PNG){
    png_read_view(filename, view);
  }
  *data = buf;
}

// Template specialization: JPEG does not support RGBA
template<>
void boost_read_image_impl<rgba8_pixel_t>(std::string filename, char** data, size_t& width, size_t& height, size_t& channels, Format format ) {
  char* buf = new char[width * height * channels];
  auto view = interleaved_view(width, height, (rgba8_pixel_t*)buf, width * channels * sizeof(char));
  if (format == Format::JPG) {
    throw ("JPEG does not support RGBA color type");
  } else  if (format == Format::PNG){
    png_read_view(filename, view);
  }
  *data = buf;
}


}
#endif
