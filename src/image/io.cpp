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
#include <image/image_type.hpp>
#include <image/io.hpp>
#include <image/io_impl.hpp>
#include <fileio/general_fstream.hpp>
#include <boost/mpl/vector.hpp>
#include <boost/algorithm/string.hpp>
#include <stdio.h>
#include <sys/stat.h>


using namespace boost::gil;

namespace graphlab {

image_type read_image(const std::string& url, const std::string& format_hint) {
  image_type img;
  char* data;
  read_raw_image(url, &data, img.m_image_data_size,
      img.m_width, img.m_height, img.m_channels,
      img.m_format, format_hint);
  img.m_image_data.reset(data);
  return img;
}

void read_raw_image(const std::string& url, char** data, size_t& length, size_t& width, size_t& height, size_t& channels, Format& format, const std::string& format_hint) {

  general_ifstream fin(url);
  length = fin.file_size();
  *data = new char[length];
  try {
    fin.read(*data, length);
    if (format_hint == "JPG") {
      format = Format::JPG;
    } else if (format_hint == "PNG") {
      format = Format::PNG;
    } else {
      if (boost::algorithm::iends_with(url, "jpg") ||
          boost::algorithm::iends_with(url, "jpeg")) {
        format = Format::JPG;
      } else if (boost::algorithm::iends_with(url, "png")) {
        format = Format::PNG;
      }
    }
    if (format == Format::JPG) {
      parse_jpeg(*data, length, width, height, channels);
    } else if (format == Format::PNG) {
      parse_png(*data, length, width, height, channels);
    } else {
      log_and_throw(std::string("Unsupported image format. Supported formats are JPG and PNG"));
    }
  } catch (...) {
    delete[] *data;
    *data = NULL;
    length = 0;
    throw;
  }
  fin.close();
};

void write_image(const std::string& filename, char* data, size_t width, size_t height, size_t channels, Format format) {
  if (channels == 1) {
    write_image_impl<gray8_pixel_t>(filename, data, width, height, channels, format);
  } else if (channels == 3) {
    write_image_impl<rgb8_pixel_t>(filename, data, width, height, channels, format);
  } else if (channels == 4) {
    write_image_impl<rgba8_pixel_t>(filename, data, width, height, channels, format);
  } else {
    log_and_throw (std::string("Unsupported channel size " + std::to_string(channels)));
  }
} 

/**************************************************************************/
/*                                                                        */
/*                             Prototype Code                             */
/*                                                                        */
/**************************************************************************/
void boost_parse_image(std::string filename, size_t& width, size_t& height, size_t& channels, Format& format, size_t& image_data_size, std::string format_string) {

  typedef boost::mpl::vector<gray8_image_t, gray16_image_t, rgb8_image_t, rgb16_image_t> my_img_types;

  any_image<my_img_types> src_image;

  if (format_string == "JPG"){
    jpeg_read_image(filename, src_image);
    format = Format::JPG;
  } else if (format_string == "PNG"){
    png_read_image(filename, src_image);
    format = Format::PNG;
  } else{
    if (boost::algorithm::ends_with(filename, "jpg") ||
      boost::algorithm::ends_with(filename, "jpeg")) {
      jpeg_read_image(filename, src_image);
      format = Format::JPG;
    } else if (boost::algorithm::ends_with(filename, "png")){
      png_read_image(filename, src_image);
      format = Format::PNG;
    } else {
      log_and_throw(std::string("Unsupported format."));
    }
  }

  // create a view of the image
  auto src_view = const_view(src_image);

  // extract image information
  width = src_view.width();
  height = src_view.height();
  channels = src_view.num_channels();
  image_data_size = width*height*channels;

  // Debug
  // std::cout << "Read image "<< filename << "\n"
  // << "width: " << width << "\n"
  // << "height: " << height << "\n"
  // << "num_channels " << channels << "\n"
  // << std::endl;
}

void boost_read_image(std::string filename, char** data, size_t& width, size_t& height, size_t& channels, Format& format, size_t& image_data_size, std::string format_string) {
    boost_parse_image(filename, width, height, channels, format, image_data_size, format_string);
    if (channels == 1) {
      boost_read_image_impl<gray8_pixel_t>(filename, data, width, height, channels, format);
    } else if (channels == 3) {
      boost_read_image_impl<rgb8_pixel_t>(filename, data, width, height, channels, format);
    } else if (channels == 4) {
      boost_read_image_impl<rgba8_pixel_t>(filename, data, width, height, channels, format);
    } else {
      log_and_throw (std::string("Unsupported channel size ") + std::to_string(channels));
    }
}





}
