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

namespace graphlab{

image_type::image_type(char* image_data, size_t height, size_t width, size_t channels, size_t image_data_size, int version, int format){
  m_image_data.reset(new char[image_data_size]);
  memcpy(&m_image_data[0], image_data, image_data_size);
  m_height = height;
  m_width = width;
  m_channels = channels;
  m_image_data_size = image_data_size;
  m_version = (char)version;
  m_format = static_cast<Format>(format);
}


void image_type::save(oarchive& oarc) const {
  oarc << m_version << m_height << m_width << m_channels << m_format <<m_image_data_size;
  if (m_image_data_size > 0) {
      oarc.write(&m_image_data[0], m_image_data_size);
  }
}

void image_type::load(iarchive& iarc) {
  iarc >> m_version >> m_height >> m_width >> m_channels >> m_format >> m_image_data_size;
  if (m_image_data_size > 0){
      m_image_data.reset (new char[m_image_data_size]);
      iarc.read(&m_image_data[0], m_image_data_size);
  } else {
      m_image_data.reset();
  }
}

const unsigned char* image_type::get_image_data() const { 
  if (m_image_data_size > 0){
    return (const unsigned char*)&m_image_data[0];
  } else{
    return NULL;
  }
}

}
