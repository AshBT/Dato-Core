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
#include <iostream>
#include <logger/assertions.hpp>
#include <boost/algorithm/string.hpp>

void usage() {
  std::cerr << "./io_example sample_in.[jpg | png] out.[jpg | png]" << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    usage();
    exit(0);
  }

  std::string input(argv[1]);
  std::string output(argv[2]);

  std::cout << "Input: " << input << "\t"
            << "Output: " << output << std::endl;

  size_t raw_size = 0;
  size_t width = 0, height = 0, channels = 0;
  char* data = NULL;
  graphlab::Format format;

  graphlab::read_raw_image(input, &data, raw_size, width, height, channels, format, "");
  std::cout << "Width: " << width << "\t Height: " << height
            <<  "\t channels: " << channels << std::endl;

  if (data != NULL) {
    if (boost::algorithm::ends_with(input, "jpg") ||
        boost::algorithm::ends_with(input, "jpeg")) {
      char* out_data;
      size_t out_length;
      graphlab::decode_jpeg(data, raw_size, &out_data, out_length);
      graphlab::write_image(output, out_data, width, height, channels, graphlab::Format::JPG);
      delete[] out_data;
    } else if (boost::algorithm::ends_with(input, "png")){
      char* out_data;
      size_t out_length;
      graphlab::decode_png(data, raw_size, &out_data, out_length);
      graphlab::write_image(output, out_data, width, height, channels, graphlab::Format::PNG);
      delete[] out_data;
    } else {
      std::cerr << "Unsupported format" << std::endl;
    }
    delete[] data;
  }
}
