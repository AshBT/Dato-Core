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
#include <stdio.h>
#include <jpeglib.h>

#include <string.h>
#include <logger/logger.hpp>

namespace graphlab {

/*
 * Here's the routine that will replace the standard error_exit method:
 */
METHODDEF(void)
jpeg_error_exit (j_common_ptr cinfo)
{
  /* Always display the message. */
  /* We could postpone this until after returning, if we chose. */
  (*cinfo->err->output_message) (cinfo);
  log_and_throw(std::string("Unexpected JPEG decode failure"));
}

void parse_jpeg(const char* data, size_t length,
                size_t& width, size_t& height,
                size_t& channels) {
  struct jpeg_decompress_struct cinfo;
  struct jpeg_error_mgr jerr;
  memset(&cinfo, 0, sizeof(cinfo));
  memset(&jerr, 0, sizeof(jerr));
  // Initialize the JPEG decompression object with default error handling
  cinfo.err = jpeg_std_error(&jerr);
  jerr.error_exit = jpeg_error_exit;
  try {
    jpeg_create_decompress(&cinfo);

    jpeg_mem_src(&cinfo, (unsigned char*)data, length); // Specify data source for decompression
    jpeg_read_header(&cinfo, TRUE); // Read file header, set default decompression parameters

    width = cinfo.image_width;
    height = cinfo.image_height;
    channels = cinfo.num_components;
  } catch (...) {
    jpeg_destroy_decompress(&cinfo);
    throw;
  }
  jpeg_destroy_decompress(&cinfo);
}

void decode_jpeg(const char* data, size_t length, char** out_data, size_t& out_length) {
  struct jpeg_decompress_struct cinfo;
  struct jpeg_error_mgr jerr;
  memset(&cinfo, 0, sizeof(cinfo));
  memset(&jerr, 0, sizeof(jerr));
  // Initialize the JPEG decompression object with default error handling
  cinfo.err = jpeg_std_error(&jerr);
  jerr.error_exit = jpeg_error_exit;
  *out_data = NULL;
  out_length = 0;

  if (data == NULL) {
    log_and_throw("Trying to decode image with NULL data pointer.");
  }

  try {
    jpeg_create_decompress(&cinfo);

    jpeg_mem_src(&cinfo, (unsigned char*)data, length); // Specify data source for decompression
    jpeg_read_header(&cinfo, TRUE); // Read file header, set default decompression parameters
    jpeg_start_decompress(&cinfo); // Start decompressor

    size_t width = cinfo.image_width;
    size_t height = cinfo.image_height;
    size_t channels = cinfo.num_components;
    out_length = width * height * channels;
    *out_data = new char[out_length];
    size_t row_stride = width * channels;

    size_t row_offset = 0;
    JSAMPROW rowptr[1];
    while (cinfo.output_scanline < cinfo.output_height) {
      rowptr[0] = (unsigned char*)(*out_data + row_offset * row_stride);
      jpeg_read_scanlines(&cinfo, rowptr, 1);
      ++row_offset;
    }

    jpeg_finish_decompress(&cinfo);
  } catch (...) {
    if (*out_data != NULL) {
      delete[] *out_data;
      out_length = 0;
    }
    jpeg_destroy_decompress(&cinfo);
    throw;
  }
  jpeg_destroy_decompress(&cinfo);
}

}
