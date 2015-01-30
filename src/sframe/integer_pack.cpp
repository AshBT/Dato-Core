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
#include <serialization/serialization_includes.hpp>
namespace graphlab {
namespace integer_pack {

size_t pack_1(uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 1;
    case 6:      c |= ((*src++)) << 2;
    case 5:      c |= ((*src++)) << 3;
    case 4:      c |= ((*src++)) << 4;
    case 3:      c |= ((*src++)) << 5;
    case 2:      c |= ((*src++)) << 6;
    case 1:      c |= ((*src++)) << 7;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}

size_t pack_2(uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 2;
    case 6:      c |= ((*src++)) << 4;
    case 5:      c |= ((*src++)) << 6;
                 (*out++) = c;
                 c = 0;
    case 4:      c |= ((*src++));
    case 3:      c |= ((*src++)) << 2;
    case 2:      c |= ((*src++)) << 4;
    case 1:      c |= ((*src++)) << 6;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}


size_t pack_4(uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 6:      c |= ((*src++));
    case 5:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 4:      c |= ((*src++));
    case 3:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 2:      c |= ((*src++));
    case 1:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}

size_t pack_8(uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return out - initial_out;
}


size_t pack_16(uint64_t* src, size_t srclen, uint16_t* out) {
  uint16_t* initial_out = out;
  uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return 2*(out - initial_out);
}

size_t pack_32(uint64_t* src, size_t srclen, uint32_t* out) {
  uint32_t* initial_out = out;
  uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return 4*(out - initial_out);
}


void unpack_1(uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  // the first byte, if incomplete, annoying is going to live
  // in the most significant bits of c. Thus if nout_value % 8 != 0,
  // I need to do a bit of shifting. 
  c >>= ((8 - (nout_values % 8)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 1; c >>= 1;
    case 7:      (*out++) = c & 1; c >>= 1;
    case 6:      (*out++) = c & 1; c >>= 1;
    case 5:      (*out++) = c & 1; c >>= 1;
    case 4:      (*out++) = c & 1; c >>= 1;
    case 3:      (*out++) = c & 1; c >>= 1;
    case 2:      (*out++) = c & 1; c >>= 1;
    case 1:      (*out++) = c & 1; c >>= 1;
   } while(--n > 0);
  }
}


void unpack_2(uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  c >>= ((8 - 2 * (nout_values % 4)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 3; c >>= 2;
    case 7:      (*out++) = c & 3; c >>= 2;
    case 6:      (*out++) = c & 3; c >>= 2;
    case 5:      (*out++) = c & 3; c >>= 2;
                 c = (*src++);
    case 4:      (*out++) = c & 3; c >>= 2;
    case 3:      (*out++) = c & 3; c >>= 2;
    case 2:      (*out++) = c & 3; c >>= 2;
    case 1:      (*out++) = c & 3; c >>= 2;
   } while(--n > 0);
  }
}


void unpack_4(uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  c >>= ((8 - 4 * (nout_values % 2)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 15; c >>= 4;
    case 7:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 6:      (*out++) = c & 15; c >>= 4;
    case 5:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 4:      (*out++) = c & 15; c >>= 4;
    case 3:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 2:      (*out++) = c & 15; c >>= 4;
    case 1:      (*out++) = c & 15; c >>= 4;
   } while(--n > 0);
  }
}


void unpack_8(uint8_t* src, size_t nout_values, uint64_t* out) {
  uint8_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}


void unpack_16(uint16_t* src, size_t nout_values, uint64_t* out) {
  uint16_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}

void unpack_32(uint32_t* src, size_t nout_values, uint64_t* out) {
  uint32_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}


} // namespace integer_pack
} // namespace graphlab
