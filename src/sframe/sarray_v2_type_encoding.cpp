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
#include <functional>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sarray_v2_block_types.hpp>
#include <sframe/sarray_v2_type_encoding.hpp>
#include <util/dense_bitset.hpp>
#include <sframe/integer_pack.hpp>


namespace graphlab {
namespace v2_block_impl {
using namespace graphlab::integer_pack;

/**
 * Encodes a collection of numbers in data, skipping all UNDEFINED values.
 * It simply loops through the data, collecting a block of up to 
 * MAX_INTEGERS_PER_BLOCK numbers and calls frame_of_reference_encode_128()
 * on it.
 *
 * \note The coding does not store the number of values stored. The decoder
 * \ref decode_number() requires the number of values to decode correctly.
 */
void encode_number(block_info& info, 
                   oarchive& oarc, 
                   const std::vector<flexible_type>& data) {
  for (size_t i = 0;i < data.size(); ) {
    uint64_t encode_buf[MAX_INTEGERS_PER_BLOCK];
    size_t encode_buflen = 0;
    // collect a block of 128 integers
    while(i < data.size() && encode_buflen < MAX_INTEGERS_PER_BLOCK) {
      if (data[i].get_type() != flex_type_enum::UNDEFINED) {
        encode_buf[encode_buflen] = data[i].get<flex_int>();
        ++encode_buflen;
      }
      ++i;
    }
    if (encode_buflen == 0) break;
    //     logstream(LOG_INFO) << "Encoding ";
    //     for (size_t i = 0;i < encode_buflen; ++i) {
    //       logstream(LOG_INFO) << " " << encode_buf[i];
    //     }
    //     logstream(LOG_INFO) << std::endl;
    frame_of_reference_encode_128(encode_buf, encode_buflen, oarc);
  }
}


/**
 * Decodes a collection of numbers into 'data'. Entries in data which are 
 * of type flex_type_enum::UNDEFINED will be skipped, and there must be exactly
 * num_undefined number of them. It simply decodes a block using
 * frame_of_reference_decode_128() and fills in data with it.
 *
 * \note We have an explicit implementation here that is equivalent to 
 * decode_number_stream for performance reasons since this is a *very* commonly
 * encountered function.
 */
void decode_number(iarchive& iarc,
                   std::vector<flexible_type>& ret,
                   size_t num_undefined) {
  uint64_t buf[MAX_INTEGERS_PER_BLOCK];
  size_t bufstart = 0;
  size_t buflen = 0;
  size_t num_values_to_read = ret.size() - num_undefined;
  for (size_t i = 0;i < ret.size(); ++i) {
    if (ret[i].get_type() != flex_type_enum::UNDEFINED) {
      if (bufstart < buflen) {
        ret[i].mutable_get<flex_int>() = buf[bufstart];
        ++bufstart;
        --num_values_to_read;
      } else {
        buflen = std::min<size_t>(num_values_to_read, MAX_INTEGERS_PER_BLOCK);
        // decode a new block of 128 integers
        frame_of_reference_decode_128(iarc, buflen, buf);
//         logstream(LOG_INFO) << "Decoding ";
//         for (size_t i = 0;i < buflen; ++i) {
//           logstream(LOG_INFO) << " " << buf[i];
//         }
//         logstream(LOG_INFO) << std::endl;
        bufstart = 0;
        ret[i].mutable_get<flex_int>() = buf[bufstart];
        ++bufstart;
        --num_values_to_read;
      }
    }
  }
}


/**
 * Encodes a collection of doubles in data, skipping all UNDEFINED values.
 * It simply loops through the data, collecting a block of up to 
 * MAX_INTEGERS_PER_BLOCK numbers and calls frame_of_reference_encode_128()
 * on it.
 *
 * \note The coding does not store the number of values stored. The decoder
 * \ref decode_number() requires the number of values to decode correctly.
 */
void encode_double(block_info& info, 
                   oarchive& oarc, 
                   const std::vector<flexible_type>& data) {
  for (size_t i = 0;i < data.size(); ) {
    uint64_t encode_buf[MAX_INTEGERS_PER_BLOCK];
    size_t encode_buflen = 0;
    // collect a block of 128 integers
    while(i < data.size() && encode_buflen < MAX_INTEGERS_PER_BLOCK) {
      if (data[i].get_type() != flex_type_enum::UNDEFINED) {
        encode_buf[encode_buflen] = data[i].get<flex_int>();
        ++encode_buflen;
      }
      ++i;
    }
    if (encode_buflen == 0) break;
    // perform a left rotate on all the numbers.
    // Basically, doubles are stored as sign-and-magnitude. This means
    // that -1.0 looks *very* different from -1. 
    for (size_t j = 0;j < encode_buflen; ++j) {
      encode_buf[j] = (encode_buf[j] << 1) | (encode_buf[j] >> 63);
    }
    frame_of_reference_encode_128(encode_buf, encode_buflen, oarc);
  }
}

/**
 * Decodes a collection of doubles into 'data'. Entries in data which are 
 * of type flex_type_enum::UNDEFINED will be skipped, and there must be exactly
 * num_undefined number of them. It simply decodes a block using
 * frame_of_reference_decode_128() and fills in data with it.
 *
 * \note We have an explicit implementation here that is equivalent to 
 * decode_number_stream for performance reasons since this is a *very* commonly
 * encountered function.
 */
void decode_double(iarchive& iarc,
                   std::vector<flexible_type>& ret,
                   size_t num_undefined) {
  uint64_t buf[MAX_INTEGERS_PER_BLOCK];
  size_t bufstart = 0;
  size_t buflen = 0;
  size_t num_values_to_read = ret.size() - num_undefined;

  for (size_t i = 0;i < ret.size(); ++i) {
    if (ret[i].get_type() != flex_type_enum::UNDEFINED) {
      if (bufstart < buflen) {
        ret[i].mutable_get<flex_int>() = buf[bufstart];
        ++bufstart;
        --num_values_to_read;
      } else {
        buflen = std::min<size_t>(num_values_to_read, MAX_INTEGERS_PER_BLOCK);
        // decode a new block of 128 integers
        frame_of_reference_decode_128(iarc, buflen, buf);
        // right rotate
        for (size_t j = 0;j < buflen ; ++j) {
          buf[j] = (buf[j] >> 1) | (buf[j] << 63);
        }
        bufstart = 0;
        ret[i].mutable_get<flex_int>() = buf[bufstart];
        ++bufstart;
        --num_values_to_read;
      }
    }
  }
}

/**
 * Encodes a collection of strings in data, skipping all UNDEFINED values.
 *
 * Two encoding strategies are used.
 * Strategy 1: 
 * Dictionary encode:
 *  - A dictionary of unique strings are built, and an array of numbers 
 *    mapping to the string values are constructed.
 *     - variable_encode(dictionary length)
 *     - for each entry in dictionary:
 *         - variable_encode entry length
 *         - write bytes contents for each entry
 *     - encode_number(dictionary mapping)
 * Strategy 2:
 * Direct encode:
 *  - encode_number(lengths of all the strings)
 *  - for each entry:
 *     - write byte contents for each entry
 *
 * \note The coding does not store the number of values stored. The decoder
 * \ref decode_string() requires the number of values to decode correctly.
 */
static void encode_string(block_info& info, 
                          oarchive& oarc, 
                          const std::vector<flexible_type>& data) {
  bool use_dictionary_encoding = true;
  std::unordered_map<std::string, size_t> unique_values;
  std::vector<flexible_type> idx_values;
  std::vector<std::string> str_values;
  idx_values.resize(data.size(), flexible_type(flex_type_enum::INTEGER));
  size_t idxctr = 0;
  for (size_t i = 0;i < data.size(); ++i) {
    if (data[i].get_type() != flex_type_enum::UNDEFINED) {
      auto iter = unique_values.find(data[i].get<std::string>());
      if (iter != unique_values.end()) {
        idx_values[idxctr++].mutable_get<flex_int>() = iter->second;
      } else {
        // if we have too many unique values, fail.
        if (unique_values.size() >= 64) {
          use_dictionary_encoding = false;
          break;
        }
        size_t newidx = unique_values.size();
        unique_values[data[i].get<std::string>()] = newidx;
        str_values.push_back(data[i].get<std::string>());
        idx_values[idxctr++].mutable_get<flex_int>() = newidx;
      }
    }
  }
  oarc << use_dictionary_encoding;
  if (use_dictionary_encoding) {
    idx_values.resize(idxctr);

    variable_encode(oarc, str_values.size());
    for (auto& str: str_values) {
      variable_encode(oarc, str.length());
      oarc.write(str.c_str(), str.length());
    }
    encode_number(info, oarc, idx_values);
  } else {
    // encode all the lengths 
    idxctr = 0;
    for (auto& f: data) {
      if (f.get_type() != flex_type_enum::UNDEFINED) {
        idx_values[idxctr++].mutable_get<flex_int>() = f.get<std::string>().length();
      }
    }
    idx_values.resize(idxctr);
    encode_number(info, oarc, idx_values);
    for (auto& f: data) {
      if (f.get_type() != flex_type_enum::UNDEFINED) {
        oarc.write(f.get<std::string>().c_str(), f.get<flex_string>().length());
      }
    }
  }
}

/**
 * Decodes a collection of strings into 'data'. Entries in data which are 
 * of type flex_type_enum::UNDEFINED will be skipped, and there must be exactly
 * num_undefined number of them. It simply decodes a block using
 * frame_of_reference_decode_128() and fills in data with it.
 */
static void decode_string(iarchive& iarc, 
                          std::vector<flexible_type>& ret,
                          size_t num_undefined) {
  unsigned int last_id = 0;
  decode_string_stream(ret.size() - num_undefined, iarc, 
                       [&](flexible_type val) {
                         while(last_id < ret.size() && 
                               ret[last_id].get_type() == flex_type_enum::UNDEFINED) {
                           ++last_id;
                         }
                         ret[last_id] = val;
                         DASSERT_LT(last_id, ret.size());
                         ++last_id;
                       });
}

/**
 * Encodes a collection of vectors in data, skipping all UNDEFINED values.
 *
 *  - encode a list of integers with all the vector lengths
 *  - encode a list of all the doubles
 *
 * \note The coding does not store the number of values stored. The decoder
 * \ref decode_vector() requires the number of values to decode correctly.
 */
static void encode_vector(block_info& info, 
                          oarchive& oarc, 
                          const std::vector<flexible_type>& data) {
  // length of each vector
  std::vector<flexible_type> lengths;
  // values of all the vectors concatted together
  std::vector<flexible_type> values;

  for (size_t i = 0;i < data.size(); ++i) {
    if (data[i].get_type() != flex_type_enum::UNDEFINED) {
      const flex_vec& vals = data[i].get<flex_vec>();
      lengths.push_back(vals.size());
      for (const double d: vals) {
        values.push_back(d);
      }
    }
  }
  encode_number(info, oarc, lengths);
  encode_double(info, oarc, values);
}

/**
 * Decodes a collection of vectors in data, skipping all UNDEFINED values.
 * Wrapper around decode_number_stream
 */
static void decode_vector(iarchive& iarc, 
                          std::vector<flexible_type>& ret,
                          size_t num_undefined) {
  unsigned int last_id = 0;
  decode_vector_stream(ret.size() - num_undefined, iarc, 
                       [&](flexible_type val) {
                         while(last_id < ret.size() && 
                               ret[last_id].get_type() == flex_type_enum::UNDEFINED) {
                           ++last_id;
                         }
                         ret[last_id] = val;
                         DASSERT_LT(last_id, ret.size());
                         ++last_id;
                       });
}
/**
 * Encodes a collection of flexible_type values. The array must be of 
 * contiguous type, but permitting undefined values.
 *
 * There is a two byte header to the block.
 * - num_types: 1 byte
 *     - if 0, the block is empty. 
 *     - if 1, the array is of contiguous type (see next byte)
 *     - if 2, the array is of contiguous type, but has missing values.
 * - type: 1 byte.
 * - [undefined bitfield]: if type is 2, this contains a bitfield of
 *   (round_op(#elem / 8) bytes) listing the positions of all the UNDEFINED 
 *   fields)
 * - type specific encoding:
 *     - if integer or float, encode_number() is called
 *     - if string, encode_string() is called
 *     - otherwise, direct serialization is currently used.
 *     - If UNDEFINED (i.e. array is of all UNDEFINED values, nothing is written)
 *
 * \note The coding does not store the number of values stored. This is
 * stored in the block_info (block.num_elem)
 */
void typed_encode(const std::vector<flexible_type>& data, 
                  block_info& block,
                  oarchive& oarc) {
  block.flags |= IS_FLEXIBLE_TYPE;
  block.num_elem = data.size();
 
  // figure out how many types there are in the array 
  graphlab::fixed_dense_bitset<16> types_appeared;
  types_appeared.clear();
  for (size_t i = 0;i < data.size(); ++i) {
    types_appeared.set_bit_unsync((char)data[i].get_type());
  }

  // write one byte for the number of types in the block
  char num_types = types_appeared.popcount();
  oarc << num_types;
  bool perform_type_encoding = true;
  if (num_types == 0) {
    // no types. no array. nothing. quit
    block.block_size = oarc.off;
    return;
  } if (num_types == 1) {
    // One type. array has only one type
    oarc << (char)data[0].get_type();
    // entire block is of UNDEFINED values. quit.
    if (data[0].get_type() == flex_type_enum::UNDEFINED) {
      block.block_size = oarc.off;
      return;
    }
  } else if (num_types == 2 && types_appeared.get((char)flex_type_enum::UNDEFINED)) {
    // two types, one of them must be UNDEFINED
    // write that type.
    for(auto t: types_appeared) {
      if ((flex_type_enum)t != flex_type_enum::UNDEFINED) {
        oarc << (char)(t);
        break;
      }
    }
    // then write a bit field containing the positions of the UNDEFINED bits.
    graphlab::dense_bitset d(data.size());
    d.clear();
    for (size_t i = 0;i < data.size(); ++i) {
      if (data[i].get_type() == flex_type_enum::UNDEFINED) d.set_bit_unsync(i);
    }
    oarc.write((char*)d.array, sizeof(size_t) * d.arrlen);
  } else {
    oarc << data;
    perform_type_encoding = false;
    block.flags |= MULTIPLE_TYPE_BLOCK;
  }
  if (perform_type_encoding) {
    if (types_appeared.get((char)flex_type_enum::INTEGER)) {
      encode_number(block, oarc, data);
    } else if(types_appeared.get((char)flex_type_enum::FLOAT)) {
      encode_double(block, oarc, data);
    } else if (types_appeared.get((char)flex_type_enum::STRING)) {
      encode_string(block, oarc, data);
    } else if (types_appeared.get((char)flex_type_enum::VECTOR)) {
      encode_vector(block, oarc, data);
    } else {
      flexible_type_impl::serializer s{oarc};
      for (size_t i = 0;i < data.size(); ++i) {
        if (data[i].get_type() != flex_type_enum::UNDEFINED) {
          data[i].apply_visitor(s);
        }
      }
    }
  }
  block.block_size = oarc.off;
}

/**
 * Decodes a collection of flexible_type values. The array must be of 
 * contiguous type, but permitting undefined values.
 *
 * See \ref typed_encode() for details.
 *
 * \note The coding does not store the number of values stored. This is
 * stored in the block_info (block.num_elem)
 */
bool typed_decode(const block_info& info,
                  char* start, size_t len,
                  std::vector<flexible_type>& ret) {
  if (!(info.flags & IS_FLEXIBLE_TYPE)) {
    logstream(LOG_ERROR) << "Attempting to decode a non-typed block"
                         << std::endl;
    return false;
  }
  graphlab::iarchive iarc(start, len);

  size_t dsize = info.num_elem;
  ret.resize(dsize);
  char num_types; iarc >> num_types;
  flex_type_enum column_type;
  size_t num_undefined = 0;
  // if it is a multiple type block, we don't perform a type decode
  bool perform_type_decoding = !(info.flags & MULTIPLE_TYPE_BLOCK);
  if (perform_type_decoding) {
    if (num_types == 0) {
      // empty block
      return true;
    } else if (num_types == 1) {
      //  one block of contiguous type. quit
      char c;
      iarc >> c;
      column_type = (flex_type_enum)c;
      for (size_t i = 0;i < dsize; ++i) {
        ret[i].reset(column_type);
      }
      // all undefined. quit
      if (column_type == flex_type_enum::UNDEFINED) return true;
    } else if (num_types == 2) {
      // two types, but with undefined entries.
      char c;
      iarc >> c;
      column_type = (flex_type_enum)c;
      for (size_t i = 0;i < dsize; ++i) {
        ret[i].reset(column_type);
      }
      // read the bitset and undefine all the flagged entries
      graphlab::dense_bitset d(info.num_elem);
      d.clear();
      iarc.read((char*)d.array, sizeof(size_t)*d.arrlen);
      for (auto t: d) {
        ret[t].reset(flex_type_enum::UNDEFINED);
      }
      num_undefined = d.popcount();
    }
  } else {
    iarc >> ret;
  }
  if (perform_type_decoding) {
    // type decode
    if (column_type == flex_type_enum::INTEGER) {
      decode_number(iarc, ret, num_undefined);
    } else if (column_type == flex_type_enum::FLOAT) {
      decode_double(iarc, ret, num_undefined);
    } else if (column_type == flex_type_enum::STRING) {
      decode_string(iarc, ret, num_undefined);
    } else if (column_type == flex_type_enum::VECTOR) {
      decode_vector(iarc, ret, num_undefined);
    } else {
      flexible_type_impl::deserializer s{iarc};
      for (size_t i = 0;i < dsize; ++i) {
        if (ret[i].get_type() != flex_type_enum::UNDEFINED) {
          ret[i].apply_mutating_visitor(s);
        }
      }
    }
  }

  if (ret.size() != info.num_elem) {
    logstream(LOG_ERROR) << "Unexpected number of elements read. "
                         << "Read " << ret.size() 
                         << ". Expecting " << info.num_elem << std::endl;
    return false;
  }
  return true;
}




} // namespace v2_block_impl
} // namespace graphlab
