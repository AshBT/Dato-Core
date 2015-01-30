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
#ifndef GRAPHLAB_SKETCHES_COUNTMIN_HPP
#define GRAPHLAB_SKETCHES_COUNTMIN_HPP
#include <cmath>
#include <cstdint>
#include <functional>
#include <random/random.hpp>
#include <util/cityhash_gl.hpp>
#include <logger/assertions.hpp>
namespace graphlab {
namespace sketches {

/**
 * An implementation of the countmin sketch for estimating the frequency
 * of each item in a stream. 
 *
 * For more information on the details of the sketch:
 * http://dimacs.rutgers.edu/~graham/pubs/papers/cmsoft.pdf
 * The implementation generally follows the pseudocode in Figure 2.
 * The resulting probabilistic data structure 
 *
 * Usage is simple.
 * \code
 *   countmin<T> cm; // for approximate counting of a stream with type T
 *   // repeatedly call
 *   cm.add(x)      // x can be anything that is hashable.
 *   cm.estimate(x) // will return an estimate of the frequency for
 *                  // a given element
 * \endcode
 * One can obtain guarantees on the error in answering a query is within a factor of
 * ε with probability δ if one sets the 
 *    width=ceiling(e / ε)
 *    depth=ceiling(log(1/δ)
 * where e is Euler's constant.
 * 
 */

template <typename T>
class countmin {
 private:

  size_t num_hash = 0; /// Number of hash functions to use
  size_t num_bits = 0; /// 2^b is the number of hash bins
  size_t num_bins = 0; /// equal to 2^b: The number of buckets

  std::vector<size_t> seeds;
  std::vector<std::vector<size_t> > counts; /// Buckets

 public:
  /**
   * Constructs a countmin sketch having "width" 2^bits and "depth".
   * The size of the matrix of counts will be depth x 2^bits.
   * 
   * \param bits The number of bins will be 2^bits. 
   *
   * \param depth The "depth" of the sketch is the number of hash functions
   * that will be used on each item. 
   */
  explicit inline countmin(size_t bits = 16, size_t depth = 4):
    num_hash(depth), num_bits(bits), num_bins(1 << num_bits) {
      ASSERT_GE(num_bins, 16);

      // Initialize hash functions and count matrix
      for (size_t j = 0; j < num_hash; ++j) {
        seeds.push_back(random::fast_uniform<size_t>(0, std::numeric_limits<size_t>::max()));
        counts.push_back(std::vector<size_t>(num_bins));
      }
   }

  /**
   * Adds an arbitrary object to be counted. Any object type can be used,
   * and there are no restrictions as long as std::hash<T> can be used to
   * obtain a hash value.
   */
  void add(const T& t, size_t count = 1) {
    // we use std::hash first, to bring it to a 64-bit number
    size_t i = hash64(std::hash<T>()(t));
    for (size_t j = 0; j < num_hash; ++j) {
      size_t bin = hash64(seeds[j] ^ i) % num_bins;  // TODO: bit mask 
      counts[j][bin] += count; 
    }
  }

 /**
   * Returns the estimate of the frequency for a given object.
   */
  inline size_t estimate(const T& t) {

    size_t E = std::numeric_limits<size_t>::max();
    size_t i = hash64(std::hash<T>()(t));
 
    // Compute the minimum value across hashes.
    for (size_t j = 0; j < num_hash; ++j) {
      size_t bin = hash64(seeds[j] ^ i) % num_bins;
      if (counts[j][bin] < E)
        E = counts[j][bin];      
    }
    return E;
  }

  /**
   * Merge two countmin datastructures. 
   * The two countmin objects must have the same width and depth.
   */
  void combine(const countmin& other) {
    ASSERT_EQ(num_bins, other.num_bins);
    ASSERT_EQ(num_hash, other.num_hash);

    for (size_t j = 0; j < num_hash; ++j) {
      for (size_t b = 0; b < num_bins; ++b) {
        counts[j][b] += other.counts[j][b];
      }
    }
  }
  
  ///// HELPER FUNCTIONS /////////////////

  /**
   * Prints the internal matrix containing the current counts.
   */
  inline void print() {
    for (size_t j = 0; j < num_hash; ++j) {
      std::cout << ">>> ";
      for (size_t b = 0; b < num_bins; ++b) {
        std::cout << counts[j][b] << " ";
      }
      std::cout << std::endl;
    }
  }

  /**
   * Computes the density of the internal counts matrix.
   */
  inline double density() {
    size_t count = 0;
    for (size_t j = 0; j < num_hash; ++j) {
      for (size_t b = 0; b < num_bins; ++b) {
        if (counts[j][b] != 0) count += 1;
      }
    }
    return (double) count / (double) (num_hash * num_bins);
  } 

}; // countmin 
} // namespace sketches
} // namespace graphlab
#endif
