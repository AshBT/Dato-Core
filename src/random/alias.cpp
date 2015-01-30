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
#include <timer/timer.hpp>
#include <random/random.hpp>
#include <vector>
#include <string>
#include <utility>
#include <random/alias.hpp>

namespace graphlab {
namespace random {

alias_sampler::alias_sampler(const std::vector<double>& p) {
  auto S = std::vector<size_t>();
  auto L = std::vector<size_t>();
  K = p.size();
  J = std::vector<size_t>(K);
  q = std::vector<double>(K);
  double sum_p = 0;
  for (auto pi : p) sum_p += pi;
  for (size_t i = 0; i < K; ++i) {
    q[i] = K * p[i] / sum_p;
    if (q[i] < 1) {
      S.emplace_back(i);
    } else {
      L.emplace_back(i);
    }
  }
  while (S.size() != 0 && L.size() != 0) {
    size_t s = S.back();
    S.pop_back();
    size_t l = L.back();
    /* assert(q[s] <= 1); assert(1 <= q[l]); */
    J[s] = l;
    q[l] -= (1 - q[s]);

    if (q[l] < 1) {
      S.emplace_back(l);
      L.pop_back(); 
    }
  }
  /** For testing purposes
    for (size_t i = 0; i < K; ++i) {
      double qi = q[i];
      for (size_t m = 0; m < K; ++m) {
        if (J[m] == i) {
          qi += 1 - q[m];
        }
      }
      assert(std::abs(qi - p[i] * K) < 0.000001);
    }
    */
}

size_t alias_sampler::sample() {
  size_t k = random::fast_uniform<size_t>(0, K - 1);
  if (q[k] > random::fast_uniform<double>(0, 1)) {
    return k;
  } else {
    return J[k];
  }
}

};
};

