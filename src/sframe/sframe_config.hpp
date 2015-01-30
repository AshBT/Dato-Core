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
#ifndef GRAPHLAB_SFRAME_CONFIG_HPP
#define GRAPHLAB_SFRAME_CONFIG_HPP
#include <cstddef>
namespace graphlab {

/**
** Global configuration for sframe, keep them as non-constants because we want to
** allow user/server to change the configuration according to the environment
**/
namespace sframe_config {
  /**
  **  The max buffer size to keep for sorting in memory
  **/
  extern size_t SFRAME_SORT_BUFFER_SIZE;

  /**
  **  The number of rows to read each time for paralleliterator
  **/
  extern size_t SFRAME_READ_BATCH_SIZE;
}

}
#endif //GRAPHLAB_SFRAME_CONFIG_HPP
